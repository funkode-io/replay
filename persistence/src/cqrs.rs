use std::sync::Arc;

use futures::{Stream, StreamExt, TryStreamExt};

use replay::{Aggregate, Event};
use urn::Urn;

use super::{AggregateVersion, CompactionOutcome, EventStore, PersistedEvent, PostgresEventStore};
use crate::{CommandContext, CommandContextMut, CommandMiddleware, CommitGate, Next, Outcome};

/// An aggregate kept between commands, and the head of its stream its state is at.
pub(crate) struct Loaded<A> {
    aggregate: A,
    head: StreamHead,
}

/// Where a stream's head is, as `streams` records it, and which event holds it.
/// `stream_seq` never goes back, a compaction included, and the head event's id is never
/// reused, a stream deleted and recreated to the same counts included — so two equal
/// heads mean nobody wrote in between. `version` is what the append's
/// optimistic-concurrency check compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct StreamHead {
    version: i64,
    stream_seq: i64,
    event: Option<uuid::Uuid>,
}

/// The commit gate `Cqrs` hands every append it makes for a command: each middleware's
/// `before_commit`, in registration order, on the context `before` left; the first that
/// does not continue has the last word, and a `Busy` reason is kept for `after`
/// (ADR-0035). Owned, so a command's append can carry it into the store whatever the path
/// — online, bulk, or a bulk worker's grouped dispatch. A clone shares the context and
/// what the gate answered, so a bulk append asks one clone per segment.
#[derive(Clone)]
pub(crate) struct MiddlewareGate {
    middlewares: Arc<[Arc<dyn CommandMiddleware>]>,
    /// `None` when no middleware is registered: the gate always continues.
    context: Option<Arc<CommandContext>>,
    busy: Arc<std::sync::OnceLock<String>>,
    /// Set when a middleware did not continue: the gate's answer is the outcome.
    stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl MiddlewareGate {
    /// The gate of a `Cqrs` with no middleware.
    fn open() -> Self {
        Self {
            middlewares: Arc::new([]),
            context: None,
            busy: Arc::default(),
            stopped: Arc::default(),
        }
    }
}

impl CommitGate for MiddlewareGate {
    async fn check(self) -> Next {
        let Some(context) = self.context else {
            return Next::Continue;
        };
        for middleware in self.middlewares.iter() {
            match middleware.before_commit(&context).await {
                Next::Continue => {}
                Next::Busy(reason) => {
                    let _ = self.busy.set(reason.clone());
                    self.stopped
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return Next::Busy(reason);
                }
                stop => {
                    self.stopped
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    return stop;
                }
            }
        }
        Next::Continue
    }

    /// Open without middleware, so a store takes no in-flight lock for it.
    fn always_continues(&self) -> bool {
        self.context.is_none()
    }
}

/// Built-in default for [`Cqrs::compaction_chunk_size`].
const DEFAULT_COMPACTION_CHUNK_SIZE: u32 = 100;

pub struct Cqrs<ES: EventStore> {
    store: Arc<ES>,
    /// How many stream ids [`Cqrs::streams_needing_compaction`] reads per store query.
    compaction_chunk_size: u32,
    /// The command middleware chain, in registration order. Shared by every clone, and
    /// bounded by how many middlewares the consumer registers, not by any data.
    middlewares: Arc<[Arc<dyn CommandMiddleware>]>,
}

/// A clone shares the store and the middleware chain, so it needs no `ES: Clone`.
impl<ES: EventStore> Clone for Cqrs<ES> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            compaction_chunk_size: self.compaction_chunk_size,
            middlewares: Arc::clone(&self.middlewares),
        }
    }
}

impl<ES: EventStore> Cqrs<ES> {
    pub fn new(event_store: ES) -> Self {
        Self {
            store: Arc::new(event_store),
            compaction_chunk_size: DEFAULT_COMPACTION_CHUNK_SIZE,
            middlewares: Arc::new([]),
        }
    }

    /// Register a command middleware, after every one registered before it.
    ///
    /// Every command [`execute`](Self::execute) runs — an application's call and a
    /// Policy dispatch alike — passes through each middleware in registration order
    /// (ADR-0035). Clones of this `Cqrs` made afterwards share the chain. A `Cqrs` with
    /// no middleware behaves as if the feature did not exist.
    pub fn with_middleware<M: CommandMiddleware + 'static>(mut self, middleware: M) -> Self {
        let mut chain = self.middlewares.to_vec();
        chain.push(Arc::new(middleware));
        self.middlewares = chain.into();
        self
    }

    /// Cap how many stream ids [`streams_needing_compaction`](Self::streams_needing_compaction)
    /// reads per store query (the `LIMIT` of each
    /// [`EventStore::streams_needing_compaction_page`](crate::EventStore::streams_needing_compaction_page)).
    ///
    /// It bounds the ids held at once and how long one query runs; the consumer still
    /// sees one stream and decides how much of it to read. Defaults to 100. Values below
    /// 1 are clamped to 1, so every query makes progress.
    pub fn compaction_chunk_size(mut self, ids: u32) -> Self {
        self.compaction_chunk_size = ids.max(1);
        self
    }

    /// Shared handle to the underlying event store.
    pub(crate) fn store(&self) -> &Arc<ES> {
        &self.store
    }

    /// Reconstruct an aggregate from its persisted event stream.
    ///
    /// - `aggregate_version`: choose which snapshot of the stream to load.  Use
    ///   [`AggregateVersion::Latest`] (the default) to replay the current event stream; use
    ///   [`AggregateVersion::Version`]`(n)` to inspect a specific archived compaction version.
    /// - `at_stream_version`: optional inclusive upper bound on the event sequence number
    ///   (events with version ≤ n are included).  Use this for time-travel reads.
    /// - `at_timestamp`: optional inclusive upper bound on the event creation timestamp
    ///   (events created at or before the given instant are included).
    pub async fn fetch_aggregate_at<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
        aggregate_version: AggregateVersion,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<A, A::Error> {
        let events = self
            .store
            .stream_events_by_stream_id::<A>(id, aggregate_version, at_stream_version, at_timestamp)
            .map_err(A::Error::from);

        let mut stream = A::with_id(id.clone());

        futures::pin_mut!(events);

        while let Some(event) = events.try_next().await? {
            stream.apply(event.into_data());
        }

        Ok(stream)
    }

    /// Reconstruct an aggregate at its latest state.
    ///
    /// This is a convenience wrapper around [`Self::fetch_aggregate_at`] that
    /// always replays the current (latest) event stream without any stream-version
    /// or timestamp upper bound.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let account = cqrs.fetch_aggregate::<BankAccountAggregate>(&account_id).await?;
    /// println!("balance: {}", account.balance);
    /// ```
    pub async fn fetch_aggregate<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
    ) -> Result<A, A::Error> {
        self.fetch_aggregate_at(id, AggregateVersion::Latest, None, None)
            .await
    }

    /// Run `command` against the aggregate at `id` and append the events it produces.
    ///
    /// Each registered [command middleware](Self::with_middleware) runs its `before`
    /// first, in registration order. The first that does not answer
    /// [`Next::Continue`] stops the command — nothing is loaded, handled or appended,
    /// and later middlewares do not run: [`Next::Busy`] reaches the caller as a
    /// `replay::Error` of kind [`Busy`](replay::ErrorKind::Busy) carrying its reason,
    /// [`Next::Error`] as the error it carries. Metadata a middleware adds is carried by
    /// the appended events; a middleware can change nothing else about the command.
    ///
    /// Once the command has finished, each middleware whose `before` answered `Continue`
    /// runs its `after`, in reverse registration order, with the [`Outcome`]: committed,
    /// busy with the reason, or failed with the error this call returns. A middleware that
    /// stopped the command, and every one after it, runs no `after`. `after` only
    /// observes: what it does never changes what this call returns.
    ///
    /// Each middleware's `before_commit` then runs at the append's commit gate, in
    /// registration order, after the events and every inline projection write are in
    /// place: the first that does not continue rolls the whole append back and reaches the
    /// caller the same way.
    pub async fn execute<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<A, A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        self.through_middleware::<A, _, _, _>(id, metadata, |metadata, gate| {
            self.run::<A>(id, metadata, command, services, expected_version, gate)
        })
        .await
    }

    /// Pass a command through the middleware chain around `command`, which does the
    /// work with the metadata the middlewares leave and appends behind the
    /// [`MiddlewareGate`] it is handed: what [`execute`](Self::execute) documents, shared
    /// by every way a command is executed.
    async fn through_middleware<A, T, F, Fut>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: F,
    ) -> Result<T, A::Error>
    where
        A: Aggregate,
        A::Error: 'static,
        F: FnOnce(replay::Metadata, MiddlewareGate) -> Fut,
        Fut: std::future::Future<Output = Result<T, A::Error>>,
    {
        if self.middlewares.is_empty() {
            return command(metadata, MiddlewareGate::open()).await;
        }

        let mut context = CommandContext::new::<A>(id, metadata);
        // How many middlewares answered `Continue`: the ones whose `after` runs.
        let mut continued = 0;
        // The reason a middleware gave when it answered `Busy`, in `before` or at the gate.
        let mut busy = None;

        let result = 'command: {
            for middleware in self.middlewares.iter() {
                // The handle lets `before` change the metadata, through its checked
                // setters, and nothing else: the command's identity and causation stay
                // as they arrived.
                match middleware
                    .before(CommandContextMut::new(&mut context))
                    .await
                {
                    Next::Continue => continued += 1,
                    Next::Busy(reason) => {
                        let error = replay::Error::busy(reason.clone())
                            .with_operation("execute")
                            .with_context("stream_id", context.stream_id())
                            .with_context("command", context.command_type());
                        busy = Some(reason);
                        break 'command Err(A::Error::from(error));
                    }
                    Next::Error(error) => break 'command Err(A::Error::from(error)),
                }
            }
            let gate = MiddlewareGate {
                middlewares: Arc::clone(&self.middlewares),
                // The context `before` left, owned by the gate so the append can hold it.
                context: Some(Arc::new(context.clone())),
                busy: Arc::default(),
                stopped: Arc::default(),
            };
            let gate_busy = Arc::clone(&gate.busy);
            let result = command(context.metadata().clone(), gate).await;
            // A `Busy` at the commit gate is as busy as one in `before`.
            busy = busy.or_else(|| gate_busy.get().cloned());
            result
        };

        let outcome = match (&result, &busy) {
            (Ok(_), _) => Outcome::Committed,
            (Err(_), Some(reason)) => Outcome::Busy(reason),
            (Err(error), None) => Outcome::Failed(error),
        };
        for middleware in self.middlewares[..continued].iter().rev() {
            middleware.after(&context, &outcome).await;
        }

        result
    }

    /// Load the aggregate, handle `command` and append what it produces behind `gate`:
    /// what [`execute`](Self::execute) runs once every `before` continued.
    async fn run<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
        gate: MiddlewareGate,
    ) -> Result<A, A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        // Always load the latest (current) event stream for command handling.
        let mut aggregate = self
            .fetch_aggregate_at::<A>(id, AggregateVersion::Latest, expected_version, None)
            .await?;

        self.handle_and_append::<A>(
            &mut aggregate,
            metadata,
            command,
            services,
            expected_version,
            gate,
            &mut false,
        )
        .await?;

        Ok(aggregate)
    }

    /// Handle `command` on the loaded `aggregate` and append what it produces behind
    /// `gate`, folding each persisted event back into it. `wrote` turns true once the
    /// first event is written, whether or not the append then commits.
    #[allow(clippy::too_many_arguments)]
    async fn handle_and_append<A: Aggregate>(
        &self,
        aggregate: &mut A,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
        gate: MiddlewareGate,
        wrote: &mut bool,
    ) -> Result<Option<(i64, i64, uuid::Uuid)>, A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        let id = aggregate.get_id().clone();
        let stream_type = A::stream_type();
        // The stream version of the last event appended and how many were, for a caller
        // that keeps the aggregate and appends against it again.
        let mut appended: Option<(i64, i64, uuid::Uuid)> = None;

        // Stream-first: the producer yields events lazily and owns its data — it does not
        // borrow the aggregate — so once it is built the borrow on `&aggregate` is released
        // and we can fold each persisted event back into the same aggregate as it streams
        // into the store (apply-as-you-stream). Events are never buffered in a `Vec`.
        //
        // A producer error is fatal and rolls back the whole append; it is surfaced to the
        // store as a `replay::Error` so the streaming contract (`Error = replay::Error`) holds.
        let event_stream = aggregate
            .handle_stream(command, services)
            .await?
            .map_err(|e| replay::Error::internal("aggregate event producer failed").with_source(e));

        self.store
            .store_events_stream::<A, _, _, _>(
                &id,
                stream_type,
                metadata,
                event_stream,
                expected_version,
                |event: &PersistedEvent<A::Event>| {
                    *wrote = true;
                    let count = appended.map_or(1, |(_, n, _)| n + 1);
                    appended = Some((event.version, count, event.id));
                    aggregate.apply(event.data.clone())
                },
                gate,
            )
            .await
            .map_err(A::Error::from)?;

        Ok(appended)
    }

    /// Compact the event stream for an aggregate.
    ///
    /// Archives the current full history under a new version number, then replaces
    /// the live stream with the minimal set of events returned by
    /// [`Compactable::compacted_events`](replay::Compactable::compacted_events).  Returns a [`CompactionOutcome`]:
    /// `Compacted { archive_version }` (starting at `1` for the first compaction), or
    /// `Skipped` when the aggregate reports the stream is already minimal
    /// ([`replay::Compaction::AlreadyCompacted`]) — in which case nothing is written but the
    /// compaction watermark is still advanced.
    ///
    /// See [`EventStore::compact`] for the full description of the algorithm.
    pub async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<CompactionOutcome, replay::Error>
    where
        A: replay::Aggregate + replay::Compactable + Sync,
    {
        self.store.compact(aggregate, metadata).await
    }

    /// Whether `id`'s stream has changed since it was last compacted.
    ///
    /// Cheap pre-check for a blanket compaction job: skip [`compact`](Self::compact)
    /// when this returns `false`, avoiding the transaction, lock, and fold for an
    /// unchanged stream. See [`EventStore::needs_compaction`](crate::EventStore::needs_compaction).
    pub async fn needs_compaction<A>(&self, id: &A::StreamId) -> Result<bool, replay::Error>
    where
        A: replay::Aggregate,
    {
        let stream_id: Urn = id.clone().into();
        self.store.needs_compaction(&stream_id).await
    }

    /// Every stream of aggregate `A` within `scope` that
    /// [`needs_compaction`](Self::needs_compaction) would answer `true` for, in stream-id
    /// order, starting after `after` (`None` for the beginning).
    ///
    /// The batch form of the pre-check, for a compaction pass over a whole scope — for
    /// example every stream of one tenant, `StreamScope::within(&tenant)`:
    ///
    /// ```rust,ignore
    /// let due = cqrs.streams_needing_compaction::<Account>(StreamScope::within(&tenant), None);
    /// futures::pin_mut!(due);
    /// while let Some(id) = due.try_next().await? {
    ///     let account = cqrs.fetch_aggregate::<Account>(&id).await?;
    ///     cqrs.compact(&account, metadata.clone()).await?;
    /// }
    /// ```
    ///
    /// Same watermark as `needs_compaction`, so a stream whose compaction was `Skipped`
    /// drops out of the next pass until a new event is appended. The stream reads in
    /// short keyset chunks of [`compaction_chunk_size`](Self::compaction_chunk_size) ids
    /// rather than through one long query, so a pass that holds it for minutes holds no connection or snapshot between chunks
    /// (`docs/adr/0029-the-public-api-is-stream-first.md`); a caller that cannot hold it
    /// across requests resumes with `after` set to the last id it saw, and takes a page
    /// with `.take(n)`. `scope` selects by stream id only; see [`crate::StreamScope`].
    pub fn streams_needing_compaction<A>(
        &self,
        scope: crate::StreamScope,
        after: Option<A::StreamId>,
    ) -> impl Stream<Item = Result<A::StreamId, replay::Error>> + Send
    where
        A: replay::Aggregate,
    {
        struct Pass<ES> {
            store: Arc<ES>,
            stream_type: String,
            scope: crate::StreamScope,
            cursor: Option<Urn>,
            // Bounded by `chunk_size`: one store page at a time, never the whole scope.
            chunk: std::vec::IntoIter<Urn>,
            last_chunk: bool,
            chunk_size: u32,
        }

        let pass = Pass {
            store: self.store.clone(),
            stream_type: A::stream_type(),
            scope,
            cursor: after.map(Into::into),
            chunk: Vec::new().into_iter(),
            last_chunk: false,
            chunk_size: self.compaction_chunk_size,
        };

        futures::stream::try_unfold(pass, |mut pass| async move {
            loop {
                if let Some(urn) = pass.chunk.next() {
                    let id = A::StreamId::try_from(urn.clone()).map_err(|e| {
                        replay::Error::internal(
                            "stored stream id does not parse as the aggregate's StreamId",
                        )
                        .with_operation("streams_needing_compaction")
                        .with_context("stream_id", urn.to_string())
                        .with_context("error", format!("{:?}", e))
                    })?;
                    return Ok(Some((id, pass)));
                }
                if pass.last_chunk {
                    return Ok(None);
                }
                let chunk = pass
                    .store
                    .streams_needing_compaction_page(
                        &pass.stream_type,
                        &pass.scope,
                        pass.cursor.as_ref(),
                        pass.chunk_size,
                    )
                    .await?;
                pass.last_chunk = chunk.len() < pass.chunk_size as usize;
                if let Some(last) = chunk.last() {
                    pass.cursor = Some(last.clone());
                }
                pass.chunk = chunk.into_iter();
            }
        })
    }

    pub async fn run_query<'a, Q, E>(&'a self, query: &'a mut Q) -> Result<(), replay::Error>
    where
        E: Event + 'a,
        Q: crate::Query<Event = E>,
    {
        let events = self
            .store
            .stream_events::<E>(query.stream_filter())
            .into_stream();

        futures::pin_mut!(events);

        while let Some(Ok(event)) = events.next().await {
            query.update(event);
        }

        Ok(())
    }
}

impl Cqrs<PostgresEventStore> {
    /// Run `command` against the aggregate at `id` on the **bulk lane**, and append the
    /// events it produces stamped bulk
    /// ([ADR-0034](https://github.com/funkode-io/replay/blob/main/docs/adr/0034-a-bulk-append-reaches-policies-through-a-work-queue.md)).
    ///
    /// For a decision that is bulk by design — a file import whose every accepted record
    /// is an event the same Policies react to as one accepted from the UI. Everything
    /// [`execute`](Self::execute) documents holds — the middleware chain, the load, the
    /// handler, inline projections, optimistic concurrency — with two differences:
    ///
    /// - **No online Policy reacts to these events.** The online feed passes them as it
    ///   passes a row its stream filter excludes.
    /// - **Each Policy registered on the bulk lane** (see
    ///   [`PolicySettings::on_lanes`](crate::PolicySettings::on_lanes)) whose tracked
    ///   stream types include this stream gets one row of bulk work per event, inserted
    ///   in the append's own transaction. Its worker claims that work in batches, calls
    ///   the same `react` on each event, dispatches and deletes it.
    ///
    /// The events are committed in **segments** of
    /// `bulk_segment_size` events, set on [`PostgresEventStore::builder`](crate::PostgresEventStore::builder)
    /// (`REPLAY_BULK_SEGMENT_SIZE`, default 1000), each segment one transaction holding its
    /// events, their inline-projection rows and their bulk work. The handler's stream is
    /// consumed as it is written, so peak memory is the segment, whatever the size of the
    /// bulk; and because each segment ends, a bulk never holds back an online Policy or a
    /// projection catch-up for longer than one segment takes to write. Every middleware's
    /// `before_commit` is asked before each segment commits.
    ///
    /// **The call is not all or nothing.** A failure — the handler's stream returning an
    /// error, a write, an inline projection or a middleware stopping the commit — rolls
    /// back the segment it is in and is returned; the segments before it stay committed,
    /// with their work queued, which is the outcome the domain would have had issuing them
    /// as separate commands. A rerun must therefore be idempotent at the command, as any
    /// command must; and if consumers need to know a bulk is complete, the aggregate
    /// records that with an ordinary event once this returns. The expected version is
    /// checked against the stream's head before the first segment only: another writer
    /// may append to the stream between segments.
    ///
    /// A Policy registered after a segment commits never receives that segment.
    pub async fn execute_bulk<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<A, A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        self.through_middleware::<A, _, _, _>(id, metadata, |metadata, gate| async move {
            let mut aggregate = self
                .fetch_aggregate_at::<A>(id, AggregateVersion::Latest, expected_version, None)
                .await?;
            let stream_type = A::stream_type();

            // As in `handle_and_append`: the producer owns its data, so each persisted
            // event is folded back into the aggregate as it streams into the store.
            let event_stream = aggregate
                .handle_stream(command, services)
                .await?
                .map_err(|e| {
                    replay::Error::internal("aggregate event producer failed").with_source(e)
                });

            self.store
                .store_bulk_events_stream::<A, _, _, _>(
                    id,
                    stream_type,
                    metadata,
                    event_stream,
                    expected_version,
                    |event: &PersistedEvent<A::Event>| aggregate.apply(event.data.clone()),
                    gate,
                )
                .await
                .map_err(A::Error::from)?;

            Ok(aggregate)
        })
        .await
    }

    /// Run `command` against the aggregate at `id`, reusing the instance in `cached`
    /// when it is that aggregate and its stream has not moved, and leave the aggregate
    /// there once the command commits: [`execute`](Self::execute) with the load shared
    /// across commands.
    ///
    /// What lets the bulk lane's worker dispatch every command a batch aims at one
    /// stream with one load of its aggregate (ADR-0034). The load happens inside the
    /// middleware chain, as in `execute`, so a middleware that stops the command stops
    /// the load too.
    ///
    /// A cached instance is reused only while the stream's head is the one it was loaded
    /// or last appended at — compared by `stream_seq`, which a compaction does not reset,
    /// and by the head event, which a recreated stream does not share — checked before
    /// the handler decides. A write landing after that check cannot pass either: an
    /// append is made against the cached `version`, unless the command names its own,
    /// and fails as a `Conflict`; an outcome that appends nothing, a refusal included, is
    /// checked against the head again and becomes a `Conflict` if it moved. A `Conflict`
    /// is retried on a fresh load. On any failure `cached` is left empty, because the
    /// instance may hold events whose append rolled back.
    pub(crate) async fn execute_cached<A: Aggregate>(
        &self,
        cached: &mut Option<Loaded<A>>,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<(), A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        let reused = cached
            .take()
            .filter(|loaded| loaded.aggregate.get_id() == id);
        self.through_middleware::<A, _, _, _>(id, metadata, |metadata, gate| async move {
            let mut loaded = match reused {
                Some(loaded) if self.head_of::<A>(id).await? == loaded.head => loaded,
                _ => self.fetch_loaded::<A>(id).await?,
            };
            let expected = expected_version.or(Some(loaded.head.version));
            let gate_stopped = Arc::clone(&gate.stopped);
            let mut wrote = false;
            let appended = self
                .handle_and_append::<A>(
                    &mut loaded.aggregate,
                    metadata,
                    command,
                    services,
                    expected,
                    gate,
                    &mut wrote,
                )
                .await;
            // A decision that wrote nothing — a refusal, a no-op, a handler's failure — met
            // no version check, so it is only as good as the state it was decided on. Read
            // the head again: a stream that moved while the handler decided makes the
            // outcome a `Conflict`, and the caller retries on a fresh load. A decision that
            // wrote was checked at its first event; and a commit gate that stopped the
            // command has the last word, so a `Busy` there stays `Busy`.
            let stopped_at_gate = gate_stopped.load(std::sync::atomic::Ordering::SeqCst);
            if !wrote && !stopped_at_gate {
                let now = self.head_of::<A>(id).await?;
                if now != loaded.head {
                    return Err(A::Error::from(
                        crate::concurrency_error(
                            id.clone().into(),
                            loaded.head.version,
                            now.version,
                        )
                        .with_operation("execute_cached"),
                    ));
                }
            }
            if let Some((version, count, event)) = appended? {
                // The append was checked against the cached version, so the head moved by
                // exactly what it wrote.
                loaded.head = StreamHead {
                    version,
                    stream_seq: loaded.head.stream_seq + count,
                    event: Some(event),
                };
            }
            *cached = Some(loaded);
            Ok(())
        })
        .await
    }

    /// The head of the stream at `id`; the default — version and place 0 — for a stream
    /// that does not exist yet.
    async fn head_of<A: Aggregate>(&self, id: &A::StreamId) -> Result<StreamHead, A::Error> {
        let stream_id: Urn = id.clone().into();
        // The head event through `uidx_events_stream_seq`; none for a stream whose head
        // place holds no live event, which every reader of it then sees the same way.
        let head: Option<(i64, i64, Option<uuid::Uuid>)> = sqlx::query_as(
            "SELECT s.version, s.stream_seq, e.id FROM streams s \
               LEFT JOIN events e ON e.stream_id = s.id AND e.stream_seq = s.stream_seq \
              WHERE s.id = $1",
        )
        .bind(stream_id.to_string())
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| A::Error::from(crate::db_error(error)))?;
        Ok(
            head.map_or_else(StreamHead::default, |(version, stream_seq, event)| {
                StreamHead {
                    version,
                    stream_seq,
                    event,
                }
            }),
        )
    }

    /// Load the aggregate at `id` at its latest state, with the head that state is at.
    ///
    /// The head is read before the events, so a write landing between the two leaves it
    /// behind the state rather than ahead of it: the next reuse sees a head that moved
    /// and loads again, which costs a load and never a stale decision.
    async fn fetch_loaded<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
    ) -> Result<Loaded<A>, A::Error> {
        let head = self.head_of::<A>(id).await?;
        let aggregate = self.fetch_aggregate::<A>(id).await?;
        Ok(Loaded { aggregate, head })
    }
}
