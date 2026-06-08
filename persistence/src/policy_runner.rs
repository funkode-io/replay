//! The native Policy runner.
//!
//! Where [`crate::policy`] is the portable *contract* (no Postgres/tokio types),
//! this module is the server-side *runtime*: it reads the global event feed,
//! routes events through registered [`Policy`]s, executes the [`Dispatch`]es they
//! return through [`Cqrs`], stamps causation metadata, and advances each policy's
//! persisted cursor.
//!
//! This is the #77 walking skeleton: a single, manual [`PolicyRunner::drain`].
//! The background daemon, leadership election, batching, and NOTIFY wake-ups are
//! later slices that build on this substrate.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use futures::future::BoxFuture;
use serde_json::{Map, Value};
use sqlx::{Pool, Postgres, QueryBuilder, Row};

use replay::{Aggregate, Metadata};

use crate::policy::{Dispatch, ErasedPolicy, Policy};
use crate::{Cqrs, PersistedEvent, PostgresEventStore, StreamFilter};

/// Erased, services-bound execution path for one aggregate type.
///
/// Registered via [`PolicyRunnerBuilder::register_services`], which captures the
/// concrete aggregate `A` *and* its `Services`. At drain time the runner looks up
/// the executor by the [`Dispatch`]'s [`TypeId`], hands over the opaque payload,
/// and the executor downcasts it back to `(A::StreamId, A::Command)` and runs it
/// through [`Cqrs::execute`].
trait AggregateExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        cqrs: &'a Cqrs<PostgresEventStore>,
        payload: Box<dyn Any + Send>,
        metadata: Metadata,
        expected_version: Option<i64>,
    ) -> BoxFuture<'a, Result<(), replay::Error>>;
}

struct TypedExecutor<A: Aggregate> {
    services: A::Services,
}

impl<A> AggregateExecutor for TypedExecutor<A>
where
    A: Aggregate + 'static,
    A::StreamId: 'static,
    A::Command: 'static,
    A::Services: Send + Sync + 'static,
{
    fn execute<'a>(
        &'a self,
        cqrs: &'a Cqrs<PostgresEventStore>,
        payload: Box<dyn Any + Send>,
        metadata: Metadata,
        expected_version: Option<i64>,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(async move {
            let (id, command) = *payload
                .downcast::<(A::StreamId, A::Command)>()
                .map_err(|_| {
                    replay::Error::internal("policy dispatch payload type mismatch")
                        .with_operation("policy_execute")
                })?;

            cqrs.execute::<A>(&id, metadata, command, &self.services, expected_version)
                .await
                .map(|_| ())
                .map_err(|e| {
                    replay::Error::internal(format!("policy command failed: {e}"))
                        .with_operation("policy_execute")
                })
        })
    }
}

/// Builds a [`PolicyRunner`] by registering aggregate services and policies.
pub struct PolicyRunnerBuilder {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Box<dyn ErasedPolicy>>,
    executors: HashMap<TypeId, Box<dyn AggregateExecutor>>,
}

impl PolicyRunnerBuilder {
    /// Register the `Services` for aggregate `A`, enabling policies to dispatch
    /// commands to it. The runner owns the services and injects them when it
    /// executes a [`Dispatch::to::<A>`].
    pub fn register_services<A>(mut self, services: A::Services) -> Self
    where
        A: Aggregate + 'static,
        A::StreamId: 'static,
        A::Command: 'static,
        A::Services: Send + Sync + 'static,
    {
        self.executors
            .insert(TypeId::of::<A>(), Box::new(TypedExecutor::<A> { services }));
        self
    }

    /// Register a policy. Its `name` becomes the stable cursor key.
    pub fn register_policy<P>(mut self, policy: P) -> Self
    where
        P: Policy + 'static,
    {
        self.policies.push(Box::new(policy));
        self
    }

    pub fn build(self) -> PolicyRunner {
        PolicyRunner {
            cqrs: self.cqrs,
            pool: self.pool,
            policies: self.policies,
            executors: self.executors,
        }
    }
}

/// Native runner that drives registered policies against the event feed.
pub struct PolicyRunner {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Box<dyn ErasedPolicy>>,
    executors: HashMap<TypeId, Box<dyn AggregateExecutor>>,
}

impl PolicyRunner {
    /// Begin configuring a runner bound to `cqrs` (and its connection pool).
    pub fn builder(cqrs: Cqrs<PostgresEventStore>) -> PolicyRunnerBuilder {
        let pool = cqrs.store().pool().clone();
        PolicyRunnerBuilder {
            cqrs,
            pool,
            policies: Vec::new(),
            executors: HashMap::new(),
        }
    }

    /// Manually drain every registered policy once.
    ///
    /// For each policy: read the gap-free prefix of events past its cursor,
    /// `react`, execute the returned dispatches through [`Cqrs`], and advance the
    /// cursor — one event at a time, advancing only after that event's commands
    /// have committed (at-least-once delivery; reactions must be idempotent).
    ///
    /// Returns the total number of dispatches executed across all policies.
    pub async fn drain(&self) -> Result<usize, replay::Error> {
        let mut total = 0;
        for policy in &self.policies {
            total += self.drain_policy(policy.as_ref()).await?;
        }
        Ok(total)
    }

    async fn drain_policy(&self, policy: &dyn ErasedPolicy) -> Result<usize, replay::Error> {
        let name = policy.name().to_string();
        let cursor = self.load_cursor(&name).await?;
        let feed = self.read_feed(policy.stream_filter(), cursor).await?;

        let mut executed = 0;
        for (global_position, raw) in feed {
            for dispatch in policy.react_erased(&raw) {
                self.execute_dispatch(&name, global_position, &raw, dispatch)
                    .await?;
                executed += 1;
            }
            // Advance only after this event's commands have committed. A crash
            // before this point re-delivers the event on the next drain.
            self.save_cursor(&name, global_position).await?;
        }

        Ok(executed)
    }

    /// Read the contiguous, gap-free prefix of events with
    /// `global_position > cursor` matching `filter`, in global order.
    ///
    /// BIGSERIAL positions are assigned at INSERT but become visible at COMMIT,
    /// so a higher position can appear before a lower one fills in. Stopping at
    /// the first gap guarantees we never skip an event that is still in flight.
    async fn read_feed(
        &self,
        filter: StreamFilter,
        cursor: i64,
    ) -> Result<Vec<(i64, PersistedEvent<Value>)>, replay::Error> {
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
             global_position FROM events WHERE global_position > ",
        );
        qb.push_bind(cursor);
        qb.push(" AND ");
        PostgresEventStore::add_filters(&mut qb, filter);
        qb.push(" ORDER BY global_position ASC");

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(crate::db_error)?;

        let mut feed = Vec::with_capacity(rows.len());
        let mut expected = cursor + 1;
        for row in rows {
            let global_position: i64 = row.get("global_position");
            if global_position != expected {
                // Gap: stop here and let the hole fill on a later drain.
                break;
            }
            let event = PersistedEvent::<Value>::try_from(row)?;
            feed.push((global_position, event));
            expected += 1;
        }

        Ok(feed)
    }

    async fn execute_dispatch(
        &self,
        policy_name: &str,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        dispatch: Dispatch,
    ) -> Result<(), replay::Error> {
        let executor = self.executors.get(&dispatch.target()).ok_or_else(|| {
            replay::Error::invalid_input(
                "no services registered for the aggregate targeted by a policy dispatch",
            )
            .with_operation("policy_drain")
            .with_context("policy", policy_name)
            .with_context("aggregate", dispatch.aggregate_name())
        })?;

        let aggregate_name = dispatch.aggregate_name();
        let dispatch_metadata = dispatch.metadata.clone();

        let metadata = merge_dispatch_metadata(
            causation_metadata(policy_name, global_position, raw),
            dispatch_metadata,
        )
        .map_err(|err| {
            err.with_operation("policy_drain")
                .with_context("policy", policy_name)
                .with_context("aggregate", aggregate_name)
        })?;

        executor
            .execute(
                &self.cqrs,
                dispatch.payload,
                metadata,
                dispatch.expected_version,
            )
            .await
    }

    async fn load_cursor(&self, name: &str) -> Result<i64, replay::Error> {
        let position =
            sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(crate::db_error)?;
        Ok(position.unwrap_or(0))
    }

    async fn save_cursor(&self, name: &str, position: i64) -> Result<(), replay::Error> {
        sqlx::query(
            "INSERT INTO policy_cursors (name, position, updated_at) VALUES ($1, $2, now()) \
             ON CONFLICT (name) DO UPDATE SET position = EXCLUDED.position, updated_at = now()",
        )
        .bind(name)
        .bind(position)
        .execute(&self.pool)
        .await
        .map_err(crate::db_error)?;
        Ok(())
    }
}

/// Causation metadata stamped on every command a policy issues.
///
/// Records which policy reacted and which event triggered it. This is the seed
/// of the causation chain that later slices use for idempotency (#80) and
/// loop-depth limiting (#83).
fn causation_metadata(
    policy_name: &str,
    global_position: i64,
    raw: &PersistedEvent<Value>,
) -> Metadata {
    Metadata::new(serde_json::json!({
        "causation": {
            "policy": policy_name,
            "event_id": raw.id.to_string(),
            "stream_id": raw.stream_id.to_string(),
            "global_position": global_position,
        }
    }))
}

fn merge_dispatch_metadata(
    causation: Metadata,
    dispatch: Option<Metadata>,
) -> Result<Metadata, replay::Error> {
    let Some(dispatch) = dispatch else {
        return Ok(causation);
    };

    let Value::Object(mut merged) = causation.to_json() else {
        return Err(replay::Error::internal(
            "policy causation metadata must be a JSON object",
        ));
    };

    let Value::Object(extra) = dispatch.to_json() else {
        return Err(replay::Error::invalid_input(
            "policy dispatch metadata must be a JSON object",
        ));
    };

    merge_no_collisions(&mut merged, extra)?;
    Ok(Metadata::new(Value::Object(merged)))
}

fn merge_no_collisions(
    destination: &mut Map<String, Value>,
    source: Map<String, Value>,
) -> Result<(), replay::Error> {
    for (key, value) in source {
        if destination.contains_key(&key) {
            return Err(replay::Error::invalid_input(
                "policy dispatch metadata contains a key that collides with causation metadata",
            )
            .with_context("key", key));
        }
        destination.insert(key, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use replay::Metadata;
    use serde_json::json;

    use super::merge_dispatch_metadata;

    #[test]
    fn merges_dispatch_metadata_without_collisions() {
        let causation = Metadata::new(json!({
            "causation": { "policy": "p", "global_position": 1 }
        }));
        let dispatch = Metadata::new(json!({
            "user_id": "u-1",
            "related_aggregate_id": "urn:catalog:1"
        }));

        let merged = merge_dispatch_metadata(causation, Some(dispatch)).expect("must merge");
        let value = merged.to_json();

        assert_eq!(value["causation"]["policy"], "p");
        assert_eq!(value["user_id"], "u-1");
        assert_eq!(value["related_aggregate_id"], "urn:catalog:1");
    }

    #[test]
    fn errors_on_metadata_key_collision() {
        let causation = Metadata::new(json!({ "causation": { "policy": "p" } }));
        let dispatch = Metadata::new(json!({ "causation": { "override": true } }));

        let err = merge_dispatch_metadata(causation, Some(dispatch)).expect_err("must fail");

        assert_eq!(err.kind(), replay::ErrorKind::InvalidInput);
        assert!(err
            .to_string()
            .contains("policy dispatch metadata contains a key that collides"));
    }
}
