use std::sync::Arc;

use futures::{StreamExt, TryStream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{
    postgres::PgRow,
    types::chrono::{self, Utc},
    Pool, Postgres, QueryBuilder, Row,
};
use tokio::sync::Mutex;

use urn::Urn;
use uuid::Uuid;

use crate::inline_projection::{ErasedInlineProjection, InlineProjection};
use crate::{EventStore, PersistedEvent, StreamFilter};
use replay::{Compactable, Event, Metadata};

/// A registered inline projection, guarded by a mutex so its `&mut self`
/// `handle`/`init` methods can be driven through the shared (`&self`) store.
type RegisteredProjection = Mutex<Box<dyn ErasedInlineProjection<Exec = sqlx::PgConnection>>>;

pub struct PostgresEventStore {
    pool: Pool<Postgres>,
    /// Builder-fixed, immutable set of inline projections. The `Vec` itself never
    /// changes after `build()`; each projection is individually locked while applied.
    projections: Arc<Vec<RegisteredProjection>>,
}

impl PostgresEventStore {
    pub fn new(pool: Pool<Postgres>) -> PostgresEventStore {
        PostgresEventStore {
            pool,
            projections: Arc::new(Vec::new()),
        }
    }

    /// Begin configuring a store with inline projections.
    ///
    /// Projections are registered on the builder and frozen by [`PostgresEventStoreBuilder::build`],
    /// which runs their first-time `init` and records their version in the `projections` table.
    pub fn builder(pool: Pool<Postgres>) -> PostgresEventStoreBuilder {
        PostgresEventStoreBuilder {
            pool,
            projections: Vec::new(),
        }
    }

    fn add_filters(query_builder: &mut QueryBuilder<Postgres>, filter: StreamFilter) {
        match filter {
            StreamFilter::All => {
                query_builder.push(" 1 = 1");
            }
            StreamFilter::WithStreamId(stream_id) => {
                query_builder
                    .push(" stream_id = ")
                    .push_bind(stream_id.to_string());
            }
            StreamFilter::ForStreamTypes(stream_types) => {
                query_builder.push(" stream_id IN (select id from streams where type IN (");

                let mut separated = query_builder.separated(", ");

                for stream_type in stream_types.clone().into_iter() {
                    separated.push_bind(stream_type);
                }

                separated.push_unseparated("))");
            }
            StreamFilter::WithMetadata(metadata) => {
                query_builder
                    .push(" metadata @> ")
                    .push_bind(metadata.to_json());
            }
            StreamFilter::AfterVersion(version) => {
                query_builder.push(" version > ").push_bind(version);
            }
            StreamFilter::UpToVersion(version) => {
                query_builder.push(" version <= ").push_bind(version);
            }
            StreamFilter::CreatedAfter(timestamp) => {
                query_builder.push(" created > ").push_bind(timestamp);
            }
            StreamFilter::CreatedBefore(timestamp) => {
                query_builder.push(" created <= ").push_bind(timestamp);
            }
            StreamFilter::WithAggregateVersion(v) => match v {
                None => {
                    query_builder.push(" aggregate_version IS NULL");
                }
                Some(version) => {
                    query_builder
                        .push(" aggregate_version = ")
                        .push_bind(version);
                }
            },
            StreamFilter::And(left, right) => {
                query_builder.push(" (");
                Self::add_filters(query_builder, *left);
                query_builder.push(")");

                query_builder.push(" AND ");

                query_builder.push(" (");
                Self::add_filters(query_builder, *right);
                query_builder.push(")");
            }
            StreamFilter::Or(left, right) => {
                query_builder.push(" (");
                Self::add_filters(query_builder, *left);
                query_builder.push(")");

                query_builder.push(" OR ");

                query_builder.push(" (");
                Self::add_filters(query_builder, *right);
                query_builder.push(")");
            }
            StreamFilter::Not(filter) => {
                query_builder.push(" NOT (");
                Self::add_filters(query_builder, *filter);
                query_builder.push(")");
            }
        }
    }
}

/// Builder for a [`PostgresEventStore`] with inline projections.
///
/// Register projections with [`register`](Self::register), then call
/// [`build`](Self::build) to run first-time `init` and freeze the registry.
pub struct PostgresEventStoreBuilder {
    pool: Pool<Postgres>,
    projections: Vec<Box<dyn ErasedInlineProjection<Exec = sqlx::PgConnection>>>,
}

impl PostgresEventStoreBuilder {
    /// Register an inline projection.
    pub fn register<P>(mut self, projection: P) -> Self
    where
        P: InlineProjection<Exec = sqlx::PgConnection> + 'static,
    {
        self.projections.push(Box::new(projection));
        self
    }

    /// Run first-time setup for the registered projections and freeze the store.
    ///
    /// For each projection that is not yet present in the `projections` table, runs its
    /// `init` and inserts its version row. All setup happens in a single transaction, so a
    /// failure leaves the registry table untouched.
    ///
    /// Version-drift handling (reset + replay) and the stored-newer-than-code guard are
    /// added by later slices; this build only covers first-time registration.
    pub async fn build(self) -> Result<PostgresEventStore, replay::Error> {
        let mut tx = self.pool.begin().await.map_err(crate::db_error)?;

        let mut registered: Vec<RegisteredProjection> = Vec::with_capacity(self.projections.len());

        for mut projection in self.projections {
            let existing: Option<i32> =
                sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
                    .bind(projection.name())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(crate::db_error)?;

            if existing.is_none() {
                projection.init(&mut tx).await?;

                sqlx::query("INSERT INTO projections (name, version) VALUES ($1, $2)")
                    .bind(projection.name())
                    .bind(projection.version())
                    .execute(&mut *tx)
                    .await
                    .map_err(crate::db_error)?;
            }

            registered.push(Mutex::new(projection));
        }

        tx.commit().await.map_err(crate::db_error)?;

        Ok(PostgresEventStore {
            pool: self.pool,
            projections: Arc::new(registered),
        })
    }
}

impl PostgresEventStore {
    /// Apply the just-appended events to every registered inline projection, inside the
    /// store's open transaction.
    ///
    /// The DB-assigned `version`/`created` for each appended event are read back (by id)
    /// so projections receive faithful `PersistedEvent`s. Each projection is locked
    /// individually and routes the batch by deserialize-or-skip; any projection error
    /// propagates and rolls back the whole append.
    async fn apply_projections(
        &self,
        conn: &mut sqlx::PgConnection,
        stream_id: &Urn,
        metadata: &Metadata,
        appended: Vec<(Uuid, String, Value)>,
    ) -> Result<(), replay::Error> {
        let mut events: Vec<PersistedEvent<Value>> = Vec::with_capacity(appended.len());

        for (id, event_type, data) in appended {
            let row = sqlx::query("SELECT version, created FROM events WHERE id = $1")
                .bind(id)
                .fetch_optional(&mut *conn)
                .await
                .map_err(crate::db_error)?;

            // Absent only when the append did not insert a row (e.g. a concurrency
            // conflict swallowed by append_event — see follow-up issue). Skip it here.
            let Some(row) = row else { continue };

            let version: i64 = row.get("version");
            let created: chrono::DateTime<Utc> = row.get("created");

            events.push(PersistedEvent {
                id,
                data,
                stream_id: stream_id.clone(),
                r#type: event_type,
                version,
                created,
                metadata: metadata.clone(),
                aggregate_version: None,
            });
        }

        if events.is_empty() {
            return Ok(());
        }

        for projection in self.projections.iter() {
            let mut projection = projection.lock().await;
            projection.handle(&mut *conn, &events).await?;
        }

        Ok(())
    }
}

impl EventStore for PostgresEventStore {
    async fn store_events<S: replay::EventStream>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> Result<(), replay::Error> {
        let mut transaction = self.pool.begin().await.map_err(crate::db_error)?;
        let stream_id: Urn = stream_id.clone().into();

        // Track the appended events so registered inline projections can be applied
        // inside this same transaction. We only retain what's needed to rebuild the
        // PersistedEvent: the generated id, the JSON payload and the event type.
        let has_projections = !self.projections.is_empty();
        let mut appended: Vec<(Uuid, String, Value)> = if has_projections {
            Vec::with_capacity(domain_events.len())
        } else {
            Vec::new()
        };

        for event in domain_events {
            let event_type = event.event_type().clone();
            let event = serde_json::to_value(event).map_err(crate::ser_error)?;
            let id = Uuid::new_v4();

            sqlx::query!(
                "SELECT append_event($1, $2, $3, $4, $5, $6, $7) ",
                id,
                event,
                metadata.to_json(),
                event_type,
                stream_id.to_string(),
                stream_type,
                expected_version
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(crate::db_error)?;

            if has_projections {
                appended.push((id, event_type, event));
            }
        }

        if has_projections {
            self.apply_projections(&mut transaction, &stream_id, &metadata, appended)
                .await?;
        }

        transaction.commit().await.map_err(crate::db_error)?;
        Ok(())
    }

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send {
        async_stream::stream! {
            let sql = "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version
                FROM events 
                WHERE " ;

            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(sql);
            Self::add_filters(&mut query_builder, filter.clone());

            let query_builder = query_builder.push(" ORDER BY created, version ASC");

            let mut rows = query_builder
                .build()
                .fetch(&self.pool)
                .map_err(|e: sqlx::Error| crate::db_error(e).with_operation("fetching events from Postgres").with_context("filter", format!("{:?}", filter)))
                .map(|result| async {
                    result.and_then(PersistedEvent::<E>::try_from)
                }).buffered(4);

            let mut count = 0;

            while let Some(row) = rows.try_next().await? {
                count += 1;
                yield Ok(row);
            }

            tracing::debug!("Streamed {} events from Postgres", count);
        }
    }

    async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<i32, replay::Error>
    where
        A: replay::Aggregate + Compactable + Sync,
    {
        let stream_id: Urn = aggregate.get_id().clone().into();
        let stream_id_str = stream_id.to_string();

        let mut tx = self.pool.begin().await.map_err(crate::db_error)?;

        // 1. Lock the stream row for the duration of this transaction.
        //    Any concurrent `append_event` call that updates (or inserts into) this stream
        //    will block on this lock and only proceed after we commit, so no events can
        //    be appended between the read and the archive steps.
        //    If the stream does not exist (0 rows matched) we return NotFound immediately
        //    rather than silently producing an empty compaction for a phantom aggregate.
        let lock_result = sqlx::query("SELECT id FROM streams WHERE id = $1 FOR UPDATE")
            .bind(&stream_id_str)
            .execute(&mut *tx)
            .await
            .map_err(crate::db_error)?;

        if lock_result.rows_affected() == 0 {
            return Err(replay::Error::not_found("Stream not found")
                .with_operation("compact")
                .with_context("stream_id", stream_id_str));
        }

        // 2. Stream the current live events inside the transaction (now protected by the lock),
        //    processing rows one at a time so the full history is never held in memory.
        let event_stream = sqlx::query(
            "SELECT data FROM events WHERE stream_id = $1 AND aggregate_version IS NULL ORDER BY version",
        )
        .bind(&stream_id_str)
        .fetch(&mut *tx)
        .map_err(crate::db_error)
        .and_then(|row: PgRow| async move {
            let data: serde_json::Value = row.get("data");
            serde_json::from_value::<A::Event>(data).map_err(crate::deser_error)
        });

        let compacted = aggregate.compacted_events(event_stream).await?;

        // 3. Determine the next archive version number.
        let next_version: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(aggregate_version), 0) + 1 FROM events WHERE stream_id = $1",
        )
        .bind(&stream_id_str)
        .fetch_one(&mut *tx)
        .await
        .map_err(crate::db_error)?;

        // 4. Archive all current (un-versioned) events for this stream.
        sqlx::query(
            "UPDATE events SET aggregate_version = $1 WHERE stream_id = $2 AND aggregate_version IS NULL",
        )
        .bind(next_version)
        .bind(&stream_id_str)
        .execute(&mut *tx)
        .await
        .map_err(crate::db_error)?;

        // 5. Reset the stream's version counter so compacted events start from 1.
        sqlx::query("UPDATE streams SET version = 0 WHERE id = $1")
            .bind(&stream_id_str)
            .execute(&mut *tx)
            .await
            .map_err(crate::db_error)?;

        // 6. Insert compacted events as the new current stream (aggregate_version = NULL).
        let stream_type = A::stream_type();
        let meta_json = metadata.to_json();
        for (seq, event) in compacted.iter().enumerate() {
            let event_type = event.event_type();
            let data = serde_json::to_value(event).map_err(crate::ser_error)?;
            let version = (seq as i64) + 1;

            sqlx::query(
                "INSERT INTO events (id, data, metadata, stream_id, type, version, aggregate_version)
                 VALUES ($1, $2, $3, $4, $5, $6, NULL)",
            )
            .bind(Uuid::new_v4())
            .bind(&data)
            .bind(&meta_json)
            .bind(&stream_id_str)
            .bind(&event_type)
            .bind(version)
            .execute(&mut *tx)
            .await
            .map_err(crate::db_error)?;
        }

        // 7. Update the stream version to the count of compacted events.
        let new_stream_version = compacted.len() as i64;
        sqlx::query("UPDATE streams SET version = $1, type = $2 WHERE id = $3")
            .bind(new_stream_version)
            .bind(&stream_type)
            .bind(&stream_id_str)
            .execute(&mut *tx)
            .await
            .map_err(crate::db_error)?;

        tx.commit().await.map_err(crate::db_error)?;

        Ok(next_version)
    }
}

impl Clone for PostgresEventStore {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            projections: self.projections.clone(),
        }
    }
}

impl<D: DeserializeOwned> TryFrom<PgRow> for PersistedEvent<D> {
    type Error = replay::Error;

    fn try_from(value: PgRow) -> Result<Self, replay::Error> {
        let id: Uuid = value.get("id");

        let data_raw: Value = value.get("data");
        let data: D = serde_json::from_value(data_raw.clone()).map_err(|e| {
            crate::deser_error(e)
                .with_context("operation", "serde json from store")
                .with_context("stored_json", data_raw.clone())
        })?;

        // should not panic as we only store urns
        let stream_id_string: String = value.get("stream_id");
        let stream_id: Urn = Urn::try_from(stream_id_string).unwrap();
        let r#type: String = value.get("type");
        let version: i64 = value.get("version");
        let created: chrono::DateTime<Utc> = value.get("created");
        let metadata: Value = value.get("metadata");
        let metadata: Metadata = Metadata::new(metadata);
        let aggregate_version: Option<i32> = value.get("aggregate_version");

        Ok(PersistedEvent {
            id,
            data,
            stream_id,
            r#type,
            version,
            created,
            metadata,
            aggregate_version,
        })
    }
}
