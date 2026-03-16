use futures::{StreamExt, TryStream, TryStreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::{
    postgres::PgRow,
    types::chrono::{self, Utc},
    Pool, Postgres, QueryBuilder, Row,
};

use urn::Urn;
use uuid::Uuid;

use crate::{EventStore, PersistedEvent, StreamFilter};
use replay::{Compactable, Event, Metadata};

pub struct PostgresEventStore {
    pool: Pool<Postgres>,
}

impl PostgresEventStore {
    pub fn new(pool: Pool<Postgres>) -> PostgresEventStore {
        PostgresEventStore { pool }
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

        for event in domain_events {
            let event_type = event.event_type().clone();
            let event = serde_json::to_value(event).map_err(crate::ser_error)?;

            sqlx::query!(
                "SELECT append_event($1, $2, $3, $4, $5, $6, $7) ",
                Uuid::new_v4(),
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

        // 2. Read the current live events inside the transaction (now protected by the lock).
        //    Using fetch_all here is intentional: compaction is a maintenance operation and
        //    the lock must be held for the entire read + archive window, which precludes
        //    interleaving other queries on the same connection.
        let rows = sqlx::query(
            "SELECT data FROM events WHERE stream_id = $1 AND aggregate_version IS NULL ORDER BY version",
        )
        .bind(&stream_id_str)
        .fetch_all(&mut *tx)
        .await
        .map_err(crate::db_error)?;

        let events: Vec<Result<A::Event, replay::Error>> = rows
            .into_iter()
            .map(|row: sqlx::postgres::PgRow| {
                let data: serde_json::Value = row.get("data");
                serde_json::from_value::<A::Event>(data).map_err(crate::deser_error)
            })
            .collect();

        let event_stream = futures::stream::iter(events);
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
