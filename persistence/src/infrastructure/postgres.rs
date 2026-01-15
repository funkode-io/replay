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
use replay::{Event, Metadata};

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
            StreamFilter::CreatedAfter(timestamp) => {
                query_builder.push(" created > ").push_bind(timestamp);
            }
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
    async fn store_events<S: replay::Stream>(
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
            let sql = "SELECT id, data, metadata, stream_id, type, version, created
                FROM events 
                WHERE " ;

            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(sql);
            Self::add_filters(&mut query_builder, filter.clone());

            let query_builder = query_builder.push(" ORDER BY created, version ASC");

            //println!("Executing query: {}", query_builder.sql());

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

            tracing::info!("Streamed {} events from Postgres", count);
        }
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

        Ok(PersistedEvent {
            id,
            data,
            stream_id,
            r#type,
            version,
            created,
            metadata,
        })
    }
}
