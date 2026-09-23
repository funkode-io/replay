use chrono::{DateTime, Utc};
use urn::Urn;
use uuid::Uuid;

use replay::{Event, Metadata};

#[derive(Debug, Clone)]
pub struct PersistedEvent<E> {
    pub id: Uuid,
    pub data: E,
    pub stream_id: Urn,
    pub r#type: String,
    /// Monotonic position of this event within the aggregate's current stream.
    pub version: i64,
    pub created: DateTime<Utc>,
    pub metadata: Metadata,
    /// `None` identifies events belonging to the current (latest) stream.
    /// `Some(n)` identifies events that were archived during the nth compaction.
    /// Matches the `INTEGER` column type in the database.
    pub aggregate_version: Option<i32>,
}

impl<E: Event> PersistedEvent<E> {
    /// An event that was never read from a store: a fixture, assembled from the two
    /// fields a test is usually about.
    ///
    /// The identity, position and timestamp are invented here — a fresh v4 id, version
    /// 1, now — so nothing about them carries meaning. Only `type` is real: it comes
    /// from [`Event::event_type`], which is what the stores write, so a fixture cannot
    /// drift from stored data the way a hand-written type string does.
    ///
    /// Override the rest with [`with_version`](Self::with_version),
    /// [`with_created`](Self::with_created), [`with_metadata`](Self::with_metadata) and
    /// [`with_aggregate_version`](Self::with_aggregate_version).
    pub fn of(stream_id: impl Into<Urn>, data: E) -> Self {
        PersistedEvent {
            id: Uuid::new_v4(),
            r#type: data.event_type(),
            data,
            stream_id: stream_id.into(),
            version: 1,
            created: Utc::now(),
            metadata: Metadata::default(),
            aggregate_version: None,
        }
    }
}

impl<E> PersistedEvent<E> {
    pub fn with_version(self, version: i64) -> Self {
        PersistedEvent { version, ..self }
    }

    pub fn with_created(self, created: DateTime<Utc>) -> Self {
        PersistedEvent { created, ..self }
    }

    pub fn with_metadata(self, metadata: impl Into<Metadata>) -> Self {
        PersistedEvent {
            metadata: metadata.into(),
            ..self
        }
    }

    /// `None` is a current-stream event; `Some(n)` one archived by the nth compaction.
    pub fn with_aggregate_version(self, aggregate_version: Option<i32>) -> Self {
        PersistedEvent {
            aggregate_version,
            ..self
        }
    }

    pub fn wrap_data_with<Other: From<E>>(self) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data: Other::from(self.data),
            stream_id: self.stream_id,
            r#type: self.r#type,
            version: self.version,
            created: self.created,
            metadata: self.metadata,
            aggregate_version: self.aggregate_version,
        }
    }

    pub fn with_data<Other: Event>(self, data: Other) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data,
            stream_id: self.stream_id,
            r#type: self.r#type,
            version: self.version,
            created: self.created,
            metadata: self.metadata,
            aggregate_version: self.aggregate_version,
        }
    }

    /// Re-envelope a *borrowed* event with new data.
    ///
    /// The counterpart to [`with_data`](Self::with_data) for the erasure bridges, which
    /// only ever hold `&PersistedEvent<Value>`: the payload is not copied at all (the
    /// caller supplies it, typically deserialized by borrow from `self.data`), and the
    /// envelope's owned fields are cloned individually rather than by cloning the whole
    /// event — which would drag the JSON payload along with them.
    pub(crate) fn with_data_from<Other: Event>(&self, data: Other) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data,
            stream_id: self.stream_id.clone(),
            r#type: self.r#type.clone(),
            version: self.version,
            created: self.created,
            metadata: self.metadata.clone(),
            aggregate_version: self.aggregate_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    // hack to use macros inside this crate
    use replay_macros::{Event, Urn};
    use serde_with::{DeserializeFromStr, SerializeDisplay};
    use urn::UrnBuilder;

    use super::*;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Event)]
    enum BankAccountEvent {
        Deposited { amount: f64 },
    }

    #[derive(Debug, Clone, Urn, SerializeDisplay, DeserializeFromStr)]
    struct BankAccountUrn(Urn);

    fn account() -> BankAccountUrn {
        BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap())
    }

    #[test]
    fn of_with_withers_matches_the_literal_it_replaces() {
        let created = Utc::now() - Duration::seconds(30);
        let metadata = Metadata::new(serde_json::json!({ "correlation": "c-1" }));

        let built = PersistedEvent::of(account(), BankAccountEvent::Deposited { amount: 123.0 })
            .with_version(7)
            .with_created(created)
            .with_metadata(metadata.clone())
            .with_aggregate_version(Some(2));

        let literal = PersistedEvent {
            id: built.id,
            data: BankAccountEvent::Deposited { amount: 123.0 },
            stream_id: account().into(),
            r#type: "Deposited".to_string(),
            version: 7,
            created,
            metadata,
            aggregate_version: Some(2),
        };

        assert_eq!(built.id, literal.id);
        assert_eq!(built.data, literal.data);
        assert_eq!(built.stream_id, literal.stream_id);
        assert_eq!(built.r#type, literal.r#type);
        assert_eq!(built.version, literal.version);
        assert_eq!(built.created, literal.created);
        assert_eq!(built.metadata, literal.metadata);
        assert_eq!(built.aggregate_version, literal.aggregate_version);
    }

    /// The stores write `Event::event_type()` — the variant, not the enum name — so a
    /// fixture that names the enum diverges from every value a read path produces.
    #[test]
    fn event_type_comes_from_the_payload() {
        let event = PersistedEvent::of(account(), BankAccountEvent::Deposited { amount: 1.0 });

        assert_eq!(event.r#type, "Deposited");
    }

    #[test]
    fn of_defaults_the_fields_a_fixture_rarely_states() {
        let before = Utc::now();
        let event = PersistedEvent::of(account(), BankAccountEvent::Deposited { amount: 1.0 });

        assert_eq!(event.version, 1);
        assert_eq!(event.metadata, Metadata::default());
        assert_eq!(event.aggregate_version, None);
        assert!(event.created >= before && event.created <= Utc::now());
        assert_eq!(event.id.get_version_num(), 4);
    }
}
