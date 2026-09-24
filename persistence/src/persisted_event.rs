use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Utc};
use urn::Urn;
use uuid::Uuid;

use replay::{Event, Metadata, ObservedEvent};

/// An event as it was stored: what a rule observes ([`ObservedEvent`]) plus the identity
/// and position the store gave it.
///
/// The observed half is reached by [`Deref`], so `event.data`, `event.stream_id`,
/// `event.metadata` and `event.created` read through unchanged; only struct-literal
/// construction has to name the embedded field (use [`of`](Self::of) instead).
#[derive(Debug, Clone)]
pub struct PersistedEvent<E> {
    pub id: Uuid,
    pub r#type: String,
    /// Monotonic position of this event within the aggregate's current stream.
    pub version: i64,
    /// `None` identifies events belonging to the current (latest) stream.
    /// `Some(n)` identifies events that were archived during the nth compaction.
    /// Matches the `INTEGER` column type in the database.
    pub aggregate_version: Option<i32>,
    /// The half a [`Policy`](replay::Policy) sees. Read it through the deref, not
    /// through this name.
    pub observed: ObservedEvent<E>,
}

impl<E> Deref for PersistedEvent<E> {
    type Target = ObservedEvent<E>;

    fn deref(&self) -> &Self::Target {
        &self.observed
    }
}

impl<E> DerefMut for PersistedEvent<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.observed
    }
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
            version: 1,
            aggregate_version: None,
            observed: ObservedEvent::of(stream_id, data).with_created(Utc::now()),
        }
    }
}

impl<E> PersistedEvent<E> {
    pub fn with_version(self, version: i64) -> Self {
        PersistedEvent { version, ..self }
    }

    pub fn with_created(self, created: DateTime<Utc>) -> Self {
        PersistedEvent {
            observed: self.observed.with_created(created),
            ..self
        }
    }

    pub fn with_metadata(self, metadata: impl Into<Metadata>) -> Self {
        PersistedEvent {
            observed: self.observed.with_metadata(metadata),
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

    /// The payload, taken out of the envelope. The counterpart to reading `event.data`
    /// through the deref, which cannot move.
    pub fn into_data(self) -> E {
        self.observed.data
    }

    pub fn wrap_data_with<Other: From<E>>(self) -> PersistedEvent<Other> {
        let ObservedEvent {
            data,
            stream_id,
            metadata,
            created,
        } = self.observed;
        PersistedEvent {
            id: self.id,
            r#type: self.r#type,
            version: self.version,
            aggregate_version: self.aggregate_version,
            observed: ObservedEvent {
                data: Other::from(data),
                stream_id,
                metadata,
                created,
            },
        }
    }

    pub fn with_data<Other: Event>(self, data: Other) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            r#type: self.r#type,
            version: self.version,
            aggregate_version: self.aggregate_version,
            observed: ObservedEvent {
                data,
                stream_id: self.observed.stream_id,
                metadata: self.observed.metadata,
                created: self.observed.created,
            },
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
            r#type: self.r#type.clone(),
            version: self.version,
            aggregate_version: self.aggregate_version,
            observed: self.observed_with_data(data),
        }
    }

    /// The observed half of a *borrowed* event, re-enveloped with new data.
    ///
    /// What the policy erasure hands to `react`: two clones (the URN and the metadata
    /// handle), and nothing of the stored payload, which stays behind in `self`.
    pub(crate) fn observed_with_data<Other>(&self, data: Other) -> ObservedEvent<Other> {
        ObservedEvent {
            data,
            stream_id: self.observed.stream_id.clone(),
            metadata: self.observed.metadata.clone(),
            created: self.observed.created,
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
            r#type: "Deposited".to_string(),
            version: 7,
            aggregate_version: Some(2),
            observed: ObservedEvent {
                data: BankAccountEvent::Deposited { amount: 123.0 },
                stream_id: account().into(),
                metadata,
                created,
            },
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

    /// The observed half is what a rule is handed, and it is the same values the
    /// persisted envelope reads through.
    #[test]
    fn the_observed_half_is_the_envelope_a_rule_sees() {
        let event = PersistedEvent::of(account(), BankAccountEvent::Deposited { amount: 1.0 });

        let observed: &ObservedEvent<BankAccountEvent> = &event;

        assert_eq!(observed.data, event.data);
        assert_eq!(observed.stream_id, event.stream_id);
        assert_eq!(observed.created, event.created);
        assert_eq!(observed.metadata, event.metadata);
    }
}
