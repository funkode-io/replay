use std::ops::Not;

use chrono::Utc;
use serde::Serialize;
use urn::Urn;

use crate::persistence::PersistedEvent;

#[derive(Debug, PartialEq, Default, Clone)]
pub enum StreamFilter {
    #[default]
    All,
    WithStreamId(Urn),
    ForStreamTypes(Vec<String>),
    WithMetadata(crate::Metadata),
    AfterVersion(i64),
    CreatedAfter(chrono::DateTime<Utc>),
    And(Box<StreamFilter>, Box<StreamFilter>),
    Or(Box<StreamFilter>, Box<StreamFilter>),
    Not(Box<StreamFilter>),
}

impl StreamFilter {
    pub fn passes<S: crate::Stream>(&self, event: &PersistedEvent<S::Event>) -> bool {
        match self {
            StreamFilter::All => true,
            StreamFilter::WithStreamId(stream_id) => event.stream_id == *stream_id,
            StreamFilter::ForStreamTypes(stream_types) => stream_types.contains(&S::stream_type()),
            StreamFilter::WithMetadata(metadata) => event.metadata == *metadata,
            StreamFilter::AfterVersion(version) => event.version > *version,
            StreamFilter::CreatedAfter(timestamp) => event.created > *timestamp,
            StreamFilter::And(left, right) => left.passes::<S>(event) && right.passes::<S>(event),
            StreamFilter::Or(left, right) => left.passes::<S>(event) || right.passes::<S>(event),
            StreamFilter::Not(filter) => !filter.passes::<S>(event),
        }
    }

    pub fn all() -> StreamFilter {
        StreamFilter::All
    }

    pub fn with_stream_id<S: crate::Stream>(stream_id: &S::StreamId) -> StreamFilter {
        StreamFilter::WithStreamId(stream_id.clone().into())
    }

    pub fn for_stream_type<S: crate::Stream>() -> StreamFilter {
        StreamFilter::ForStreamTypes(vec![S::stream_type()])
    }

    pub fn with_metadata(metadata: impl Serialize) -> StreamFilter {
        StreamFilter::WithMetadata(crate::Metadata::new(metadata))
    }

    pub fn after_version(version: i64) -> StreamFilter {
        StreamFilter::AfterVersion(version)
    }

    pub fn created_after(timestamp: chrono::DateTime<Utc>) -> StreamFilter {
        StreamFilter::CreatedAfter(timestamp)
    }

    // implement methods from an existing filter
    pub fn and(self, other: StreamFilter) -> StreamFilter {
        StreamFilter::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: StreamFilter) -> StreamFilter {
        StreamFilter::Or(Box::new(self), Box::new(other))
    }

    pub fn and_with_metadata(self, metadata: impl Serialize) -> StreamFilter {
        self.and(StreamFilter::with_metadata(metadata))
    }

    pub fn and_at_stream_version(self, version: i64) -> StreamFilter {
        self.and(StreamFilter::AfterVersion(version))
    }

    pub fn and_at_stream_version_optional(self, version: Option<i64>) -> StreamFilter {
        match version {
            Some(version) => self.and(StreamFilter::AfterVersion(version)),
            None => self,
        }
    }

    pub fn and_at_timestamp(self, timestamp: chrono::DateTime<Utc>) -> StreamFilter {
        self.and(StreamFilter::CreatedAfter(timestamp))
    }

    pub fn and_at_timestamp_optional(
        self,
        timestamp: Option<chrono::DateTime<Utc>>,
    ) -> StreamFilter {
        match timestamp {
            Some(timestamp) => self.and(StreamFilter::CreatedAfter(timestamp)),
            None => self,
        }
    }
}

impl Not for StreamFilter {
    type Output = StreamFilter;

    fn not(self) -> StreamFilter {
        StreamFilter::Not(Box::new(self))
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Not;

    // hack to use macros inside this crate
    use crate as replay;
    use replay_macros::{Event, Urn};
    use serde_with::{DeserializeFromStr, SerializeDisplay};
    use urn::{Urn, UrnBuilder};

    use crate::Metadata;

    // create bank account stream
    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct BankAccountStream {
        pub balance: f64,
    }

    impl crate::Stream for BankAccountStream {
        type Event = BankAccountEvent;
        type StreamId = BankAccountUrn;

        fn stream_type() -> String {
            "BankAccount".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                BankAccountEvent::Deposited { amount } => {
                    self.balance += amount;
                }
                BankAccountEvent::Withdrawn { amount } => {
                    self.balance -= amount;
                }
            }
        }
    }

    // create bank account events: deposited, withdrawn
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Event)]
    pub enum BankAccountEvent {
        Deposited { amount: f64 },
        Withdrawn { amount: f64 },
    }

    // bank account urn
    #[derive(Debug, Clone, PartialEq, Urn, SerializeDisplay, DeserializeFromStr)]
    pub struct BankAccountUrn(Urn);

    // create metadata with bank account urn
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct BankAccountMetadata {
        bank_account: BankAccountUrn,
    }

    impl From<BankAccountMetadata> for Metadata {
        fn from(metadata: BankAccountMetadata) -> Self {
            Metadata::new(metadata)
        }
    }

    // test an event pass filter `StreamFilter::All`
    #[test]
    fn test_all() {
        let filter = super::StreamFilter::all();
        let event = BankAccountEvent::Deposited { amount: 123f64 };
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::WithStreamId`
    #[test]
    fn test_with_stream_id() {
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
        let filter = super::StreamFilter::with_stream_id::<BankAccountStream>(&bank_account_urn);
        let event = BankAccountEvent::Deposited { amount: 123f64 };

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::ForStreamTypes`
    #[test]
    fn test_for_stream_type() {
        let filter = super::StreamFilter::for_stream_type::<BankAccountStream>();
        let event = BankAccountEvent::Deposited { amount: 123f64 };
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::WithMetadata`
    #[test]
    fn test_with_metadata() {
        let metadata = BankAccountMetadata {
            bank_account: BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap()),
        };
        let filter = super::StreamFilter::with_metadata(metadata.clone());
        let event = BankAccountEvent::Deposited { amount: 123f64 };

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: metadata.bank_account.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: metadata.into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::AfterVersion`
    #[test]
    fn test_after_version() {
        let filter = super::StreamFilter::after_version(1);
        let event = BankAccountEvent::Deposited { amount: 123f64 };
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 2,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::CreatedAfter`
    #[test]
    fn test_created_after() {
        let filter =
            super::StreamFilter::created_after(chrono::Utc::now() - chrono::Duration::seconds(1));
        let event = BankAccountEvent::Deposited { amount: 123f64 };
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::And`
    #[test]
    fn test_and() {
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
        let filter = super::StreamFilter::with_stream_id::<BankAccountStream>(&bank_account_urn)
            .and(super::StreamFilter::after_version(1));
        let event = BankAccountEvent::Deposited { amount: 123f64 };

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 2,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::Or`
    #[test]
    fn test_or() {
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
        let filter = super::StreamFilter::with_stream_id::<BankAccountStream>(&bank_account_urn)
            .or(super::StreamFilter::after_version(1));
        let event = BankAccountEvent::Deposited { amount: 123f64 };

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };
        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }

    // test an event pass filter `StreamFilter::Not`
    #[test]
    fn test_not() {
        let bank_account_urn =
            BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
        let filter = super::StreamFilter::with_stream_id::<BankAccountStream>(&bank_account_urn)
            .and(super::StreamFilter::after_version(1).not());
        let event = BankAccountEvent::Deposited { amount: 123f64 };

        let persisted_event = crate::persistence::PersistedEvent {
            id: uuid::Uuid::new_v4(),
            data: event,
            stream_id: bank_account_urn.clone().into(),
            r#type: "BankAccountEvent".to_string(),
            version: 1,
            created: chrono::Utc::now(),
            metadata: BankAccountMetadata {
                bank_account: bank_account_urn,
            }
            .into(),
        };

        assert!(filter.passes::<BankAccountStream>(&persisted_event));
    }
}
