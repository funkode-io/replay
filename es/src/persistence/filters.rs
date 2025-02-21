use std::ops::Not;

use chrono::Utc;
use serde::Serialize;
use urn::Urn;

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
