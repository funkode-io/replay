use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON document attached to every event of an append.
///
/// The document is shared behind an [`Arc`]: metadata is constant for a whole
/// append yet carried by every event in it, so cloning must be a cheap handle
/// copy rather than a deep copy of the tree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Metadata {
    value: Arc<Value>,
}

impl Metadata {
    pub fn new<S: Serialize>(value: S) -> Self {
        Metadata {
            value: Arc::new(serde_json::to_value(value).unwrap()),
        }
    }

    /// Build metadata from a JSON document the caller already owns.
    ///
    /// Unlike [`Metadata::new`], which serializes its input, this takes the
    /// [`Value`] by move: no re-serialization, no deep copy. Use it wherever the
    /// document is already a `Value` — notably when mapping a stored row.
    pub fn from_json(value: Value) -> Self {
        Metadata {
            value: Arc::new(value),
        }
    }

    pub fn to_json(&self) -> Value {
        (*self.value).clone()
    }

    /// Check if one metadata matches another.
    ///
    /// If passed metadata has different type of current metadata, returns false
    ///
    /// If metadata is not an object we do equals comparison
    ///
    /// If metadata is an object we compare only common fields
    pub fn matches(&self, other: &Metadata) -> bool {
        // Compared by reference: matching a filter against every event of a feed
        // must not deep-copy either document.
        match (&*self.value, &*other.value) {
            (Value::Object(self_map), Value::Object(other_map)) => {
                for (key, value) in other_map {
                    if let Some(self_value) = self_map.get(key) {
                        if self_value != value {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            (self_json, other_json) => self_json == other_json,
        }
    }
}

impl From<Metadata> for Value {
    fn from(metadata: Metadata) -> Self {
        Arc::try_unwrap(metadata.value).unwrap_or_else(|shared| (*shared).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn metadata_from_owned_json_matches_metadata_from_serialize() {
        let value = json!({ "tenant": "acme", "user": "alice" });

        assert_eq!(Metadata::from_json(value.clone()), Metadata::new(&value));
    }

    #[test]
    fn metadata_from_owned_json_round_trips_the_document() {
        let value = json!({ "tenant": "acme", "nested": { "n": 1 } });

        assert_eq!(Metadata::from_json(value.clone()).to_json(), value);
    }

    #[test]
    fn metadata_serializes_as_a_wrapped_document() {
        let metadata = Metadata::from_json(json!({ "tenant": "acme" }));
        let serialized = serde_json::to_value(&metadata).unwrap();

        assert_eq!(serialized, json!({ "value": { "tenant": "acme" } }));
        assert_eq!(
            serde_json::from_value::<Metadata>(serialized).unwrap(),
            metadata
        );
    }
}
