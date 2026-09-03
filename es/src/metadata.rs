use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Metadata {
    value: Value,
}

impl Metadata {
    pub fn new<S: Serialize>(value: S) -> Self {
        Metadata {
            value: serde_json::to_value(value).unwrap(),
        }
    }

    /// Build metadata from a JSON document the caller already owns.
    ///
    /// Unlike [`Metadata::new`], which serializes its input, this takes the
    /// [`Value`] by move: no re-serialization, no deep copy. Use it wherever the
    /// document is already a `Value` — notably when mapping a stored row.
    pub fn from_json(value: Value) -> Self {
        Metadata { value }
    }

    pub fn to_json(&self) -> Value {
        self.value.clone()
    }

    /// Check if one metadata matches another.
    ///
    /// If passed metadata has different type of current metadata, returns false
    ///
    /// If metadata is not an object we do equals comparison
    ///
    /// If metadata is an object we compare only common fields
    pub fn matches(&self, other: &Metadata) -> bool {
        let self_json = self.to_json();
        let other_json = other.to_json();

        match (&self_json, &other_json) {
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
            _ => self_json == other_json,
        }
    }
}

impl From<Metadata> for Value {
    fn from(metadata: Metadata) -> Self {
        metadata.to_json()
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
}
