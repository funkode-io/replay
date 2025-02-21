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

    pub fn to_json(&self) -> Value {
        self.value.clone()
    }
}

impl From<Metadata> for Value {
    fn from(metadata: Metadata) -> Self {
        metadata.to_json()
    }
}
