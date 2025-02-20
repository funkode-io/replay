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
}
