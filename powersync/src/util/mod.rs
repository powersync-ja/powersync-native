mod bson_split;
mod shared_future;

pub use bson_split::BsonObjects;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, value::RawValue};
pub use shared_future::SharedFuture;

#[derive(PartialEq, Debug)]
#[repr(transparent)]
pub struct SerializedJsonObject {
    json: str,
}

impl SerializedJsonObject {
    fn from_owned_value(raw: Box<RawValue>) -> Box<Self> {
        unsafe {
            // Safety: Identical representation.
            std::mem::transmute(raw)
        }
    }

    pub fn from_value(value: &Map<String, Value>) -> Box<Self> {
        let serialized = serde_json::to_string(value).unwrap();
        let raw = serde_json::from_str::<Box<RawValue>>(&serialized).unwrap();
        Self::from_owned_value(raw)
    }
}

impl<'de> Deserialize<'de> for Box<SerializedJsonObject> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(SerializedJsonObject::from_owned_value(
            Box::<RawValue>::deserialize(deserializer)?,
        ))
    }
}

impl Serialize for SerializedJsonObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.json.serialize(serializer)
    }
}

impl AsRef<str> for SerializedJsonObject {
    fn as_ref(&self) -> &str {
        &self.json
    }
}

impl AsRef<RawValue> for SerializedJsonObject {
    fn as_ref(&self) -> &RawValue {
        unsafe {
            // Safety: The representations are identical, and this always corresponds to a well-
            // formed JSON object without padding.
            std::mem::transmute(self)
        }
    }
}
