mod bson_split;
mod shared_future;

pub use bson_split::BsonObjects;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::value::to_raw_value;
use serde_json::{Map, Value, value::RawValue};
pub use shared_future::SharedFuture;

/// A variant of [RawValue] that is guaranteed to be a JSON object.
#[derive(PartialEq, Eq, Debug, Hash)]
#[repr(transparent)]
pub struct SerializedJsonObject {
    json: str,
}

impl SerializedJsonObject {
    /// Safety: This must only be called for raw values that are known to be objects.
    unsafe fn from_owned_value(raw: Box<RawValue>) -> Box<Self> {
        unsafe {
            // Safety: Identical representation.
            std::mem::transmute(raw)
        }
    }

    /// Safety: This must only be called for raw values that are known to be objects.
    unsafe fn from_raw_value(raw: &RawValue) -> &Self {
        unsafe {
            // Safety: Identical representation.
            std::mem::transmute(raw)
        }
    }

    /// Serializes the given json object and returns its string representation.
    pub fn from_value(value: &Map<String, Value>) -> Box<Self> {
        let raw = to_raw_value(value).unwrap();

        unsafe {
            // Safety: We've just serialized an object.
            Self::from_owned_value(raw)
        }
    }

    pub fn get(&self) -> &str {
        &self.json
    }

    pub fn get_value(&self) -> Map<String, Value> {
        // We can unwrap this because we already know this is a complete JSON object.
        serde_json::from_str(&self.json).unwrap()
    }
}

impl ToOwned for SerializedJsonObject {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let raw: &RawValue = self.as_ref();
        unsafe {
            // Safety: Value is derived from this object, so this is an object too.
            Self::from_owned_value(raw.to_owned())
        }
    }
}

impl Clone for Box<SerializedJsonObject> {
    fn clone(&self) -> Self {
        let serialized: &SerializedJsonObject = self.as_ref();
        let raw: &RawValue = serialized.as_ref();
        unsafe {
            // Safety: Value is derived from this object, so this is an object too.
            SerializedJsonObject::from_owned_value(raw.to_owned())
        }
    }
}

impl<'de> Deserialize<'de> for &'de SerializedJsonObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw: &'de RawValue = Deserialize::deserialize(deserializer)?;
        if raw.get().starts_with('{') {
            Ok(unsafe {
                // Safety: We know it's a valid JSON value without padding. Since it starts with {,
                // it must be a valid JSON object.
                SerializedJsonObject::from_raw_value(raw)
            })
        } else {
            Err(Error::custom("Expected a JSON object"))
        }
    }
}

impl<'de> Deserialize<'de> for Box<SerializedJsonObject> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        if raw.get().starts_with('{') {
            Ok(unsafe {
                // Safety: We know it's a valid JSON value without padding. Since it starts with {,
                // it must be a valid JSON object.
                SerializedJsonObject::from_owned_value(raw)
            })
        } else {
            Err(Error::custom("Expected a JSON object"))
        }
    }
}

impl Serialize for SerializedJsonObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let raw: &RawValue = self.as_ref();
        raw.serialize(serializer)
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
            // Safety: The representations are identical, and this always corresponds to a
            // well-formed JSON object without padding.
            std::mem::transmute(self)
        }
    }
}
