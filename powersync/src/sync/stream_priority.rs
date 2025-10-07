use serde::{Deserialize, Serialize, de::Visitor};

use crate::error::PowerSyncError;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamPriority {
    number: i32,
}

impl StreamPriority {
    pub const HIGHEST: StreamPriority = StreamPriority { number: 0 };

    /// A low priority used to represent fully-completed sync operations across all priorities.
    pub const SENTINEL: StreamPriority = StreamPriority { number: i32::MAX };

    pub fn priority_number(self) -> i32 {
        self.into()
    }
}

impl TryFrom<i32> for StreamPriority {
    type Error = PowerSyncError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < StreamPriority::HIGHEST.number {
            return Err(PowerSyncError::argument_error(
                "Invalid bucket priority value",
            ));
        }

        return Ok(StreamPriority { number: value });
    }
}

impl Into<i32> for StreamPriority {
    fn into(self) -> i32 {
        self.number
    }
}

impl PartialOrd<StreamPriority> for StreamPriority {
    fn partial_cmp(&self, other: &StreamPriority) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamPriority {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.number.cmp(&other.number).reverse()
    }
}

impl<'de> Deserialize<'de> for StreamPriority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PriorityVisitor;
        impl<'de> Visitor<'de> for PriorityVisitor {
            type Value = StreamPriority;

            fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
                formatter.write_str("a priority as an integer between 0 and 3 (inclusive)")
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                StreamPriority::try_from(v).map_err(|e| E::custom(e))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let i: i32 = v.try_into().map_err(|_| E::custom("int too large"))?;
                Self::visit_i32(self, i)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let i: i32 = v.try_into().map_err(|_| E::custom("int too large"))?;
                Self::visit_i32(self, i)
            }
        }

        deserializer.deserialize_i32(PriorityVisitor)
    }
}

impl Serialize for StreamPriority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(self.number)
    }
}
