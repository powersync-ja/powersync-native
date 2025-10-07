use crate::{PowerSyncDatabase, sync::streams::StreamDescription, util::SerializedJsonObject};

pub struct SyncStream<'a> {
    db: &'a PowerSyncDatabase,
    name: &'a str,
    parameters: Option<Box<SerializedJsonObject>>,
}

impl<'a> SyncStream<'a> {
    pub fn new(
        db: &'a PowerSyncDatabase,
        name: &'a str,
        parameters: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        Self {
            db,
            name,
            parameters: parameters.map(SerializedJsonObject::from_value),
        }
    }
}

impl<'a> Into<StreamDescription<'a>> for &'a SyncStream<'a> {
    fn into(self) -> StreamDescription<'a> {
        StreamDescription {
            name: self.name,
            parameters: match self.parameters.as_ref() {
                Some(params) => Some(&**params),
                None => None,
            },
        }
    }
}
