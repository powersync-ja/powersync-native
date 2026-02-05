use powersync::StreamPriority;
use serde::{Serialize, ser::SerializeMap};
use serde_with::{DisplayFromStr, serde_as};

pub enum SyncLine<'a> {
    Checkpoint(Checkpoint<'a>),
    Data(DataLine<'a>),
    Custom(serde_json::Value),
}

impl<'a> Serialize for SyncLine<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SyncLine::Checkpoint(cp) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("checkpoint", cp)?;
                map.end()
            }
            SyncLine::Data(data) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("data", data)?;
                map.end()
            }
            SyncLine::Custom(value) => value.serialize(serializer),
        }
    }
}

#[serde_as]
#[derive(Serialize)]
pub struct Checkpoint<'a> {
    #[serde_as(as = "DisplayFromStr")]
    pub last_op_id: i64,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub write_checkpoint: Option<i64>,
    #[serde(borrow)]
    pub buckets: Vec<BucketChecksum<'a>>,
    #[serde(default, borrow)]
    pub streams: Vec<StreamDescription<'a>>,
}

impl<'a> Checkpoint<'a> {
    pub fn single_bucket(name: &'a str, count: i64, prio: Option<StreamPriority>) -> Self {
        let (stream, bucket) = BucketChecksum::with_stream(name, count, prio);

        Self {
            last_op_id: count,
            write_checkpoint: None,
            buckets: vec![bucket],
            streams: vec![stream],
        }
    }
}

#[derive(Serialize)]
pub struct StreamDescription<'a> {
    pub name: &'a str,
    pub is_default: bool,
    pub errors: Vec<()>,
}

#[serde_as]
#[derive(Serialize)]
pub struct BucketChecksum<'a> {
    #[serde(borrow)]
    pub bucket: &'a str,
    pub checksum: u32,
    #[serde(default)]
    pub priority: Option<StreamPriority>,
    #[serde(default)]
    pub count: Option<i64>,
    #[serde(default)]
    pub subscriptions: Vec<BucketSubscriptionReason>,
    //    #[serde(default)]
    //    #[serde(deserialize_with = "deserialize_optional_string_to_i64")]
    //    pub last_op_id: Option<i64>,
}

impl<'a> BucketChecksum<'a> {
    pub fn with_stream(
        name: &'a str,
        count: i64,
        prio: Option<StreamPriority>,
    ) -> (StreamDescription<'a>, Self) {
        let stream = StreamDescription {
            name,
            is_default: true,
            errors: Default::default(),
        };
        let cs = BucketChecksum {
            bucket: name,
            checksum: 0,
            priority: prio,
            count: Some(count),
            subscriptions: vec![BucketSubscriptionReason::DerivedFromDefaultStream(0)],
        };

        (stream, cs)
    }
}

#[derive(Serialize)]
pub enum BucketSubscriptionReason {
    #[serde(rename = "default")]
    DerivedFromDefaultStream(usize),
    #[serde(rename = "sub")]
    DerivedFromExplicitSubscription(usize),
}

#[derive(Serialize)]
pub struct DataLine<'a> {
    pub bucket: &'a str,
    pub data: Vec<OplogEntry<'a>>,
}

#[serde_as]
#[derive(Serialize)]
pub struct OplogEntry<'a> {
    pub checksum: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub op_id: i64,
    pub op: OpType,
    pub object_id: Option<String>,
    pub object_type: Option<&'a str>,
    pub subkey: Option<&'a str>,
    pub data: Option<&'a str>,
}

#[derive(Serialize)]
pub enum OpType {
    CLEAR,
    MOVE,
    PUT,
    REMOVE,
}
