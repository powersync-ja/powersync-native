use std::time::{Duration, SystemTime};

use serde::{
    Deserialize,
    de::{self, DeserializeSeed, IgnoredAny, Visitor},
};
use serde_json::value::RawValue;

use crate::{
    sync::{progress::ProgressCounters, stream_priority::StreamPriority},
    util::SerializedJsonObject,
};

/// An instruction sent by the core extension to the SDK.
#[derive(Deserialize, Debug)]
pub enum Instruction {
    LogLine {
        severity: LogSeverity,
        line: String,
    },
    /// Update the download status for the ongoing sync iteration.
    UpdateSyncStatus {
        status: DownloadSyncStatus,
    },
    /// Connect to the sync service using the [StreamingSyncRequest] created by the core extension,
    /// and then forward received lines via [SyncEvent::TextLine] and [SyncEvent::BinaryLine].
    EstablishSyncStream {
        request: Box<RawValue>,
    },
    FetchCredentials {
        /// Whether the credentials currently used have expired.
        ///
        /// If false, this is a pre-fetch.
        did_expire: bool,
    },
    // These are defined like this because deserializers in Kotlin can't support either an
    // object or a literal value
    /// Close the websocket / HTTP stream to the sync service.
    CloseSyncStream(CloseSyncStream),
    /// Flush the file-system if it's non-durable (only applicable to the Dart SDK).
    FlushFileSystem {},
    /// Notify that a sync has been completed, prompting client SDKs to clear earlier errors.
    DidCompleteSync {},
}

#[derive(Deserialize, Debug)]
pub struct CloseSyncStream {
    /// Whether clients should hide the brief disconnected status from the public sync status and
    /// reconnect immediately.
    pub hide_disconnect: bool,
}

#[derive(Deserialize, Debug)]
pub enum LogSeverity {
    DEBUG,
    INFO,
    WARNING,
}

/// Information about a progressing download.
#[derive(Deserialize, Default, Debug)]
pub struct DownloadSyncStatus {
    /// Whether the socket to the sync service is currently open and connected.
    ///
    /// This starts being true once we receive the first line, and is set to false as the iteration
    /// ends.
    pub connected: bool,
    /// Whether we've requested the client SDK to connect to the socket while not receiving sync
    /// lines yet.
    pub connecting: bool,
    /// Provides stats over which bucket priorities have already been synced (or when they've last
    /// been changed).
    ///
    /// Always sorted by descending [BucketPriority] in [SyncPriorityStatus] (or, in other words,
    /// increasing priority numbers).
    // TODO: Hide in favor of stream status
    pub priority_status: Vec<SyncPriorityStatus>,
    /// When a download is active (that is, a `checkpoint` or `checkpoint_diff` line has been
    /// received), information about how far the download has progressed.
    pub downloading: Option<SyncDownloadProgress>,
    pub streams: Vec<ActiveStreamSubscription>,
}

#[derive(Deserialize, Debug)]
pub struct SyncPriorityStatus {
    pub priority: StreamPriority,
    pub last_synced_at: Option<Timestamp>,
    pub has_synced: Option<bool>,
}

/// Per-bucket download progress information.
#[derive(Deserialize, Debug)]
pub struct BucketProgress {
    pub priority: StreamPriority,
    pub at_last: i64,
    pub since_last: i64,
    pub target_count: i64,
}

#[derive(Deserialize, Debug)]
pub struct ActiveStreamSubscription {
    pub name: String,
    pub parameters: Option<Box<SerializedJsonObject>>,
    pub priority: Option<StreamPriority>,
    pub active: bool,
    pub is_default: bool,
    pub has_explicit_subscription: bool,
    pub expires_at: Option<Timestamp>,
    pub last_synced_at: Option<Timestamp>,
    pub progress: ProgressCounters,
}

#[derive(Debug)]
pub struct SyncDownloadProgress {
    by_priority: Vec<BucketProgress>,
}

impl<'de> Deserialize<'de> for SyncDownloadProgress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BucketProgressVisitor;

        impl<'de> Visitor<'de> for BucketProgressVisitor {
            type Value = Vec<BucketProgress>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "map of <dontcare, BucketProgress>")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = vec![];
                while let Some((_, v)) = map.next_entry::<IgnoredAny, BucketProgress>()? {
                    values.push(v);
                }

                Ok(values)
            }
        }

        impl<'de> DeserializeSeed<'de> for BucketProgressVisitor {
            type Value = Vec<BucketProgress>;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                deserializer.deserialize_map(self)
            }
        }

        struct SyncDownloadProgressVisitor;

        impl<'de> Visitor<'de> for SyncDownloadProgressVisitor {
            type Value = SyncDownloadProgress;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    formatter,
                    "SyncDownloadProgress (buckets: map of <dontcare, BucketProgress>"
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut buckets = None::<Vec<BucketProgress>>;

                while let Some(key) = map.next_key()? {
                    match key {
                        "buckets" => buckets = Some(map.next_value_seed(BucketProgressVisitor)?),
                        _ => return Err(de::Error::unknown_field(key, &["buckets"])),
                    }
                }

                let buckets = buckets.ok_or_else(|| de::Error::missing_field("buckets"))?;

                Ok(SyncDownloadProgress {
                    by_priority: buckets,
                })
            }
        }

        deserializer.deserialize_struct(
            "SyncDownloadProgress",
            &["buckets"],
            SyncDownloadProgressVisitor,
        )
    }
}

#[repr(transparent)]
#[derive(Deserialize, Clone, Copy, Debug)]
pub struct Timestamp(pub i64);

impl Into<SystemTime> for Timestamp {
    fn into(self) -> SystemTime {
        let since_epoch = Duration::from_secs(self.0 as u64);
        SystemTime::UNIX_EPOCH + since_epoch
    }
}
