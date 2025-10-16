use std::time::{Duration, SystemTime};

use serde::{Deserialize, de::IgnoredAny};
use serde_json::value::RawValue;

use crate::{sync::progress::ProgressCounters, util::SerializedJsonObject};

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
        #[serde(rename = "did_expire")]
        _did_expire: bool,
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
    #[serde(rename = "DEBUG")]
    Debug,
    #[serde(rename = "INFO")]
    Info,
    #[serde(rename = "WARNING")]
    Warning,
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
    pub streams: Vec<ActiveStreamSubscription>,
    pub downloading: Option<IgnoredAny>,
}

#[derive(Deserialize, Debug)]
pub struct ActiveStreamSubscription {
    pub name: String,
    pub parameters: Option<Box<SerializedJsonObject>>,
    //pub priority: Option<StreamPriority>,
    pub active: bool,
    pub is_default: bool,
    pub has_explicit_subscription: bool,
    pub expires_at: Option<Timestamp>,
    pub last_synced_at: Option<Timestamp>,
    pub progress: ProgressCounters,
}

#[repr(transparent)]
#[derive(Deserialize, Clone, Copy, Debug)]
pub struct Timestamp(pub i64);

impl From<Timestamp> for SystemTime {
    fn from(val: Timestamp) -> Self {
        let since_epoch = Duration::from_secs(val.0 as u64);
        SystemTime::UNIX_EPOCH + since_epoch
    }
}
