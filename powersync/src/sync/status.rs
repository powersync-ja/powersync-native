use std::{
    fmt::Debug,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use event_listener::{Event, EventListener};
use powersync_sqlite_nostd::ResultCode;
use serde::Deserialize;

use crate::{StreamPriority, db::connection::SqliteConnection, sync::instruction::Timestamp};
use crate::{
    error::PowerSyncError,
    sync::{
        instruction::{ActiveStreamSubscription, DownloadSyncStatus},
        progress::ProgressCounters,
        streams::{StreamDescription, StreamSubscriptionDescription},
    },
};

/// An internal struct holding the current sync status, which allows notifying listeners.
pub struct SyncStatus {
    data: Mutex<Arc<SyncStatusData>>,
}

impl SyncStatus {
    pub(crate) fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn current_snapshot(&self) -> Arc<SyncStatusData> {
        let data = self.data.lock().unwrap();
        Arc::clone(&*data)
    }

    pub(crate) fn update<T>(&self, update: impl FnOnce(&mut SyncStatusData) -> T) -> T {
        // Update status.
        let mut data = self.data.lock().unwrap();
        let mut new = data.new_revision();
        let res = update(&mut new);

        // Then notify listeners.
        let old_state = std::mem::replace(&mut *data, Arc::new(new));
        old_state.is_invalidated.store(true, Ordering::SeqCst);
        old_state.invalidated.notify(usize::MAX);

        res
    }
}

#[derive(Debug, Default)]
pub enum UploadStatus {
    #[default]
    Idle,
    Uploading,
    Error(PowerSyncError),
}

#[derive(Default)]
pub struct SyncStatusData {
    downloading: Arc<DownloadSyncStatus>,
    download_error: Option<PowerSyncError>,
    uploads: UploadStatus,

    /// Raised when a new instance is installed in [SyncStatus].
    is_invalidated: AtomicBool,
    /// Notified when a new instance is installed in [SyncStatus].
    invalidated: Event,
}

impl SyncStatusData {
    fn new_revision(&self) -> Self {
        Self {
            downloading: self.downloading.clone(),
            download_error: self.download_error.clone(),
            uploads: Default::default(),
            is_invalidated: Default::default(),
            invalidated: Default::default(),
        }
    }

    pub fn is_connecting(&self) -> bool {
        self.downloading.connecting
    }

    pub fn is_connected(&self) -> bool {
        self.downloading.connected
    }

    pub fn is_downloading(&self) -> bool {
        self.downloading.downloading.is_some()
    }

    pub fn download_error(&self) -> Option<&PowerSyncError> {
        self.download_error.as_ref()
    }

    pub fn any_error(&self) -> Option<&PowerSyncError> {
        match self.download_error() {
            Some(e) => Some(e),
            None => self.upload_error(),
        }
    }

    pub fn is_uploading(&self) -> bool {
        matches!(self.uploads, UploadStatus::Uploading)
    }

    pub fn upload_error(&self) -> Option<&PowerSyncError> {
        match self.uploads {
            UploadStatus::Error(ref e) => Some(e),
            _ => None,
        }
    }

    /// Status information for a stream, if it's a stream that is currently tracked by the sync
    /// client.
    pub fn for_stream<'a, 'b>(
        &'a self,
        stream: impl Into<StreamDescription<'b>>,
    ) -> Option<SyncStreamStatus<'a>> {
        let key: StreamDescription<'b> = stream.into();

        for stream in &self.downloading.streams {
            if stream.name == key.name && stream.parameters.as_deref() == key.parameters {
                return Some(self.publish_stream_subscription(stream));
            }
        }

        None
    }

    /// This is only meant to be used for internal use-cases where this SDK is used to implement
    /// another PowerSync SDK.
    #[doc(hidden)]
    pub fn internal_streams(&self) -> &[ActiveStreamSubscription] {
        &self.downloading.streams
    }

    /// Information about when streams in priorities have last synced.
    pub fn priority_status(&self) -> &[PriorityStatus] {
        &self.downloading.priority_status
    }

    pub fn priority_status_for(&self, priority: StreamPriority) -> Option<&PriorityStatus> {
        // Lower-priority values always override higher-priority status entries. The internal
        // priority is already sorted in ascending priority order.
        for entry in self.priority_status() {
            if entry.priority <= priority {
                return Some(entry);
            }
        }

        None
    }

    /// Whether all tracked Sync Streams have ever synced.
    pub fn has_synced(&self) -> bool {
        self.priority_status_for(StreamPriority::SENTINEL)
            .is_some_and(|status| status.has_synced())
    }

    /// The last timestamp at which all tracked Sync Streams have synced.
    pub fn last_synced_at(&self) -> Option<SystemTime> {
        self.priority_status_for(StreamPriority::SENTINEL)?
            .last_synced_at()
    }

    /// All sync streams currently being tracked in the database.
    pub fn streams<'a>(&'a self) -> impl Iterator<Item = SyncStreamStatus<'a>> + 'a {
        self.downloading
            .streams
            .iter()
            .map(|stream| self.publish_stream_subscription(stream))
    }

    fn publish_stream_subscription<'a>(
        &'a self,
        stream: &'a ActiveStreamSubscription,
    ) -> SyncStreamStatus<'a> {
        SyncStreamStatus {
            progress: self
                .downloading
                .downloading
                .map(|_| stream.progress.clone()),
            subscription: StreamSubscriptionDescription { core: stream },
        }
    }

    pub(crate) fn is_checkpoint_request_applied(&self, id: i64) -> bool {
        let Some(last_applied) = self.downloading.last_applied_checkpoint_request else {
            return false;
        };
        return last_applied >= id;
    }

    /// Returns an [EventListener] that completes once this data is stale, or returns [None]
    /// immediately if this is already stale.
    pub(crate) fn listen_for_changes(&self) -> Option<EventListener> {
        if self.is_invalidated.load(Ordering::SeqCst) {
            return None;
        }

        let listener = self.invalidated.listen();
        if self.is_invalidated.load(Ordering::SeqCst) {
            return None;
        }

        Some(listener)
    }

    pub(crate) fn resolve_offline_state(
        &mut self,
        conn: &SqliteConnection,
    ) -> Result<(), PowerSyncError> {
        let stmt = conn.prepare("SELECT powersync_offline_sync_status()")?;
        let ResultCode::ROW = stmt.step()? else {
            panic!("Expected row"); // Can't happen, scalar select
        };

        let raw_status = stmt.column_text(0)?;
        self.update_from_core(serde_json::from_str(raw_status)?);
        Ok(())
    }

    pub(crate) fn update_from_core(&mut self, core: DownloadSyncStatus) {
        self.downloading = Arc::new(core);
    }

    pub(crate) fn set_download_error(&mut self, e: PowerSyncError) {
        // TODO: Reset to offline sync state?
        self.download_error = Some(e);
    }

    pub(crate) fn set_upload_state(&mut self, state: UploadStatus) {
        self.uploads = state;
    }

    pub(crate) fn clear_download_errors(&mut self) {
        self.download_error = None;
    }
}

impl Debug for SyncStatusData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncStatusData")
            .field("downloading", &self.downloading)
            .field("download_error", &self.download_error)
            .field("uploads", &self.uploads)
            .finish()
    }
}

pub struct SyncStreamStatus<'a> {
    pub progress: Option<ProgressCounters>,
    pub subscription: StreamSubscriptionDescription<'a>,
}

/// Information about partial sync.
///
/// When different Sync Streams have different priorities assigned to them, clients may be in a
/// state where only some of them have synced. This information is exposed on
/// [ActiveStreamSubscription] for affected streams, [DownloadSyncStatus::priority_status] reports
/// it aggregated by priority.
#[derive(Deserialize, Debug)]
pub struct PriorityStatus {
    /// The priority for which information is reported.
    pub priority: StreamPriority,
    /// The last timestamp at which streams with this priority have synced.
    last_synced_at: Option<Timestamp>,
}

impl PriorityStatus {
    pub fn last_synced_at(&self) -> Option<SystemTime> {
        Some(self.last_synced_at?.into())
    }

    pub fn has_synced(&self) -> bool {
        self.last_synced_at.is_some()
    }
}
