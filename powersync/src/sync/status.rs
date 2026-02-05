use std::{
    fmt::Debug,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use event_listener::{Event, EventListener};
use rusqlite::{Connection, params};

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

#[derive(Debug)]
pub enum UploadStatus {
    Idle,
    Uploading,
    Error(PowerSyncError),
}

impl Default for UploadStatus {
    fn default() -> Self {
        Self::Idle
    }
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
        conn: &Connection,
    ) -> Result<(), PowerSyncError> {
        let mut stmt = conn.prepare_cached("SELECT powersync_offline_sync_status()")?;
        let raw_status: String = stmt.query_row(params![], |row| row.get(0))?;

        self.update_from_core(serde_json::from_str(&raw_status)?);
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
