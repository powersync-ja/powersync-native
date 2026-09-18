use async_lock::Mutex as AsyncMutex;
use std::sync::{Arc, Mutex};

use super::client;
use async_channel::{Receiver, Sender};

use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    env::PowerSyncTask,
    error::PowerSyncError,
    sync::{download::DownloadEvent, streams::ChangedSyncSubscriptions},
};

/// Implements `connect()` and `disconnect()` by dispatching messages to the upload and download
/// actors.
///
/// Since actors only have access to the receiving end of their channels, dropping the coordinator
/// will also terminate all actors (albeit asynchronously).
#[derive(Default)]
pub struct SyncSignals {
    task: AsyncMutex<Option<(async_oneshot::Sender<()>, PowerSyncTask)>>,
    channels: Mutex<Option<SyncChannels>>,
}

impl SyncSignals {
    pub async fn connect(self: Arc<Self>, db: Arc<InnerPowerSyncState>, options: SyncOptions) {
        self.disconnect().await;

        let mut guard = self.task.lock().await;
        let (abort_controller, abort_signal) = async_oneshot::oneshot();

        let task = client::spawn(db, options, self.clone(), abort_signal);
        *guard = Some((abort_controller, task));
    }

    pub async fn disconnect(&self) {
        let mut guard = self.task.lock().await;

        if let Some((mut request_cancellation, task)) = guard.take() {
            // Gracefully shut down the sync task by requesting a cancellation.
            let _ = request_cancellation.send(());
            task.join().await;
        }
    }

    /// If we're offline, update the offline sync status and emit it into the database.
    ///
    /// This is used after adding a new subscription to include it in the sync status even if we're
    /// disconnected.
    /// This is a no-op while connected.
    pub async fn resolve_offline_sync_status(
        &self,
        db: &InnerPowerSyncState,
    ) -> Result<(), PowerSyncError> {
        let guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let writer = db.writer().await?;
        db.status
            .update(|s| s.resolve_offline_state(writer.sqlite_connection()))
    }

    /// Handle the set of active sync stream subscriptions changing.
    ///
    /// This is a no-op if not connected.
    pub async fn handle_subscriptions_changed(&self, update: ChangedSyncSubscriptions) {
        let channel = {
            let guard = self.channels.lock().unwrap();
            guard
                .as_ref()
                .map(|channels| channels.local_download_events.clone())
        };

        if let Some(channel) = channel {
            let _ = channel
                .send(DownloadEvent::UpdateSubscriptions { keys: update.0 })
                .await;
        }
    }

    pub fn install_channels(self: Arc<Self>, channels: SyncChannels) -> impl Drop + 'static {
        {
            let mut guard = self.channels.lock().unwrap();
            *guard = Some(channels);
        }

        scopeguard::guard(self, |signals| {
            let mut guard = signals.channels.lock().unwrap();
            *guard = None;
        })
    }
}

#[derive(Clone)]
pub struct SyncChannels {
    local_download_events: Sender<DownloadEvent>,
    trigger_upload: Sender<()>,
}

impl SyncChannels {
    pub fn create() -> (Self, Receiver<DownloadEvent>, Receiver<()>) {
        let (download_send, download_receive) = async_channel::unbounded();
        let (uploads_send, uploads_receive) = async_channel::bounded(1);

        (
            Self {
                local_download_events: download_send,
                trigger_upload: uploads_send,
            },
            download_receive,
            uploads_receive,
        )
    }

    pub fn trigger_crud_upload(&self) {
        // If an existing crud request is already buffered in the channel, we can replace it.
        let _ = self.trigger_upload.force_send(());
    }

    /// Marks CRUD uploads as complete, allowing the download client to retry if a previous
    /// checkpoint was blocked by pending uploads.
    pub async fn mark_crud_uploads_completed(&self) {
        let _ = self
            .local_download_events
            .send(DownloadEvent::CompletedUpload)
            .await;
    }
}
