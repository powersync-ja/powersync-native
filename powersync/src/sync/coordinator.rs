use async_lock::Mutex as AsyncMutex;
use std::sync::Arc;

use async_channel::{Receiver, Sender};

use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    env::PowerSyncTask,
    error::PowerSyncError,
    sync::{
        download::{DownloadEvent, download_loop},
        state::CheckpointStateSignals,
        streams::ChangedSyncSubscriptions,
        upload::crud_upload_loop,
    },
};

/// Implements `connect()` and `disconnect()` by starting asynchronous tasks driving those loops.
///
/// Dropping the coordinator will also terminate sync tasks (albeit asynchronously).
#[derive(Default)]
pub struct SyncCoordinator {
    task: AsyncMutex<Option<SyncTasks>>,
}

impl SyncCoordinator {
    pub async fn connect(self: Arc<Self>, db: Arc<InnerPowerSyncState>, options: SyncOptions) {
        self.disconnect(&db).await;

        let mut guard = self.task.lock().await;

        let (channels, download_receive, uploads_receive) = SyncChannels::create();

        let downloads = db.env.spawn(download_loop(
            db.clone(),
            channels.clone(),
            options.clone(),
            download_receive,
        ));
        let uploads = db.env.spawn(crud_upload_loop(
            db.clone(),
            options,
            channels.clone(),
            uploads_receive,
        ));

        *guard = Some(SyncTasks {
            channels,
            uploads: Some(uploads),
            downloads: Some(downloads),
        });
    }

    pub async fn disconnect(&self, db: &InnerPowerSyncState) {
        let mut guard = self.task.lock().await;

        if let Some(task) = guard.take() {
            task.cancel().await;
            let _ = Self::fetch_offline_sync_status(db).await;
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

        Self::fetch_offline_sync_status(db).await
    }

    async fn fetch_offline_sync_status(db: &InnerPowerSyncState) -> Result<(), PowerSyncError> {
        let writer = db.writer().await?;
        db.status
            .update(|s| s.resolve_offline_state(writer.sqlite_connection()))
    }

    /// Handle the set of active sync stream subscriptions changing.
    ///
    /// This is a no-op if not connected.
    pub async fn handle_subscriptions_changed(&self, update: ChangedSyncSubscriptions) {
        let Some(channel) = ({
            let guard = self.task.lock().await;

            guard
                .as_ref()
                .map(|tasks| tasks.channels.local_download_events.clone())
        }) else {
            return;
        };

        let _ = channel
            .send(DownloadEvent::UpdateSubscriptions { keys: update.0 })
            .await;
    }
}

struct SyncTasks {
    channels: SyncChannels,
    downloads: Option<PowerSyncTask>,
    uploads: Option<PowerSyncTask>,
}

impl SyncTasks {
    pub async fn cancel(mut self) {
        if let Some(task) = self.downloads.take() {
            task.cancel_and_join().await;
        }
        if let Some(task) = self.uploads.take() {
            task.cancel_and_join().await;
        }
    }
}

impl Drop for SyncTasks {
    fn drop(&mut self) {
        if let Some(task) = self.downloads.take() {
            task.cancel();
        }
        if let Some(task) = self.uploads.take() {
            task.cancel();
        }
    }
}

#[derive(Clone)]
pub struct SyncChannels {
    pub checkpoints: Arc<CheckpointStateSignals>,
    local_download_events: Sender<DownloadEvent>,
    trigger_upload: Sender<()>,
}

impl SyncChannels {
    pub fn create() -> (Self, Receiver<DownloadEvent>, Receiver<()>) {
        let (download_send, download_receive) = async_channel::unbounded();
        let (uploads_send, uploads_receive) = async_channel::bounded(1);

        (
            Self {
                checkpoints: Default::default(),
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
