use async_lock::{Mutex as AsyncMutex, MutexGuard};
use std::sync::Arc;

use async_channel::{Receiver, Sender};

use crate::{
    CheckpointError, CheckpointMode, CheckpointRequest, SyncOptions,
    db::{
        DisconnectAndClearFlags,
        connection::{TransactionGuard, exec_stmt},
        internal::InnerPowerSyncState,
    },
    env::PowerSyncTask,
    error::PowerSyncError,
    sync::{
        checkpoint::repost_unacknowledged_checkpoints,
        download::{DownloadEvent, download_loop},
        instruction::Instruction,
        state::CheckpointStateSignals,
        streams::ChangedSyncSubscriptions,
        upload::{crud_upload_loop, get_client_id, post_checkpoint_request},
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
        let mut guard = self.task.lock().await;
        let _ = Self::disconnect_in(&mut guard, &db).await;

        let (channels, download_receive, uploads_receive) = SyncChannels::create();

        let downloads = db.env.spawn(download_loop(
            db.clone(),
            channels.clone(),
            options.clone(),
            download_receive,
        ));
        let uploads = db.env.spawn(crud_upload_loop(
            db.clone(),
            options.clone(),
            channels.clone(),
            uploads_receive,
        ));
        let checkpoints = db.env.spawn(repost_unacknowledged_checkpoints(
            db.clone(),
            channels.clone(),
            options.clone(),
        ));

        *guard = Some(SyncTasks {
            channels,
            options,
            uploads: Some(uploads),
            downloads: Some(downloads),
            retried_checkpoints: Some(checkpoints),
        });
    }

    pub async fn disconnect(&self, db: &InnerPowerSyncState) {
        let mut guard = self.task.lock().await;
        let _ = Self::disconnect_in(&mut guard, db).await;
    }

    pub async fn disconnect_and_clear(
        &self,
        db: &InnerPowerSyncState,
        flags: DisconnectAndClearFlags,
    ) -> Result<(), PowerSyncError> {
        let mut guard = self.task.lock().await;
        Self::disconnect_in(&mut guard, db).await?;

        {
            let mut writer = db.writer().await?;
            let tx = TransactionGuard::new(writer.sqlite_connection_mut())?;
            let stmt = tx.inner.prepare("SELECT powersync_clear(?)")?;
            stmt.bind_int(1, flags.flags())?;
            exec_stmt(stmt)?;
            tx.commit()?;
        }

        let _ = Self::fetch_offline_sync_status(db).await;

        Ok(())
    }

    async fn disconnect_in<'a>(
        guard: &mut MutexGuard<'a, Option<SyncTasks>>,
        db: &InnerPowerSyncState,
    ) -> Result<(), PowerSyncError> {
        if let Some(task) = guard.take() {
            task.cancel().await;

            // If we have interrupted an active sync task, manually call stop. This is harmless if
            // we're already stopped, otherwise it gives us an instruction to update the sync
            // status to reflect removed connections.
            let mut writer = db.writer().await?;
            let instructions =
                DownloadEvent::Stop.invoke_control(writer.sqlite_connection_mut())?;

            for instruction in instructions {
                match instruction {
                    Instruction::UpdateSyncStatus { status } => {
                        db.status.update(|s| s.update_from_core(status))
                    }
                    _ => continue,
                }
            }
        }

        Ok(())
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

    pub async fn request_checkpoint(
        self: Arc<Self>,
        db: Arc<InnerPowerSyncState>,
    ) -> Result<CheckpointRequest, CheckpointError> {
        let guard = self.task.lock().await;
        let tasks = Self::extract_connected_with_requests(guard.as_ref())?;

        let channels = tasks.channels.clone();
        let connector = tasks.options.connector.clone();
        // Avoid holding the lock across a suspension point. It's fine if there's a concurrent
        // reconnect, post_checkpoint_request will return an error in that case.
        drop(guard);

        let client_id = get_client_id(&db)
            .await
            .map_err(CheckpointError::as_request_error)?;
        let checkpoint_request_id =
            post_checkpoint_request(client_id, connector.as_ref(), &channels, &db)
                .await
                .map_err(CheckpointError::as_request_error)?;

        Ok(CheckpointRequest::new(checkpoint_request_id, self, db))
    }

    pub async fn check_connected_with_requests_mode(&self) -> Result<(), CheckpointError> {
        let guard = self.task.lock().await;
        Self::extract_connected_with_requests(guard.as_ref())?;
        Ok(())
    }

    fn extract_connected_with_requests(
        tasks: Option<&SyncTasks>,
    ) -> Result<&SyncTasks, CheckpointError> {
        let Some(tasks) = tasks else {
            return Err(CheckpointError::Disconnected.into());
        };
        if !matches!(tasks.options.checkpoints, CheckpointMode::Requests(_)) {
            return Err(CheckpointError::Disabled.into());
        }

        Ok(tasks)
    }
}

struct SyncTasks {
    channels: SyncChannels,
    options: SyncOptions,
    downloads: Option<PowerSyncTask>,
    uploads: Option<PowerSyncTask>,
    retried_checkpoints: Option<PowerSyncTask>,
}

impl SyncTasks {
    fn tasks(&mut self) -> [&mut Option<PowerSyncTask>; 3] {
        [
            &mut self.downloads,
            &mut self.uploads,
            &mut self.retried_checkpoints,
        ]
    }

    pub async fn cancel(mut self) {
        for maybe_task in self.tasks() {
            if let Some(task) = maybe_task.take() {
                task.cancel_and_join().await;
            }
        }
    }
}

impl Drop for SyncTasks {
    fn drop(&mut self) {
        for maybe_task in self.tasks() {
            if let Some(task) = maybe_task.take() {
                task.cancel();
            }
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
