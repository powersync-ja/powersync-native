use std::sync::Arc;

use futures_lite::StreamExt;
use log::{debug, warn};
use thiserror::Error;

use crate::{
    BackendConnector, CheckpointMode, RequestsCheckpointMode, SyncOptions, SyncStatusData,
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::{
        coordinator::{SyncChannels, SyncCoordinator},
        download::http::checkpoint_request,
        instruction::CheckpointRequestPayload,
        upload::get_client_id,
    },
};

/// A checkpoint requests created by [crate::PowerSyncDatabase::request_checkpoint].
///
/// Use this to wait until the local database has applied server-side changes up to the requested
/// checkpoint. This is useful for explicit refresh flows where the caller wants confirmation that
/// the local view has caught up to the service.
///
/// Checkpoint requests are backed by request ids tracked in the local database, so they are
/// reusable across disconnect and reconnect cycles. A [Self::wait_for_sync] interrupted by a
/// disconnect returns an error, but the same request can be awaited again once a new connection is
/// established.
///
/// Requests do not survive clearing a database, instances created before a clear should be
/// discarded and requested again.
#[derive(Clone)]
pub struct CheckpointRequest {
    id: i64,
    sync: Arc<SyncCoordinator>,
    db: Arc<InnerPowerSyncState>,
}

impl CheckpointRequest {
    pub(crate) fn new(id: i64, sync: Arc<SyncCoordinator>, db: Arc<InnerPowerSyncState>) -> Self {
        Self { id, sync, db }
    }

    /// Whether this checkpoint request has synced before.
    pub fn has_synced(&self) -> bool {
        self.has_synced_in(&self.db.status.current_snapshot())
    }

    /// Waits until this checkpoint has been synced locally.
    ///
    /// This method fails on sync errors: If a download or upload error occurs before this
    /// checkpoint request has synced, that error is returned here.
    /// This makes it easier to observe sync errors when relying on checkpoints. Once sync has
    /// recovered, it is valid to call this method again to await the checkpoint.
    pub async fn wait_for_sync(&self) -> Result<(), CheckpointError> {
        let mut stream = self.db.watch_status();
        loop {
            let status = stream.next().await.unwrap();
            if self.has_synced_in(&status) {
                break Ok(());
            }

            self.sync.check_connected_with_requests_mode().await?;

            if let Some(error) = status.any_error() {
                break Err(CheckpointError::StatusError {
                    cause: error.clone(),
                });
            }

            if !status.is_connected() && !status.is_connecting() {
                break Err(CheckpointError::Disconnected);
            }
        }
    }

    fn has_synced_in(&self, status: &SyncStatusData) -> bool {
        status.is_checkpoint_request_applied(self.id)
    }
}

#[derive(Error, Debug)]
pub enum CheckpointError {
    #[error(
        "The PowerSync service does not support checkpoint requests. Update to PowerSync service version 1.24.0 or later to use this API."
    )]
    InstanceNotSupported,
    #[error("Cannot request checkpoints, sync client is disconnected")]
    Disconnected,
    #[error("Connected with legacy checkpoint mode, cannot request checkpoints")]
    Disabled,
    #[error("Could not request checkpoint: {cause}")]
    CouldNotRequest { cause: PowerSyncError },
    #[error("Error on sync status before checkpoint was applied: {cause}")]
    StatusError { cause: PowerSyncError },
}

pub async fn repost_unacknowledged_checkpoints(
    db: Arc<InnerPowerSyncState>,
    channels: SyncChannels,
    options: SyncOptions,
) {
    let CheckpointMode::Requests(requests) = options.checkpoints else {
        return;
    };

    loop {
        // Make sure the system is seeded and ready.
        let result = repost_unacknowledged_checkpoint_iteration(
            &db,
            &channels,
            options.connector.as_ref(),
            requests,
        )
        .await;

        if let Err(err) = result {
            if let RawPowerSyncError::Checkpoint {
                error: CheckpointError::InstanceNotSupported,
            } = err.inner.as_ref()
            {
                return;
            }

            warn!("Error retrying checkpoint request: {err}");
            db.env.runtime.delay_once(requests.retry_delay).await;
        }
    }
}

async fn repost_unacknowledged_checkpoint_iteration(
    db: &InnerPowerSyncState,
    channels: &SyncChannels,
    connector: &dyn BackendConnector,
    mode: RequestsCheckpointMode,
) -> Result<(), PowerSyncError> {
    // Make sure the system is seeded and ready
    channels
        .checkpoints
        .wait_for_checkpoint_requests_ready(false)
        .await?;

    // Get the current checkpoint_request_id
    let Some(request_id) = db
        .current_checkpoint_request_id()
        .await?
        .take_if(|id| *id > 0)
    else {
        // This should not be reached. For completeness sake - wait a bit.
        db.env.runtime.delay_once(mode.retry_delay).await;
        return Ok(());
    };

    // Give the request some time to sync
    db.env.runtime.delay_once(mode.retry_delay).await;

    if db.current_checkpoint_request_id().await? != Some(request_id) {
        return Ok(());
    }

    // If the request was applied, we don't need to retry
    if db
        .status
        .current_snapshot()
        .is_checkpoint_request_applied(request_id)
    {
        return Ok(());
    }

    // Make sure we are online and ready before making the request
    channels
        .checkpoints
        .wait_for_checkpoint_requests_ready(false)
        .await?;

    // It's safe if this request races with a new one. The service will reject it.
    debug!("Retrying checkpoint request id {request_id}");
    checkpoint_request(
        &db,
        connector,
        &CheckpointRequestPayload {
            client_id: get_client_id(db).await?,
            checkpoint_request_id: request_id,
        },
    )
    .await?;

    Ok(())
}
