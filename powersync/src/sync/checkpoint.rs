use std::sync::Arc;

use log::{debug, warn};
use thiserror::Error;

use crate::{
    BackendConnector, CheckpointMode, RequestsCheckpointMode, SyncOptions,
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::{
        coordinator::SyncChannels, download::http::checkpoint_request,
        instruction::CheckpointRequestPayload, upload::get_client_id,
    },
};

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
