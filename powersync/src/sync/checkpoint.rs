use thiserror::Error;

use crate::error::PowerSyncError;

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
