use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

use async_channel::Receiver;
use futures_lite::{
    StreamExt,
    future::{self},
};
use log::{debug, info, warn};
use powersync_sqlite_nostd::{Destructor, ResultCode};

use crate::{
    MutationUploader, SyncOptions,
    db::connection::{SqliteConnection, TransactionGuard},
    error::RawPowerSyncError,
    sync::{
        download::http::checkpoint_request,
        instruction::CheckpointRequestPayload,
        options::{CheckpointMode, EndpointAndAuthenticator},
    },
};
use crate::{
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::{MAX_OP_ID, download::http::write_checkpoint, status::UploadStatus},
};
use crate::{db::watch::ListenerConfiguration, sync::coordinator::SyncChannels};

pub async fn crud_upload_loop(
    db: Arc<InnerPowerSyncState>,
    options: SyncOptions,
    channels: SyncChannels,
    trigger_uploads: Receiver<()>,
) {
    let common: CommonCrudUpload<'_> = CommonCrudUpload {
        options: &options,
        channels: &channels,
        db: db.as_ref(),
    };

    let uploader = match (&options.endpoint, &options.uploader) {
        (_, Some(uploader)) => uploader,
        (Some(endpoint), None) => {
            // We can't upload data, but we're connected for downloads. We might have been connected
            // for uploads before, and thus need to request a checkpoint before syncing completed
            // uploads.
            common.request_target_checkpoint_once(endpoint).await;
            return;
        }
        _ => return,
    };

    let mut tables = HashSet::new();
    tables.insert("ps_crud".to_string());

    let mut stream = db
        .env
        .pool
        .update_notifiers()
        .listen(ListenerConfiguration::if_matches(tables, false));

    loop {
        let next_trigger = future::or(
            async {
                stream.next().await?;
                Some(())
            },
            async {
                trigger_uploads.recv().await.ok()?;
                Some(())
            },
        );

        let mut upload = CrudUpload {
            common,
            uploader: uploader.as_ref(),
        };
        upload.run().await;
        next_trigger.await;
    }
}

pub async fn post_checkpoint_request(
    client_id: String,
    authenticator: &EndpointAndAuthenticator,
    channels: &SyncChannels,
    db: &InnerPowerSyncState,
) -> Result<i64, PowerSyncError> {
    channels
        .checkpoints
        .wait_for_checkpoint_requests_ready(true)
        .await
        .map_err(|e| RawPowerSyncError::Checkpoint { error: e })?;

    let checkpoint_request_id = db.next_checkpoint_request_id().await?;
    checkpoint_request(
        db,
        authenticator,
        &CheckpointRequestPayload {
            client_id,
            checkpoint_request_id,
        },
    )
    .await
}

#[derive(Clone, Copy)]
struct CommonCrudUpload<'a> {
    options: &'a SyncOptions,
    channels: &'a SyncChannels,
    db: &'a InnerPowerSyncState,
}

struct CrudUpload<'a> {
    common: CommonCrudUpload<'a>,
    uploader: &'a dyn MutationUploader,
}

impl<'a> CrudUpload<'a> {
    pub async fn run(&mut self) {
        let mut last_item_id = None::<i64>;
        scopeguard::defer! {
            self.common.db.status.update(|s| s.set_upload_state(UploadStatus::Idle));
        }

        // Invoke upload method on connector until there are no remaining CRUD items to upload.
        loop {
            match self.upload_step(&mut last_item_id).await {
                Ok(ControlFlow::Break(_)) => break,
                Ok(ControlFlow::Continue(_)) => continue,
                Err(e) => {
                    last_item_id = None;
                    info!("CRUD uploads failed, will retry, {e}");

                    let db = self.common.db;
                    db.status
                        .update(|data| data.set_upload_state(UploadStatus::Error(e)));
                    self.common.options.retry_delay(&db.env).await;
                }
            }
        }
    }

    async fn upload_step(
        &self,
        last_item_id: &mut Option<i64>,
    ) -> Result<ControlFlow<()>, PowerSyncError> {
        let Some(item) = self.oldest_crud_item_id().await? else {
            // Uploading is completed, advance write checkpoint.
            if let Some(ref endpoint) = self.common.options.endpoint {
                self.common.request_checkpoint_if_needed(endpoint).await?;
            }

            return Ok(ControlFlow::Break(()));
        };

        self.common
            .db
            .status
            .update(|data| data.set_upload_state(UploadStatus::Uploading));
        if matches!(*last_item_id, Some(x) if x == item) {
            warn!("{}", Self::DUPLICATE_ITEM_WARNING);
            return Err(PowerSyncError::argument_error(
                "Delaying due to previously encountered CRUD item.",
            ));
        }

        *last_item_id = Some(item);
        self.uploader.upload().await?;

        Ok(ControlFlow::Continue(()))
    }

    async fn oldest_crud_item_id(&self) -> Result<Option<i64>, PowerSyncError> {
        let reader = self.common.db.reader().await?;
        Self::read_oldest_crud_item_id(reader.sqlite_connection())
    }

    fn read_oldest_crud_item_id(conn: &SqliteConnection) -> Result<Option<i64>, PowerSyncError> {
        let stmt = conn.prepare("SELECT id FROM ps_crud ORDER BY id LIMIT 1")?;

        Ok(match stmt.step()? {
            ResultCode::ROW => Some(stmt.column_int64(0)),
            _ => None,
        })
    }

    const DUPLICATE_ITEM_WARNING: &'static str = "
Potentially previously uploaded CRUD entries are still present in the upload queue.
Make sure to handle uploads and complete CRUD transactions or batches by calling and awaiting their
`complete()` method.
The next upload iteration will be delayed.";
}

impl<'a> CommonCrudUpload<'a> {
    async fn request_checkpoint_if_needed(
        &self,
        endpoint: &EndpointAndAuthenticator,
    ) -> Result<(), PowerSyncError> {
        let did_request_checkpoint =
            if let Some(advance_target) = self.sequence_for_checkpoint().await? {
                let write_checkpoint = self.get_write_checkpoint(endpoint).await?;
                advance_target.complete(write_checkpoint, &self.db).await?;
            };

        // It's possible that pending CRUD uploads were preventing data from  syncing. So now
        // that that's completed, notify the download client in case it needs to retry.
        self.channels.mark_crud_uploads_completed().await;

        Ok(did_request_checkpoint)
    }

    async fn get_write_checkpoint(
        &self,
        endpoint: &EndpointAndAuthenticator,
    ) -> Result<i64, PowerSyncError> {
        let client_id = get_client_id(&self.db).await?;

        match self.options.checkpoints {
            CheckpointMode::Legacy => write_checkpoint(&self.db, &client_id, endpoint).await,
            CheckpointMode::Requests(_) => {
                post_checkpoint_request(client_id, endpoint, &self.channels, &self.db).await
            }
        }
    }

    fn ps_crud_sequence(tx: &TransactionGuard) -> Result<Option<i64>, PowerSyncError> {
        let seq_before = tx
            .inner
            .prepare("SELECT seq FROM main.sqlite_sequence WHERE name = ?")?;
        seq_before.bind_text(1, "ps_crud", Destructor::STATIC)?;

        let ResultCode::ROW = seq_before.step()? else {
            return Ok(None);
        };

        Ok(Some(seq_before.column_int64(0)))
    }

    async fn sequence_for_checkpoint(
        &self,
    ) -> Result<Option<PendingCheckpointRequest>, PowerSyncError> {
        let mut reader = self.db.reader().await?;
        let reader = reader.sqlite_connection_mut();
        let read_tx = TransactionGuard::new(reader)?;

        let current_target = InnerPowerSyncState::target_checkpoint_request_id(&read_tx, None)?;
        if current_target != Some(MAX_OP_ID) {
            // Nothing to update.
            return Ok(None);
        }

        let seq_before = Self::ps_crud_sequence(&read_tx)?;
        Ok(seq_before.map(|seq_before| PendingCheckpointRequest {
            crud_sequence: seq_before,
        }))
    }

    /// Tries requesting a checkpoint until that is successful.
    async fn request_target_checkpoint_once(&self, endpoint: &EndpointAndAuthenticator) {
        scopeguard::defer! {
            self.db.status.update(|s| s.set_upload_state(UploadStatus::Idle));
        }

        loop {
            match self.request_checkpoint_if_needed(endpoint).await {
                Ok(()) => break,
                Err(e) => {
                    self.db
                        .status
                        .update(|s| s.set_upload_state(UploadStatus::Error(e)));

                    self.options.retry_delay(&self.db.env).await;
                }
            }
        }
    }
}

struct PendingCheckpointRequest {
    crud_sequence: i64,
}

impl PendingCheckpointRequest {
    pub async fn complete(
        self,
        op_id: i64,
        db: &InnerPowerSyncState,
    ) -> Result<(), PowerSyncError> {
        info!("Updating target to checkpoint {}", self.crud_sequence);

        let mut writer = db.writer().await?;
        let writer = TransactionGuard::new(writer.sqlite_connection_mut())?;

        if CrudUpload::read_oldest_crud_item_id(writer.inner)?.is_some() {
            warn!("ps_crud is not empty, won't advance target");
            return Ok(());
        }

        let seq_after = CommonCrudUpload::ps_crud_sequence(&writer)?
            .expect("sqlite sequence should not be empty");

        if seq_after != self.crud_sequence {
            debug!(
                "Sequence on ps_crud changed while fetching checkpoint. From {} to {}",
                self.crud_sequence, seq_after
            );
            return Ok(());
        }

        InnerPowerSyncState::target_checkpoint_request_id(&writer, Some(op_id))?;

        writer.commit()?;
        Ok(())
    }
}

pub async fn get_client_id(db: &InnerPowerSyncState) -> Result<String, PowerSyncError> {
    let reader = db.reader().await?;

    let stmt = reader
        .sqlite_connection()
        .prepare("SELECT powersync_client_id()")?;
    let ResultCode::ROW = stmt.step()? else {
        panic!("Expected row"); // Can't happen, scalar select
    };

    Ok(stmt.column_text(0)?.to_string())
}
