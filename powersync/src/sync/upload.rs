use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

use async_channel::Receiver;
use futures_lite::{
    StreamExt,
    future::{self},
};
use log::{debug, info, warn};
use powersync_sqlite_nostd::{Destructor, ResultCode};

use crate::{
    SyncOptions,
    db::connection::{SqliteConnection, TransactionGuard},
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
            options: &options,
            db: &db,
            channels: &channels,
        };
        upload.run().await;
        next_trigger.await;
    }
}

struct CrudUpload<'a> {
    options: &'a SyncOptions,
    channels: &'a SyncChannels,
    db: &'a InnerPowerSyncState,
}

impl<'a> CrudUpload<'a> {
    pub async fn run(&mut self) {
        let mut last_item_id = None::<i64>;
        scopeguard::defer! {
            self.db.status.update(|s| s.set_upload_state(UploadStatus::Idle));
        }

        // Invoke upload method on connector until there are no remaining CRUD items to upload.
        loop {
            match self.upload_step(&mut last_item_id).await {
                Ok(ControlFlow::Break(_)) => break,
                Ok(ControlFlow::Continue(_)) => continue,
                Err(e) => {
                    last_item_id = None;
                    info!("CRUD uploads failed, will retry, {e}");

                    self.db
                        .status
                        .update(|data| data.set_upload_state(UploadStatus::Error(e)));
                    self.options.retry_delay(&self.db.env).await;
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
            if let Some(advance_target) = self.sequence_for_checkpoint().await? {
                let write_checkpoint = self.get_write_checkpoint().await?;
                advance_target.complete(write_checkpoint, &self.db).await?;
            }

            // It's possible that pending CRUD uploads were preventing data from  syncing. So now
            // that that's completed, notify the download client in case it needs to retry.
            self.channels.mark_crud_uploads_completed().await;

            return Ok(ControlFlow::Break(()));
        };

        self.db
            .status
            .update(|data| data.set_upload_state(UploadStatus::Uploading));
        if matches!(*last_item_id, Some(x) if x == item) {
            warn!("{}", Self::DUPLICATE_ITEM_WARNING);
            return Err(PowerSyncError::argument_error(
                "Delaying due to previously encountered CRUD item.",
            ));
        }

        *last_item_id = Some(item);
        self.options.connector.upload_data().await?;

        Ok(ControlFlow::Continue(()))
    }

    async fn oldest_crud_item_id(&self) -> Result<Option<i64>, PowerSyncError> {
        let reader = self.db.reader().await?;
        Self::read_oldest_crud_item_id(reader.sqlite_connection())
    }

    async fn get_write_checkpoint(&self) -> Result<i64, PowerSyncError> {
        let client_id = {
            let reader = self.db.reader().await?;

            let stmt = reader
                .sqlite_connection()
                .prepare("SELECT powersync_client_id()")?;
            let ResultCode::ROW = stmt.step()? else {
                panic!("Expected row"); // Can't happen, scalar select
            };

            stmt.column_text(0)?.to_string()
        };

        let credentials = self.options.connector.fetch_credentials().await?;
        write_checkpoint(&self.db, &client_id, credentials).await
    }

    fn read_oldest_crud_item_id(conn: &SqliteConnection) -> Result<Option<i64>, PowerSyncError> {
        let stmt = conn.prepare("SELECT id FROM ps_crud ORDER BY id LIMIT 1")?;

        Ok(match stmt.step()? {
            ResultCode::ROW => Some(stmt.column_int64(0)),
            _ => None,
        })
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

    const DUPLICATE_ITEM_WARNING: &'static str = "
Potentially previously uploaded CRUD entries are still present in the upload queue.
Make sure to handle uploads and complete CRUD transactions or batches by calling and awaiting their
`complete()` method.
The next upload iteration will be delayed.";
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

        let seq_after =
            CrudUpload::ps_crud_sequence(&writer)?.expect("sqlite sequence should not be empty");

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
