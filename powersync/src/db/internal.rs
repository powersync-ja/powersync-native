use std::sync::Arc;

use async_channel::Receiver;
use rusqlite::{Connection, Transaction, params};

use crate::{
    PowerSyncEnvironment,
    db::{core_extension::CoreExtensionVersion, pool::LeasedConnection},
    error::PowerSyncError,
    schema::Schema,
    sync::{AsyncRequest, download::DownloadActorCommand, status::SyncStatus},
    util::SharedFuture,
};

pub struct InnerPowerSyncState {
    pub env: PowerSyncEnvironment,
    did_initialize: SharedFuture<Result<(), PowerSyncError>>,
    pub schema: Arc<Schema<'static>>,
    pub receive_download_commands: Receiver<AsyncRequest<DownloadActorCommand>>,
    pub status: SyncStatus,
}

impl InnerPowerSyncState {
    pub fn new(
        env: PowerSyncEnvironment,
        schema: Schema<'static>,
        receive_download_commands: Receiver<AsyncRequest<DownloadActorCommand>>,
    ) -> Self {
        Self {
            env,
            did_initialize: SharedFuture::new(),
            schema: Arc::new(schema),
            status: SyncStatus::new(),
            receive_download_commands,
        }
    }

    async fn initialize(&self) -> Result<(), PowerSyncError> {
        let pool = &self.env.pool;
        self.did_initialize
            .run(|| async {
                let conn = pool.writer().await;
                CoreExtensionVersion::check_from_db(&conn)?;

                conn.prepare("SELECT powersync_init()")?
                    .query_row(params![], |_| Ok(()))?;

                self.update_schema_internal(&conn)?;
                self.status.update(|old| old.resolve_offline_state(&conn))?;

                Ok(())
            })
            .await
            .clone()
    }

    fn update_schema_internal(&self, conn: &Connection) -> Result<(), PowerSyncError> {
        let serialized_schema = serde_json::to_string(&self.schema)?;
        conn.prepare("SELECT powersync_replace_schema(?)")?
            .query_one(params![serialized_schema], |_| Ok(()))?;
        // TODO: Update readers? Should be fine at the moment because we're only doing this during
        // initialization.
        Ok(())
    }

    /// Marks all crud items up until the `last_client_id` (inclusive) as handled and optionally
    /// applies a custom write checkpoint.
    pub async fn complete_crud_items(
        &self,
        last_client_id: i64,
        write_checkpoint: Option<i64>,
    ) -> Result<(), PowerSyncError> {
        let mut writer = self.writer().await?;
        let writer = writer.transaction()?;

        writer.execute("DELETE FROM ps_crud WHERE id <= ?", params![last_client_id])?;
        let mut target_op: i64 = 9223372036854775807;
        if let Some(write_checkpoint) = write_checkpoint {
            // If there are no remaining crud items we can set the target op to the checkpoint.
            let mut stmt = writer.prepare("SELECT 1 FROM ps_crud LIMIT 1")?;
            if stmt.query(params![])?.next()?.is_none() {
                target_op = write_checkpoint;
            }
        }

        writer.execute(
            "UPDATE ps_buckets SET target_op = ? WHERE name = ?",
            params![target_op, "$local"],
        )?;
        writer.commit()?;

        Ok(())
    }

    pub async fn reader(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.initialize().await?;
        Ok(self.env.pool.reader().await)
    }

    pub async fn writer(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.initialize().await?;
        Ok(self.env.pool.writer().await)
    }

    pub async fn sync_iteration_delay(&self) {}
}
