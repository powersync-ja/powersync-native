use std::sync::Arc;

use async_channel::Receiver;
use rusqlite::{Connection, params};

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
