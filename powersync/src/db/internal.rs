use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::EventListener;
use futures_lite::{FutureExt, Stream, StreamExt, ready};
use rusqlite::{Connection, params};

use crate::{
    PowerSyncEnvironment,
    db::{
        core_extension::CoreExtensionVersion, pool::LeasedConnection, streams::SyncStreamTracker,
    },
    error::PowerSyncError,
    schema::Schema,
    sync::{MAX_OP_ID, coordinator::SyncCoordinator, status::SyncStatus, status::SyncStatusData},
    util::SharedFuture,
};

pub struct InnerPowerSyncState {
    pub env: PowerSyncEnvironment,
    did_initialize: SharedFuture<Result<(), PowerSyncError>>,
    pub schema: Arc<Schema<'static>>,
    pub status: SyncStatus,
    pub sync: SyncCoordinator,
    pub current_streams: SyncStreamTracker,
}

impl InnerPowerSyncState {
    pub fn new(env: PowerSyncEnvironment, schema: Schema<'static>) -> Self {
        Self {
            env,
            did_initialize: SharedFuture::new(),
            schema: Arc::new(schema),
            status: SyncStatus::new(),
            sync: Default::default(),
            current_streams: SyncStreamTracker::default(),
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
        let mut target_op: i64 = MAX_OP_ID;
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

    pub fn watch_status<'a>(&'a self) -> impl Stream<Item = Arc<SyncStatusData>> + 'a {
        struct StreamImpl<'a> {
            db: &'a InnerPowerSyncState,
            last_data: Option<Arc<SyncStatusData>>,
            waiter: Option<EventListener>,
        }

        impl<'a> Stream for StreamImpl<'a> {
            type Item = Arc<SyncStatusData>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                let this = &mut *self;

                let Some(last_data) = &mut this.last_data else {
                    // First poll, return immediately with the initial snapshot.
                    let data = this.db.status.current_snapshot();
                    this.last_data = Some(data.clone());
                    return Poll::Ready(Some(data));
                };

                loop {
                    // Are we already waiting? If so, continue.
                    if let Some(waiter) = &mut this.waiter {
                        ready!(waiter.poll(cx));
                        this.waiter = None;

                        let data = this.db.status.current_snapshot();
                        *last_data = data.clone();
                        return Poll::Ready(Some(data));
                    }

                    // Wait for previous data to become outdated.
                    let Some(listener) = last_data.listen_for_changes() else {
                        let data = this.db.status.current_snapshot();
                        *last_data = data.clone();
                        return Poll::Ready(Some(data));
                    };

                    this.waiter = Some(listener);
                }
            }
        }

        StreamImpl {
            db: self,
            last_data: None,
            waiter: None,
        }
    }

    pub async fn wait_for_status(&self, mut predicate: impl FnMut(&SyncStatusData) -> bool) {
        let mut stream = self.watch_status();
        loop {
            let status = stream.next().await.unwrap();
            if predicate(&status) {
                return;
            }
        }
    }
}
