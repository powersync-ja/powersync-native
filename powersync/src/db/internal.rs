use crate::db::connection::{SqliteConnection, TransactionGuard, exec_stmt};
use crate::schema::SchemaOrCustom;
use crate::{
    db::{
        core_extension::CoreExtensionVersion, pool::LeasedConnection, streams::SyncStreamTracker,
    },
    env::PowerSyncEnvironment,
    error::PowerSyncError,
    sync::{MAX_OP_ID, coordinator::SyncCoordinator, status::SyncStatus, status::SyncStatusData},
    util::SharedFuture,
};
use event_listener::EventListener;
use futures_lite::{FutureExt, Stream, StreamExt, ready};
use powersync_sqlite_nostd::{Destructor, ResultCode};
use std::sync::{Mutex, Weak};
use std::time::Duration;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub struct InnerPowerSyncState {
    /// External dependencies (timers and HTTP clients) used to implement SDK functionality.
    pub env: PowerSyncEnvironment,
    /// Whether the database has been initialized.
    did_initialize: SharedFuture<Result<(), PowerSyncError>>,
    /// The schema passed to the database.
    ///
    /// This is forwarded to the sync client for raw tables.
    pub schema: Arc<SchemaOrCustom>,
    /// A container for the current sync status.
    pub status: SyncStatus,
    /// A collection of currently-referenced sync stream subscriptions.
    pub(crate) current_streams: SyncStreamTracker,
    /// Clients have a strong reference to the sync coordinator, but since sync actors have a
    /// reference to [InnerPowerSyncState], we only keep a weak reference here to ensure we can drop
    /// actors through the channels owned by [SyncCoordinator].
    pub(crate) sync: Weak<SyncCoordinator>,
    pub(crate) retry_delay: Mutex<Option<Duration>>,
}

impl InnerPowerSyncState {
    pub fn new(
        env: PowerSyncEnvironment,
        schema: SchemaOrCustom,
        sync: &Arc<SyncCoordinator>,
    ) -> Self {
        Self {
            env,
            did_initialize: SharedFuture::new(),
            schema: Arc::new(schema),
            status: SyncStatus::new(),
            current_streams: SyncStreamTracker::default(),
            retry_delay: Default::default(),
            sync: Arc::downgrade(sync),
        }
    }

    async fn initialize(&self) -> Result<(), PowerSyncError> {
        let pool = &self.env.pool;
        self.did_initialize
            .run(|| async {
                let conn = pool.writer().await;
                let conn = conn.sqlite_connection();
                CoreExtensionVersion::check_from_db(conn)?;

                conn.exec(c"SELECT powersync_init()")?;

                self.update_schema_internal(conn)?;
                self.status.update(|old| old.resolve_offline_state(conn))?;

                Ok(())
            })
            .await
            .clone()
    }

    fn update_schema_internal(&self, conn: &SqliteConnection) -> Result<(), PowerSyncError> {
        if let SchemaOrCustom::Schema(schema) = self.schema.as_ref() {
            schema.validate()?;
        };

        let serialized_schema = serde_json::to_string(&self.schema)?;
        let stmt = conn.prepare("SELECT powersync_replace_schema(?)")?;
        // Fine because we drop the statement before the serialized schema
        stmt.bind_text(1, &serialized_schema, Destructor::STATIC)?;
        exec_stmt(stmt)?;

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
        let writer = TransactionGuard::new(writer.sqlite_connection_mut())?;

        {
            let stmt = writer.inner.prepare("DELETE FROM ps_crud WHERE id <= ?")?;
            stmt.bind_int64(1, last_client_id)?;
            exec_stmt(stmt)?;
        }

        let mut target_op: i64 = MAX_OP_ID;
        if let Some(write_checkpoint) = write_checkpoint {
            // If there are no remaining crud items we can set the target op to the checkpoint.
            let stmt = writer.inner.prepare("SELECT 1 FROM ps_crud LIMIT 1")?;
            if let ResultCode::OK = stmt.step()? {
                target_op = write_checkpoint;
            }
        }

        Self::set_local_target_op(writer.inner, target_op)?;
        writer.commit()
    }

    pub fn set_local_target_op(writer: &SqliteConnection, op: i64) -> Result<(), PowerSyncError> {
        let stmt = writer.prepare("UPDATE ps_buckets SET target_op = ? WHERE name = ?")?;
        stmt.bind_int64(1, op)?;
        stmt.bind_text(2, "$local", Destructor::STATIC)?;
        exec_stmt(stmt)
    }

    pub async fn reader(&self) -> Result<LeasedConnection, PowerSyncError> {
        self.initialize().await?;
        Ok(self.env.pool.reader().await)
    }

    pub async fn writer(&self) -> Result<LeasedConnection, PowerSyncError> {
        self.initialize().await?;
        Ok(self.env.pool.writer().await)
    }

    pub async fn sync_iteration_delay(&self) {
        let delay = {
            let guard = self.retry_delay.lock().unwrap();
            *guard
        };

        if let Some(delay) = delay {
            self.env.timer.delay_once(delay).await
        }
    }

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
