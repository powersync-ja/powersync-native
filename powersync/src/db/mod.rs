use std::sync::Arc;

use futures_lite::{FutureExt, Stream, StreamExt};

use crate::db::async_support::AsyncDatabaseTasks;
use crate::sync::coordinator::SyncCoordinator;
use crate::{
    CrudTransaction, SyncOptions,
    db::{
        crud::CrudTransactionStream, internal::InnerPowerSyncState, pool::LeasedConnection,
        streams::SyncStream,
    },
    env::PowerSyncEnvironment,
    error::PowerSyncError,
    schema::Schema,
    sync::{download::DownloadActor, status::SyncStatusData, upload::UploadActor},
};

mod async_support;
pub mod core_extension;
pub mod crud;
pub(crate) mod internal;
pub mod pool;
pub mod schema;
pub mod streams;
pub mod watch;

#[derive(Clone)]
pub struct PowerSyncDatabase {
    pub(crate) sync: Arc<SyncCoordinator>,
    pub(crate) inner: Arc<InnerPowerSyncState>,
}

/// An opened database managed by the PowerSync SDK.
///
/// To run SQL statements against this database, use [Self::reader] and [Self::writer].
///
/// To connect to a PowerSync service, first call [Self::download_actor] and [Self::upload_actor]
/// and have those futures be polled by an async runtime of your choice. They will run as long as
/// the database is active.
/// Then, call [Self::connect] to establish a connection to the PowerSync service.
impl PowerSyncDatabase {
    pub fn new(env: PowerSyncEnvironment, schema: Schema) -> Self {
        let coordinator = Arc::new(SyncCoordinator::default());

        Self {
            inner: Arc::new(InnerPowerSyncState::new(env, schema, &coordinator)),
            sync: coordinator,
        }
    }

    /// Returns a collection of [AsyncDatabaseTasks] that need to be started before connecting this
    /// PowerSync database to a PowerSync service.
    ///
    /// To start the tasks, see the documentation on [AsyncDatabaseTasks].
    ///
    /// By exposing these async tasks instead of starting them automatically, the SDK stays
    /// executor-agnostic and is easier to access from C.
    #[must_use = "Returned tasks still need to be spawned on an async runtime"]
    pub fn async_tasks(&self) -> AsyncDatabaseTasks {
        let mut downloads = DownloadActor::new(self.inner.clone(), &self.sync);
        let mut uploads = UploadActor::new(self.inner.clone(), &self.sync);

        AsyncDatabaseTasks::new(
            async move { downloads.run().await }.boxed(),
            async move { uploads.run().await }.boxed(),
        )
    }

    /// Requests the download actor, started with [Self::download_actor], to start establishing a
    /// connection to the PowerSync service.
    pub async fn connect(&self, options: SyncOptions) {
        self.sync.connect(options, &self.inner).await
    }

    /// If the sync client is currently connected, requests it to disconnect.
    pub async fn disconnect(&self) {
        self.sync.disconnect().await
    }

    /// Returns an asynchronous [Stream] emitting an empty event every time one of the specified
    /// tables is written to.
    ///
    /// The `emit_initially` option can be used to control whether the stream should emit as well
    /// when polled for the first time. This can be useful to build streams emitting a complete
    /// snapshot of results every time a source table is changed.
    pub fn watch_tables<'a>(
        &self,
        emit_initially: bool,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> impl Stream<Item = ()> {
        self.inner.env.pool.update_notifiers().listen(
            emit_initially,
            tables
                .into_iter()
                .flat_map(|s| {
                    [
                        s.to_string(),
                        format!("ps_data__{s}"),
                        format!("ps_data_local__{s}"),
                    ]
                })
                .collect(),
        )
    }

    /// Returns a [Stream] traversing through transactions that have been completed on this
    /// database.
    ///
    /// Each [CrudTransaction] contains all local writes made in that transaction, allowing a
    /// backend connector to upload them.
    pub fn crud_transactions<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<CrudTransaction<'a>, PowerSyncError>> + 'a {
        CrudTransactionStream::new(&self.inner)
    }

    /// Returns the first transaction that has not been marked as completed.
    ///
    /// This is always the first item of [Self::crud_transactions], see that method for details.
    pub async fn next_crud_transaction<'a>(
        &'a self,
    ) -> Result<Option<CrudTransaction<'a>>, PowerSyncError> {
        let mut stream = self.crud_transactions();
        stream.try_next().await
    }

    /// Returns the current [SyncStatusData] snapshot reporting the sync state of this database.
    pub fn status(&self) -> Arc<SyncStatusData> {
        self.inner.status.current_snapshot()
    }

    /// Returns an updating [Stream] of [SyncStatusData] events emitting every time the status is
    /// changed.
    pub fn watch_status<'a>(&'a self) -> impl Stream<Item = Arc<SyncStatusData>> + 'a {
        self.inner.watch_status()
    }

    /// Creates a [SyncStream] based on name and optional parameters.
    ///
    /// PowerSync will sync data from the requested stream when calling [SyncStream::subscribe].
    /// After the subscription handle returned by that method is dropped, PowerSync will continue
    /// syncing the stream for a specified amount of time as a local cache.
    pub fn sync_stream<'a>(
        &'a self,
        name: &'a str,
        parameters: Option<&serde_json::Value>,
    ) -> SyncStream<'a> {
        SyncStream::new(
            self,
            name,
            parameters.map(|e| e.as_object().expect("Parameters should be a JSON object")),
        )
    }

    /// Obtains a [LeasedConnection] that can be used to run read-only queries on this database.
    pub async fn reader(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.reader().await
    }

    /// Obtains a [LeasedConnection] allowing reading and writing queries.
    pub async fn writer(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.writer().await
    }
}
