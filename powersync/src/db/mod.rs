use std::sync::Arc;

use futures_lite::{Stream, StreamExt};

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

pub mod core_extension;
pub mod crud;
pub(crate) mod internal;
pub mod pool;
pub mod schema;
pub mod streams;
pub mod watch;

#[derive(Clone)]
pub struct PowerSyncDatabase {
    inner: Arc<InnerPowerSyncState>,
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
        Self {
            inner: Arc::new(InnerPowerSyncState::new(env, schema)),
        }
    }

    /// Starts an actor responsible for establishing a connection to the sync service when
    /// [Self::connect] is called on the database.
    ///
    /// By exposing this as an entrypoint instead of starting the task on a specific entrypoint,
    /// the SDK stays executor-agnostic and is easier to access from C.
    pub fn download_actor(&self) -> impl Future<Output = ()> + 'static {
        // Important: This needs to run outside of the future, so that the channel is created before
        // this function completes.
        let mut actor = DownloadActor::new(self.inner.clone());
        async move { actor.run().await }
    }

    /// Starts an actor responsible for watching the local database for local writes that need to
    /// be uploaded.
    ///
    /// By exposing this as an entrypoint instead of starting the task on a specific entrypoint,
    /// the SDK stays executor-agnostic and is easier to access from C.
    pub fn upload_actor(&self) -> impl Future<Output = ()> + 'static {
        let mut actor = UploadActor::new(self.inner.clone());
        async move { actor.run().await }
    }

    /// Requests the download actor, started with [Self::download_actor], to start establishing a
    /// connection to the PowerSync service.
    pub async fn connect(&self, options: SyncOptions) {
        self.inner.sync.connect(options).await
    }

    /// If the sync client is currently connected, requests it to disconnect.
    pub async fn disconnect(&self) {
        self.inner.sync.disconnect().await
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
        CrudTransactionStream::new(self)
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

    /// Returns the shared [InnerPowerSyncState] backing this database.
    ///
    /// This is meant to be used internally to build the PowerSync C++ SDK.
    #[cfg(feature = "ffi")]
    pub fn into_raw(self) -> *const InnerPowerSyncState {
        Arc::into_raw(self.inner)
    }

    /// Creates a [Self] from the raw inner pointer.
    ///
    /// ## Safety
    ///
    /// The inner pointer must have been obtained from [Self::into_raw] without calling
    /// [Self::drop_raw] on it in the meantime.
    #[cfg(feature = "ffi")]
    pub unsafe fn interpret_raw(inner: *const InnerPowerSyncState) -> Self {
        unsafe { Arc::increment_strong_count(inner) };
        Self {
            inner: unsafe { Arc::from_raw(inner) },
        }
    }

    /// Frees resources from a [Self::into_raw] pointer.
    ///
    /// ## Safety
    ///
    /// This method must be called at most once on a pointer previously returned by
    /// [Self::into:raw].
    #[cfg(feature = "ffi")]
    pub unsafe fn drop_raw(inner: *const InnerPowerSyncState) {
        drop(unsafe { Arc::from_raw(inner) });
    }
}
