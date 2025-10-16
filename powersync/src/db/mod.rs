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

    pub fn upload_actor(&self) -> impl Future<Output = ()> + 'static {
        let mut actor = UploadActor::new(self.inner.clone());
        async move { actor.run().await }
    }

    /// Requests the download actor, started with [Self::download_actor], to start establishing a
    /// connection to the PowerSync service.
    pub async fn connect(&self, options: SyncOptions) {
        self.inner.sync.connect(options).await
    }

    pub async fn disconnect(&self) {
        self.inner.sync.disconnect().await
    }

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

    pub fn crud_transactions<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<CrudTransaction<'a>, PowerSyncError>> + 'a {
        CrudTransactionStream::new(self)
    }

    pub async fn next_crud_transaction<'a>(
        &'a self,
    ) -> Result<Option<CrudTransaction<'a>>, PowerSyncError> {
        let mut stream = self.crud_transactions();
        stream.try_next().await
    }

    pub fn status(&self) -> Arc<SyncStatusData> {
        self.inner.status.current_snapshot()
    }

    pub fn watch_status<'a>(&'a self) -> impl Stream<Item = Arc<SyncStatusData>> + 'a {
        self.inner.watch_status()
    }

    /// Creates a [SyncStream] based on name and optional parameters.
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

    pub async fn reader(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.reader().await
    }

    pub async fn writer(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.writer().await
    }

    #[cfg(feature = "ffi")]
    pub fn into_raw(self) -> *const InnerPowerSyncState {
        Arc::into_raw(self.inner)
    }

    #[cfg(feature = "ffi")]
    pub unsafe fn interpret_raw(inner: *const InnerPowerSyncState) -> Self {
        unsafe { Arc::increment_strong_count(inner) };
        Self {
            inner: unsafe { Arc::from_raw(inner) },
        }
    }

    #[cfg(feature = "ffi")]
    pub unsafe fn drop_raw(inner: *const InnerPowerSyncState) {
        drop(unsafe { Arc::from_raw(inner) });
    }
}
