use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

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
use futures_lite::stream::{once, once_future};
use futures_lite::{FutureExt, Stream, StreamExt};
use rusqlite::{Params, Statement, params};

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
    sync: Arc<SyncCoordinator>,
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
    const PS_DATA_PREFIX: &'static str = "ps_data__";
    const PS_DATA_LOCAL_PREFIX: &'static str = "ps_data_local__";

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
    pub fn watch_tables<'a, Tables: IntoIterator<Item = impl Into<Cow<'a, str>>>>(
        &self,
        emit_initially: bool,
        tables: Tables,
    ) -> impl Stream<Item = ()> + 'static {
        self.inner.env.pool.update_notifiers().listen(
            emit_initially,
            tables
                .into_iter()
                .flat_map(|s| {
                    let s = s.into();

                    [
                        format!("{}{s}", Self::PS_DATA_PREFIX),
                        format!("{}{s}", Self::PS_DATA_LOCAL_PREFIX),
                        Cow::into_owned(s),
                    ]
                })
                .collect(),
        )
    }

    /// Returns an asynchronous [Stream] emitting snapshots of a `SELECT` statement every time
    /// source tables are modified.
    ///
    /// `sql` is the `SELECT` statement to execute and `params` are parameters to use.
    /// The `read` function obtains the raw prepared statement and a copy of parameters to use,
    /// and can run the statements into desired results.
    ///
    /// This method is a core building block for reactive applications with PowerSync - since it
    /// updates automatically, all writes (regardless of whether they're local or due to synced
    /// writes from your backend) are reflected.
    pub fn watch_statement<T, F, P: Params + Clone + 'static>(
        &self,
        sql: String,
        params: P,
        read: F,
    ) -> impl Stream<Item = Result<T, PowerSyncError>> + 'static
    where
        for<'a> F: (Fn(&'a mut Statement, P) -> Result<T, PowerSyncError>) + 'static + Clone,
    {
        // Find and watch referenced tables. We assume the set of read tables is fixed for a given
        // SQL query and parameters. We also want this to emit initially without an update so that
        // this stream can emit the initial snapshot.
        let update_notifications =
            self.emit_on_statement_changes(true, sql.to_string(), params.clone());

        let db = self.clone();
        // Note that this does not necessarily re-run for every update notification: If multiple
        // updates happened while `then` or a downstream consumer is busy, they will be batched and
        // emitted as a single notification. This matches the backpressure behavior of our other
        // SDKs.
        update_notifications.then(move |notification| {
            let db = db.clone();
            let sql = sql.clone();
            let params = params.clone();
            let mapper = read.clone();

            async move {
                if let Err(e) = notification {
                    return Err(e);
                }

                let reader = db.reader().await?;
                let mut stmt = reader.prepare_cached(&sql)?;

                mapper(&mut stmt, params)
            }
        })
    }

    fn emit_on_statement_changes(
        &self,
        emit_initially: bool,
        sql: String,
        params: impl Params + 'static,
    ) -> impl Stream<Item = Result<(), PowerSyncError>> + 'static {
        // Stream emitting referenced tables once.
        let tables = once_future(self.clone().find_tables(sql, params));

        // Stream emitting updates, or a single error if we couldn't resolve tables.
        let db = self.clone();
        tables.flat_map(move |referenced_tables| match referenced_tables {
            Ok(referenced_tables) => db
                .watch_tables(emit_initially, referenced_tables)
                .map(Ok)
                .boxed(),
            Err(e) => once(Err(e)).boxed(),
        })
    }

    /// Finds all tables that are used in a given select statement.
    ///
    /// This can be used together with [watch_tables] to build an auto-updating stream of queries.
    async fn find_tables<P: Params>(
        self,
        sql: impl Into<Cow<'static, str>>,
        params: P,
    ) -> Result<Vec<String>, PowerSyncError> {
        let reader = self.reader().await?;
        let mut stmt = reader.prepare(&format!("EXPLAIN {}", sql.into()))?;
        let mut rows = stmt.query(params)?;

        let mut find_table_stmt =
            reader.prepare_cached("SELECT tbl_name FROM sqlite_schema WHERE rootpage = ?")?;
        let mut found_tables = HashSet::new();

        while let Some(row) = rows.next()? {
            let opcode = row.get_ref("opcode")?;
            let p2 = row.get_ref("p2")?;
            let p3 = row.get_ref("p3")?;

            if matches!(opcode.as_str(), Ok("OpenRead"))
                && matches!(p3.as_i64(), Ok(0))
                && let Ok(page) = p2.as_i64()
            {
                let mut found_table = find_table_stmt.query(params![page])?;
                if let Some(found_table) = found_table.next()? {
                    let table_name: String = found_table.get(0)?;
                    found_tables.insert(table_name);
                }
            }
        }

        Ok(found_tables
            .into_iter()
            .map(|table| {
                if let Some(name) = table.strip_prefix(Self::PS_DATA_PREFIX) {
                    name.to_string()
                } else if let Some(name) = table.strip_prefix(Self::PS_DATA_LOCAL_PREFIX) {
                    name.to_string()
                } else {
                    table
                }
            })
            .collect())
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

    /*
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
     */
}

impl Debug for PowerSyncDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowerSyncDatabase").finish_non_exhaustive()
    }
}
