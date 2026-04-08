#[cfg(feature = "rusqlite")]
use std::ops::{Deref, DerefMut};
use std::{collections::HashSet, mem::MaybeUninit, path::Path, sync::Arc};

use async_channel::{Receiver, Sender};
use async_lock::{Mutex, MutexGuardArc};
use powersync_sqlite_nostd::ResultCode;
use powersync_sqlite_nostd::bindings::{
    SQLITE_OPEN_CREATE, SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE,
};
use serde::Deserialize;

use crate::db::connection::{RawSqliteConnection, SqliteConnection};
use crate::{db::watch::TableNotifiers, error::PowerSyncError};

/// A raw connection pool, giving out both synchronous and asynchronous leases to SQLite
/// connections as well as managing update hooks.
#[derive(Clone)]
pub struct ConnectionPool {
    state: Arc<PoolState>,
}

impl ConnectionPool {
    fn prepare_writer(connection: SqliteConnection) -> Arc<Mutex<SqliteConnection>> {
        connection
            .exec(c"SELECT powersync_update_hooks('install');")
            .expect("could not install update hook");

        Arc::new(Mutex::new(connection))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, PowerSyncError> {
        let writer = SqliteConnection::from(RawSqliteConnection::open_path(
            &path,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
        )?);

        writer.exec(c"PRAGMA journal_mode = WAL")?;
        writer.exec(c"PRAGMA journal_size_limit = 6291456")?; // 6 * 1024 * 1024
        writer.exec(c"PRAGMA busy_timeout = 30000")?;
        writer.exec(c"PRAGMA cache_size = -51200")?; // -(50 * 1024)

        let mut readers = vec![];
        for _ in 0..5 {
            let reader = SqliteConnection::from(RawSqliteConnection::open_path(
                &path,
                SQLITE_OPEN_READONLY,
            )?);
            reader.exec(c"PRAGMA query_only = 1")?;
            readers.push(reader);
        }

        Ok(Self::wrap_connections(writer, readers))
    }

    /// Creates a pool backed by a single write and multiple reader connections.
    ///
    /// Connections will be configured to use WAL mode.
    pub fn wrap_connections(
        writer: impl Into<SqliteConnection>,
        readers: impl IntoIterator<Item = impl Into<SqliteConnection>>,
    ) -> Self {
        let writer = Self::prepare_writer(writer.into());
        let (release, consume) = async_channel::unbounded::<SqliteConnection>();
        for reader in readers {
            release.send_blocking(reader.into()).unwrap();
        }

        Self {
            state: Arc::new(PoolState {
                writer,
                readers: Some(PoolReaders {
                    take_reader: consume,
                    release_reader: release,
                }),
                table_notifiers: Default::default(),
            }),
        }
    }

    /// Creates a connection pool backed by a single sqlite connection.
    pub fn single_connection(conn: impl Into<SqliteConnection>) -> Self {
        Self {
            state: Arc::new(PoolState {
                writer: Self::prepare_writer(conn.into()),
                readers: None,
                table_notifiers: Default::default(),
            }),
        }
    }

    pub fn update_notifiers(&self) -> &Arc<TableNotifiers> {
        &self.state.table_notifiers
    }

    fn take_connection_sync(&'_ self, writer: bool) -> LeasedConnection {
        if !writer && let Some(readers) = &self.state.readers {
            let reader = readers
                .take_reader
                .recv_blocking()
                .expect("should receive connection");

            LeasedConnection {
                inner: OwnedConnectionLease::Reader {
                    connection: MaybeUninit::new(reader),
                    pool: self.clone(),
                },
            }
        } else {
            let guard = self.state.writer.lock_arc_blocking();
            LeasedConnection {
                inner: OwnedConnectionLease::Writer {
                    connection: guard,
                    pool: self.clone(),
                },
            }
        }
    }

    fn take_update_notifications(
        &self,
        writer: &SqliteConnection,
    ) -> Result<SqliteUpdateNotification, PowerSyncError> {
        let stmt = writer.prepare("SELECT powersync_update_hooks('get');")?;

        match stmt.step()? {
            ResultCode::ROW => {
                let updates =
                    serde_json::from_str::<SqliteUpdateNotification>(stmt.column_text(0)?)?;

                if !updates.tables.is_empty() {
                    self.state.table_notifiers.notify_updates(&updates.tables);
                }

                Ok(updates)
            }
            code => Err(code.into()),
        }
    }

    async fn take_connection_async(&self, writer: bool) -> LeasedConnection {
        if !writer && let Some(readers) = &self.state.readers {
            let reader = readers
                .take_reader
                .recv()
                .await
                .expect("should receive connection");

            LeasedConnection {
                inner: OwnedConnectionLease::Reader {
                    connection: MaybeUninit::new(reader),
                    pool: self.clone(),
                },
            }
        } else {
            let guard = self.state.writer.lock_arc().await;
            LeasedConnection {
                inner: OwnedConnectionLease::Writer {
                    connection: guard,
                    pool: self.clone(),
                },
            }
        }
    }

    pub async fn writer(&self) -> LeasedConnection {
        self.take_connection_async(true).await
    }

    pub fn writer_sync(&self) -> LeasedConnection {
        self.take_connection_sync(true)
    }

    pub async fn reader(&self) -> LeasedConnection {
        self.take_connection_async(false).await
    }

    pub fn reader_sync(&self) -> LeasedConnection {
        self.take_connection_sync(false)
    }
}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct SqliteUpdateNotification {
    tables: Arc<HashSet<String>>,
}

struct PoolState {
    writer: Arc<Mutex<SqliteConnection>>,
    readers: Option<PoolReaders>,
    table_notifiers: Arc<TableNotifiers>,
}

struct PoolReaders {
    take_reader: Receiver<SqliteConnection>,
    release_reader: Sender<SqliteConnection>,
}

enum OwnedConnectionLease {
    Writer {
        connection: MutexGuardArc<SqliteConnection>,
        pool: ConnectionPool,
    },
    Reader {
        connection: MaybeUninit<SqliteConnection>,
        pool: ConnectionPool,
    },
}

impl Drop for OwnedConnectionLease {
    fn drop(&mut self) {
        match self {
            OwnedConnectionLease::Writer { connection, pool } => {
                // Send update notifications for writes made on this connection while leased.
                let _ = pool.take_update_notifications(connection);
            }
            OwnedConnectionLease::Reader { connection, pool } => {
                let connection = std::mem::replace(connection, MaybeUninit::uninit());
                let connection = unsafe {
                    // safety: Only dropped here
                    connection.assume_init()
                };

                pool.state
                    .readers
                    .as_ref()
                    .unwrap()
                    .release_reader
                    .send_blocking(connection)
                    .expect("should send connection into pool");
            }
        }
    }
}

/// A temporary lease of a connection taken from the [ConnectionPool].
///
/// The connection is released into the pool when dropped.
pub struct LeasedConnection {
    inner: OwnedConnectionLease,
}

impl LeasedConnection {
    pub(crate) fn sqlite_connection(&self) -> &SqliteConnection {
        match &self.inner {
            OwnedConnectionLease::Writer { connection, .. } => connection,
            OwnedConnectionLease::Reader { connection, .. } => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                connection.assume_init_ref()
            },
        }
    }

    pub(crate) fn sqlite_connection_mut(&mut self) -> &mut SqliteConnection {
        match &mut self.inner {
            OwnedConnectionLease::Writer { connection, .. } => connection,
            OwnedConnectionLease::Reader { connection, .. } => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                connection.assume_init_mut()
            },
        }
    }
}

#[cfg(feature = "rusqlite")]
impl Deref for LeasedConnection {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        self.sqlite_connection().rusqlite_connection()
    }
}

#[cfg(feature = "rusqlite")]
impl DerefMut for LeasedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sqlite_connection_mut().rusqlite_connection_mut()
    }
}
