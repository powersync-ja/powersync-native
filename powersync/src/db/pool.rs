use std::{
    collections::HashSet,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use async_lock::{Mutex, MutexGuardArc};
use rusqlite::{Connection, Error, params};
use serde::Deserialize;

use crate::{db::watch::TableNotifiers, error::PowerSyncError};

/// A raw connection pool, giving out both synchronous and asynchronous leases to SQLite
/// connections as well as managing update hooks.
#[derive(Clone)]
pub struct ConnectionPool {
    state: Arc<PoolState>,
}

impl ConnectionPool {
    fn prepare_writer(connection: Connection) -> Arc<Mutex<Connection>> {
        connection
            .prepare("SELECT powersync_update_hooks('install');")
            .expect("should prepare statement for update hooks")
            .query_one(params![], |_| Ok(()))
            .expect("could not install update hook");

        Arc::new(Mutex::new(connection))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, PowerSyncError> {
        let writer = Connection::open(&path)?;

        writer.pragma_update(None, "journal_mode", "WAL")?;
        writer.pragma_update(None, "journal_size_limit", 6 * 1024 * 1024)?;
        writer.pragma_update(None, "busy_timeout", 30_000)?;
        writer.pragma_update(None, "cache_size", 50 * 1024)?;

        let mut readers = vec![];
        for _ in 0..5 {
            let reader = Connection::open(&path)?;
            reader.pragma_update(None, "query_only", true)?;
            readers.push(reader);
        }

        Ok(Self::wrap_connections(writer, readers))
    }

    /// Creates a pool backed by a single write ad multiple reader connections.
    ///
    /// Connections will be configured to use WAL mode.
    pub fn wrap_connections(
        writer: Connection,
        readers: impl IntoIterator<Item = Connection>,
    ) -> Self {
        let writer = Self::prepare_writer(writer);
        let (release, consume) = async_channel::unbounded::<Connection>();
        for reader in readers {
            release.send_blocking(reader).unwrap();
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
    pub fn single_connection(conn: Connection) -> Self {
        Self {
            state: Arc::new(PoolState {
                writer: Self::prepare_writer(conn),
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
        writer: &Connection,
    ) -> Result<SqliteUpdateNotification, Error> {
        let mut stmt = writer.prepare_cached("SELECT powersync_update_hooks('get');")?;
        let rows: String = stmt.query_one(params![], |row| row.get(0))?;

        let updates = serde_json::from_str::<SqliteUpdateNotification>(&rows)
            .map_err(|_| Error::InvalidQuery)?;

        if !updates.tables.is_empty() {
            self.state.table_notifiers.notify_updates(&updates.tables);
        }

        Ok(updates)
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
    writer: Arc<Mutex<Connection>>,
    readers: Option<PoolReaders>,
    table_notifiers: Arc<TableNotifiers>,
}

struct PoolReaders {
    take_reader: Receiver<Connection>,
    release_reader: Sender<Connection>,
}

enum OwnedConnectionLease {
    Writer {
        connection: MutexGuardArc<Connection>,
        pool: ConnectionPool,
    },
    Reader {
        connection: MaybeUninit<Connection>,
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

impl Deref for LeasedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            OwnedConnectionLease::Writer { connection, .. } => connection,
            OwnedConnectionLease::Reader { connection, .. } => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                connection.assume_init_ref()
            },
        }
    }
}

impl DerefMut for LeasedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            OwnedConnectionLease::Writer { connection, .. } => connection,
            OwnedConnectionLease::Reader { connection, .. } => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                connection.assume_init_mut()
            },
        }
    }
}
