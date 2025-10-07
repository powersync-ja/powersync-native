use std::{
    collections::BTreeSet,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use async_channel::{Receiver, Sender};
use async_lock::{Mutex, MutexGuard};
use rusqlite::{Connection, Error, params};
use serde::Deserialize;

use crate::error::PowerSyncError;

/// A raw connection pool, giving out both synchronous and asynchronous leases to SQLite
/// connections as well as managing update hooks.
#[derive(Clone)]
pub struct ConnectionPool {
    state: Arc<PoolState>,
}

impl ConnectionPool {
    fn prepare_writer(connection: Connection) -> Mutex<Connection> {
        connection
            .prepare("SELECT powersync_update_hooks('install');")
            .expect("should prepare statement for update hooks")
            .query_one(params![], |_| Ok(()))
            .expect("could not install update hook");

        Mutex::new(connection)
    }

    pub fn updates(&self) -> async_broadcast::Receiver<SqliteUpdateNotification> {
        self.state.send_notifications.new_receiver()
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

    pub fn wrap_connections(
        writer: Connection,
        readers: impl IntoIterator<Item = Connection>,
    ) -> Self {
        let writer = Self::prepare_writer(writer);
        let (release, consume) = async_channel::unbounded::<Connection>();
        for reader in readers {
            release.send_blocking(reader).unwrap();
        }
        let (send_updates, _) = async_broadcast::broadcast(64);

        Self {
            state: Arc::new(PoolState {
                writer,
                readers: Some(PoolReaders {
                    take_reader: consume,
                    release_reader: release,
                }),
                send_notifications: send_updates,
            }),
        }
    }

    pub fn single_connection(conn: Connection) -> Self {
        let (send_updates, _) = async_broadcast::broadcast(64);

        Self {
            state: Arc::new(PoolState {
                writer: Self::prepare_writer(conn),
                readers: None,
                send_notifications: send_updates,
            }),
        }
    }

    fn take_connection_sync(&'_ self, writer: bool) -> LeasedConnectionImpl<'_> {
        if !writer && let Some(readers) = &self.state.readers {
            let reader = readers
                .take_reader
                .recv_blocking()
                .expect("should receive connection");

            LeasedConnectionImpl::Reader(LeasedReader {
                connection: MaybeUninit::new(reader),
                release: &readers.release_reader,
            })
        } else {
            let guard = self.state.writer.lock_blocking();
            LeasedConnectionImpl::Writer(LeasedWriter {
                connection: guard,
                pool: self,
            })
        }
    }

    fn take_update_notifications(&self, writer: &Connection) -> Result<(), Error> {
        let mut stmt = writer.prepare_cached("SELECT powersync_update_hooks('get');")?;
        let rows: String = stmt.query_one(params![], |row| row.get(0))?;

        let updates = serde_json::from_str::<SqliteUpdateNotification>(&rows)
            .map_err(|_| Error::InvalidQuery)?;

        if updates.tables.len() > 0 {
            let _ = self.state.send_notifications.broadcast_blocking(updates);
        }

        Ok(())
    }

    async fn take_connection_async(&'_ self, writer: bool) -> LeasedConnectionImpl<'_> {
        if !writer && let Some(readers) = &self.state.readers {
            let reader = readers
                .take_reader
                .recv()
                .await
                .expect("should receive connection");

            LeasedConnectionImpl::Reader(LeasedReader {
                connection: MaybeUninit::new(reader),
                release: &readers.release_reader,
            })
        } else {
            let guard = self.state.writer.lock().await;
            LeasedConnectionImpl::Writer(LeasedWriter {
                connection: guard,
                pool: self,
            })
        }
    }

    pub async fn writer(&self) -> impl LeasedConnection {
        return self.take_connection_async(true).await;
    }

    pub fn writer_sync(&self) -> impl LeasedConnection {
        return self.take_connection_sync(true);
    }

    pub async fn reader(&self) -> impl LeasedConnection {
        return self.take_connection_async(false).await;
    }

    pub fn reader_sync(&self) -> impl LeasedConnection {
        return self.take_connection_sync(false);
    }
}

pub trait LeasedConnection: DerefMut<Target = Connection> {}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct SqliteUpdateNotification {
    tables: Arc<BTreeSet<String>>,
}

struct PoolState {
    writer: Mutex<Connection>,
    readers: Option<PoolReaders>,
    send_notifications: async_broadcast::Sender<SqliteUpdateNotification>,
}

struct PoolReaders {
    take_reader: Receiver<Connection>,
    release_reader: Sender<Connection>,
}

struct LeasedWriter<'a> {
    connection: MutexGuard<'a, Connection>,
    pool: &'a ConnectionPool,
}

impl Drop for LeasedWriter<'_> {
    fn drop(&mut self) {
        let _ = self.pool.take_update_notifications(&self.connection);
    }
}

struct LeasedReader<'a> {
    connection: MaybeUninit<Connection>,
    release: &'a Sender<Connection>,
}

impl Drop for LeasedReader<'_> {
    fn drop(&mut self) {
        let connection = std::mem::replace(&mut self.connection, MaybeUninit::uninit());
        let connection = unsafe {
            // safety: Only dropped here
            connection.assume_init()
        };

        self.release
            .send_blocking(connection)
            .expect("should send connection into pool");
    }
}

enum LeasedConnectionImpl<'a> {
    Writer(LeasedWriter<'a>),
    Reader(LeasedReader<'a>),
}

impl<'a> Deref for LeasedConnectionImpl<'a> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        match self {
            LeasedConnectionImpl::Writer(writer) => &*writer.connection,
            LeasedConnectionImpl::Reader(reader) => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                reader.connection.assume_init_ref()
            },
        }
    }
}

impl<'a> DerefMut for LeasedConnectionImpl<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            LeasedConnectionImpl::Writer(writer) => &mut *writer.connection,
            LeasedConnectionImpl::Reader(reader) => unsafe {
                // safety: This is initialized by default, and only uninitialized on Drop.
                reader.connection.assume_init_mut()
            },
        }
    }
}

impl<'a> LeasedConnection for LeasedConnectionImpl<'a> {}
