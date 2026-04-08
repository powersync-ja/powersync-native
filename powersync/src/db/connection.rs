use crate::error::{PowerSyncError, RawPowerSyncError};
use num_traits::cast::FromPrimitive;
use powersync_sqlite_nostd::bindings::sqlite3_open_v2;
use powersync_sqlite_nostd::{Connection, ManagedConnection, ManagedStmt, ResultCode, sqlite3};
use std::ffi::{CStr, CString, c_int};
use std::mem::MaybeUninit;
use std::path::Path;
use std::ptr::null;

/// The SQLite connection used by the PowerSync Rust SDK.
///
/// When the `rusqlite` feature is enabled, we use rusqlite connections.
/// Without that feature, we use raw `*mut sqlite3` pointers. Disabling that
/// feature can be useful when a custom SQLite build (e.g. `sqlite3mc`) needs
/// to be used with the SDK.
pub struct SqliteConnection {
    #[cfg(not(feature = "rusqlite"))]
    raw: RawSqliteConnection,
    #[cfg(feature = "rusqlite")]
    inner: rusqlite::Connection,
}

impl SqliteConnection {
    /// Returns the `*mut sqlite3` pointer from the inner connection.
    ///
    /// This method is unsafe since the pointer could be used to transform the connection
    /// into an unexpected state.
    #[cfg(feature = "rusqlite")]
    pub unsafe fn handle(&self) -> *mut sqlite3 {
        unsafe { self.inner.handle() }.cast()
    }

    #[cfg(not(feature = "rusqlite"))]
    pub unsafe fn handle(&self) -> *mut sqlite3 {
        self.raw.0.db
    }

    #[cfg(feature = "rusqlite")]
    pub fn rusqlite_connection(&self) -> &rusqlite::Connection {
        &self.inner
    }

    #[cfg(feature = "rusqlite")]
    pub fn rusqlite_connection_mut(&mut self) -> &mut rusqlite::Connection {
        &mut self.inner
    }

    /// Executes a SQL statement without parameters.
    pub fn exec(&self, stmt: &CStr) -> Result<(), PowerSyncError> {
        unsafe {
            // Safety: We know the stmt is null-terminated.
            self.handle().exec(stmt.as_ptr())
        }
        .map_err(|rc| RawPowerSyncError::RawSqlite {
            code: rc,
            context: format!("Could not run {}", stmt.to_string_lossy()),
        })?;

        Ok(())
    }

    pub fn prepare(&self, stmt: &str) -> Result<ManagedStmt, PowerSyncError> {
        unsafe {
            // Safety: We're not doing anything that could close the connection.
            self.handle()
        }
        .prepare_v2(stmt)
        .map_err(|rc| {
            RawPowerSyncError::RawSqlite {
                code: rc,
                context: format!("Could not prepare {stmt}"),
            }
            .into()
        })
    }
}

/// Utility for running a block in a transaction.
pub struct TransactionGuard<'a> {
    pub inner: &'a mut SqliteConnection,
    active: bool,
}

impl<'a> TransactionGuard<'a> {
    pub fn new(connection: &'a mut SqliteConnection) -> Result<Self, PowerSyncError> {
        if !unsafe { connection.handle().get_autocommit() } {
            return Err(PowerSyncError::argument_error(
                "Connection already in transaction",
            ));
        }

        connection.exec(c"BEGIN")?;
        Ok(TransactionGuard {
            inner: connection,
            active: true,
        })
    }

    pub fn commit(mut self) -> Result<(), PowerSyncError> {
        self.active = false;
        self.inner.exec(c"COMMIT")
    }

    fn rollback_internal(&mut self) -> Result<(), PowerSyncError> {
        self.inner.exec(c"ROLLBACK")
    }
}

impl Drop for TransactionGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            // Rollback if the transaction hasn't explicitly been committed.
            let _ = self.rollback_internal();
        }
    }
}

#[cfg(feature = "rusqlite")]
impl From<rusqlite::Connection> for SqliteConnection {
    fn from(value: rusqlite::Connection) -> Self {
        Self { inner: value }
    }
}

#[cfg(not(feature = "rusqlite"))]
impl From<RawSqliteConnection> for SqliteConnection {
    fn from(value: RawSqliteConnection) -> Self {
        Self { raw: value }
    }
}

#[cfg(feature = "rusqlite")]
impl From<RawSqliteConnection> for SqliteConnection {
    fn from(value: RawSqliteConnection) -> Self {
        let conn = value.0.db;

        // Don't call sqlite3_close_v2, we want to transfer ownership.
        let _ = std::mem::ManuallyDrop::new(value.0);

        Self {
            inner: unsafe {
                // Safety: The never dropped ManuallyDrop transfers ownership from the
                // RawSqliteConnection to rusqlite.
                rusqlite::Connection::from_handle_owned(conn.cast())
            }
            .unwrap(),
        }
    }
}

pub struct RawSqliteConnection(ManagedConnection);

unsafe impl Send for RawSqliteConnection {}

impl RawSqliteConnection {
    pub fn open(path: &CStr, flags: u32) -> Result<Self, PowerSyncError> {
        let mut db = MaybeUninit::<*mut sqlite3>::uninit();
        let rc = ResultCode::from_i32(unsafe {
            sqlite3_open_v2(path.as_ptr(), db.as_mut_ptr(), flags as c_int, null())
        })
        .unwrap();

        if rc == ResultCode::OK {
            Ok(Self(ManagedConnection {
                db: unsafe {
                    // sqlite3_open_v2 returned 0, so SQLite will have written the pointer.
                    db.assume_init()
                },
            }))
        } else {
            Err(RawPowerSyncError::RawSqlite {
                code: rc,
                context: format!("Could not open database {}", path.to_string_lossy()),
            }
            .into())
        }
    }

    pub fn open_path<P: AsRef<Path>>(path: P, flags: u32) -> Result<Self, PowerSyncError> {
        Self::open(path_to_cstring(path.as_ref())?.as_ref(), flags)
    }
}

pub fn exec_stmt(stmt: ManagedStmt) -> Result<(), PowerSyncError> {
    while let ResultCode::ROW = stmt.step().map_err(|e| RawPowerSyncError::RawSqlite {
        code: e,
        context: format!("Stepping through {}", stmt.sql().unwrap_or("unknown SQL")),
    })? {
        // Keep stepping through statement.
    }

    Ok(())
}

#[cfg(unix)]
fn path_to_cstring(p: &Path) -> Result<CString, PowerSyncError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(
        CString::new(p.as_os_str().as_bytes()).map_err(|_| RawPowerSyncError::ArgumentError {
            desc: format!("Invalid path: {p:?}").into(),
        })?,
    )
}

#[cfg(not(unix))]
fn path_to_cstring(p: &Path) -> Result<CString, PowerSyncError> {
    let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
    Ok(
        CString::new(s).map_err(|_| RawPowerSyncError::ArgumentError {
            desc: format!("Invalid path: {p:?}").into(),
        })?,
    )
}
