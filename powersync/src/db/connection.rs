use crate::error::{PowerSyncError, RawPowerSyncError};
use num_traits::cast::FromPrimitive;
use powersync_sqlite_nostd::bindings::sqlite3_open_v2;
use powersync_sqlite_nostd::{
    Connection, Destructor, ManagedConnection, ManagedStmt, ResultCode, sqlite3,
};
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
            // Safety: We're not doing anything that could close the connection.
            self.handle().exec(stmt)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionMode {
    Read,
    Write,
}

/// A prepared statement whose lifetime is confined to a scoped transaction.
///
/// The raw `ManagedStmt` is deliberately private so a statement or row borrow
/// cannot escape after the transaction and pool lease have ended.
pub struct TransactionStatement {
    inner: ManagedStmt,
}

impl TransactionStatement {
    pub fn step(&mut self) -> Result<ResultCode, PowerSyncError> {
        self.inner.step().map_err(PowerSyncError::from)
    }

    pub fn execute(&mut self) -> Result<(), PowerSyncError> {
        while let ResultCode::ROW = self.step()? {}
        Ok(())
    }

    pub fn bind_null(&mut self, index: i32) -> Result<(), PowerSyncError> {
        self.inner.bind_null(index)?;
        Ok(())
    }

    pub fn bind_text(&mut self, index: i32, value: &str) -> Result<(), PowerSyncError> {
        self.inner.bind_text(index, value, Destructor::TRANSIENT)?;
        Ok(())
    }

    pub fn bind_blob(&mut self, index: i32, value: &[u8]) -> Result<(), PowerSyncError> {
        self.inner.bind_blob(index, value, Destructor::TRANSIENT)?;
        Ok(())
    }

    pub fn bind_int64(&mut self, index: i32, value: i64) -> Result<(), PowerSyncError> {
        self.inner.bind_int64(index, value)?;
        Ok(())
    }

    pub fn bind_int(&mut self, index: i32, value: i32) -> Result<(), PowerSyncError> {
        self.inner.bind_int(index, value)?;
        Ok(())
    }

    pub fn bind_double(&mut self, index: i32, value: f64) -> Result<(), PowerSyncError> {
        self.inner.bind_double(index, value)?;
        Ok(())
    }

    pub fn column_text(&self, index: i32) -> Result<&str, PowerSyncError> {
        self.inner.column_text(index).map_err(PowerSyncError::from)
    }

    pub fn column_blob(&self, index: i32) -> Result<&[u8], PowerSyncError> {
        self.inner.column_blob(index).map_err(PowerSyncError::from)
    }

    pub fn column_int64(&self, index: i32) -> i64 {
        self.inner.column_int64(index)
    }

    pub fn column_int(&self, index: i32) -> i32 {
        self.inner.column_int(index)
    }

    pub fn column_double(&self, index: i32) -> f64 {
        self.inner.column_double(index)
    }
}

/// A transaction whose commit and rollback lifecycle is owned by the SDK.
pub struct TransactionGuard<'a> {
    pub(crate) inner: &'a mut SqliteConnection,
    active: bool,
}

impl<'a> TransactionGuard<'a> {
    /// Compatibility constructor for existing SDK internals. New public
    /// callers should use `PowerSyncDatabase::{read,write}_transaction`.
    pub(crate) fn new(connection: &'a mut SqliteConnection) -> Result<Self, PowerSyncError> {
        Self::with_mode(connection, TransactionMode::Read)
    }

    pub(crate) fn with_mode(
        connection: &'a mut SqliteConnection,
        mode: TransactionMode,
    ) -> Result<Self, PowerSyncError> {
        if !unsafe { connection.handle().get_autocommit() } {
            return Err(PowerSyncError::argument_error(
                "Connection already in transaction",
            ));
        }

        connection.exec(match mode {
            TransactionMode::Read => c"BEGIN",
            TransactionMode::Write => c"BEGIN IMMEDIATE",
        })?;
        Ok(TransactionGuard {
            inner: connection,
            active: true,
        })
    }

    /// Prepare and use one statement without allowing it to escape the callback.
    ///
    /// ```compile_fail
    /// use powersync::{PowerSyncStatement, PowerSyncTransaction};
    /// use powersync::error::PowerSyncError;
    ///
    /// fn escape<'a>(
    ///     transaction: &'a mut PowerSyncTransaction<'a>,
    /// ) -> Result<&'a mut PowerSyncStatement, PowerSyncError> {
    ///     transaction.with_statement("SELECT 1", |statement| Ok(statement))
    /// }
    /// ```
    pub fn with_statement<T>(
        &mut self,
        sql: &str,
        operation: impl for<'statement> FnOnce(
            &'statement mut TransactionStatement,
        ) -> Result<T, PowerSyncError>,
    ) -> Result<T, PowerSyncError> {
        reject_transaction_control(sql)?;
        let statement = self.inner.prepare(sql)?;
        let mut statement = TransactionStatement { inner: statement };
        operation(&mut statement)
    }

    /// Execute one statement inside this transaction.
    pub fn execute(&mut self, sql: &str) -> Result<(), PowerSyncError> {
        self.with_statement(sql, TransactionStatement::execute)
    }

    /// Compatibility exit for existing SDK internals.
    pub(crate) fn commit(mut self) -> Result<(), PowerSyncError> {
        self.commit_in_place()
    }

    fn commit_in_place(&mut self) -> Result<(), PowerSyncError> {
        if !self.active {
            return Ok(());
        }
        self.inner.exec(c"COMMIT")?;
        self.active = false;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), PowerSyncError> {
        if !self.active {
            return Ok(());
        }
        self.inner.exec(c"ROLLBACK")?;
        self.active = false;
        Ok(())
    }
}

impl Drop for TransactionGuard<'_> {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = self.rollback()
        {
            log::error!("Could not roll back scoped PowerSync transaction: {error}");
        }
    }
}

fn reject_transaction_control(sql: &str) -> Result<(), PowerSyncError> {
    let Some(keyword) = first_sql_keyword(sql) else {
        return Err(PowerSyncError::argument_error(
            "Scoped transaction statement is empty",
        ));
    };

    if matches!(
        keyword.as_str(),
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
    ) {
        return Err(PowerSyncError::argument_error(
            "Transaction control is owned by the scoped transaction API",
        ));
    }

    Ok(())
}

fn first_sql_keyword(mut sql: &str) -> Option<String> {
    loop {
        sql = sql.trim_start_matches(|character: char| {
            character.is_ascii_whitespace() || character == '\u{feff}'
        });
        if let Some(rest) = sql.strip_prefix("--") {
            sql = rest.split_once('\n').map_or("", |(_, remaining)| remaining);
            continue;
        }
        if let Some(rest) = sql.strip_prefix("/*") {
            sql = rest.split_once("*/").map_or("", |(_, remaining)| remaining);
            continue;
        }
        break;
    }

    let keyword = sql
        .bytes()
        .take_while(u8::is_ascii_alphabetic)
        .collect::<Vec<_>>();
    (!keyword.is_empty()).then(|| String::from_utf8_lossy(&keyword).to_ascii_uppercase())
}

pub(crate) fn run_transaction<T>(
    connection: &mut SqliteConnection,
    mode: TransactionMode,
    operation: impl FnOnce(&mut TransactionGuard<'_>) -> Result<T, PowerSyncError>,
) -> Result<T, PowerSyncError> {
    let mut transaction = TransactionGuard::with_mode(connection, mode)?;
    match operation(&mut transaction) {
        Ok(value) => {
            transaction.commit_in_place()?;
            Ok(value)
        }
        Err(original) => {
            if let Err(rollback_error) = transaction.rollback() {
                log::error!(
                    "Could not roll back scoped PowerSync transaction after error: \
                     {rollback_error}"
                );
            }
            Err(original)
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

#[cfg(all(test, feature = "rusqlite"))]
#[path = "connection_tests.rs"]
mod tests;

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
