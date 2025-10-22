use crate::connector::{CppConnector, wrap_cpp_connector};
use crate::crud::StringView;
use crate::error::{LAST_ERROR, PowerSyncResultCode};
use crate::schema::RawSchema;
use futures_lite::future;
use http_client::isahc::IsahcClient;
use powersync::env::PowerSyncEnvironment;
use powersync::error::PowerSyncError;
use powersync::ffi::RawPowerSyncDatabase;
use powersync::schema::Schema;
use powersync::{CallbackListenerHandle, ConnectionPool, LeasedConnection, PowerSyncDatabase};
use rusqlite::Connection;
use rusqlite::ffi::sqlite3;
use std::collections::HashSet;
use std::ffi::{CString, c_char, c_void};
use std::ops::Deref;
use std::ptr::null;
use std::sync::Arc;

fn create_db(schema: Schema, pool: ConnectionPool) -> PowerSyncDatabase {
    let env = PowerSyncEnvironment::custom(
        Arc::new(IsahcClient::new()),
        pool,
        Box::new(PowerSyncEnvironment::async_io_timer()),
    );

    PowerSyncDatabase::new(env, schema)
}

struct RawConnectionLease<'a> {
    lease: Box<dyn LeasedConnection + 'a>,
}

#[repr(C)]
struct ConnectionLeaseResult<'a> {
    sqlite3: *mut sqlite3,
    lease: *mut RawConnectionLease<'a>,
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_in_memory(
    schema: RawSchema,
    out_db: &mut RawPowerSyncDatabase,
) -> PowerSyncResultCode {
    ps_try!(PowerSyncEnvironment::powersync_auto_extension());
    let conn = ps_try!(Connection::open_in_memory().map_err(PowerSyncError::from));
    *out_db = create_db(
        schema.copy_to_rust(),
        ConnectionPool::single_connection(conn),
    )
    .into();

    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_connect(
    db: &RawPowerSyncDatabase,
    connector: *const CppConnector,
) -> PowerSyncResultCode {
    ps_try!(future::block_on(
        db.connect(unsafe { wrap_cpp_connector(connector) })
    ));
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_disconnect(db: &RawPowerSyncDatabase) -> PowerSyncResultCode {
    ps_try!(future::block_on(db.disconnect()));
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_reader<'a>(
    db: &'a RawPowerSyncDatabase,
    out_lease: &mut ConnectionLeaseResult<'a>,
) -> PowerSyncResultCode {
    let reader = ps_try!(future::block_on(db.lease_reader()));

    out_lease.sqlite3 = unsafe { reader.deref().handle() };
    out_lease.lease = Box::into_raw(Box::new(RawConnectionLease {
        lease: Box::new(reader),
    }));
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_writer<'a>(
    db: &'a RawPowerSyncDatabase,
    out_lease: &mut ConnectionLeaseResult<'a>,
) -> PowerSyncResultCode {
    let writer = ps_try!(future::block_on(db.lease_writer()));

    out_lease.sqlite3 = unsafe { writer.deref().handle() };
    out_lease.lease = Box::into_raw(Box::new(RawConnectionLease {
        lease: Box::new(writer),
    }));
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_return_lease(lease: *mut RawConnectionLease) {
    let lease = unsafe { Box::from_raw(lease) };
    drop(lease);
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_watch_tables<'a>(
    db: &'a RawPowerSyncDatabase,
    tables: *const StringView,
    table_count: usize,
    listener: extern "C" fn(*const c_void),
    token: *const c_void,
) -> *mut c_void {
    let resolved_tables = {
        let mut resolved_tables = HashSet::new();
        let table_names = unsafe { std::slice::from_raw_parts(tables, table_count) };

        for table in table_names {
            let name: &str = table.as_ref();
            resolved_tables.insert(name.to_string());
            resolved_tables.insert(format!("ps_data__{name}"));
            resolved_tables.insert(format!("ps_data_local__{name}"));
        }

        resolved_tables
    };

    #[derive(Clone)]
    struct PendingListener {
        listener: extern "C" fn(*const c_void),
        token: *const c_void,
    }

    // Safety: We require listeners to be thread-safe in C++.
    unsafe impl Send for PendingListener {}
    unsafe impl Sync for PendingListener {}

    let listener = PendingListener { listener, token };
    let handle: CallbackListenerHandle<'a, HashSet<String>> =
        db.install_table_listener(resolved_tables, move || {
            let inner = &listener;
            (inner.listener)(inner.token);
        });

    Box::into_raw(Box::new(handle)) as *mut c_void
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_watch_tables_end(watcher: *mut c_void) {
    drop(unsafe { Box::from_raw(watcher as *mut CallbackListenerHandle<'_, HashSet<String>>) });
}

#[unsafe(no_mangle)]
extern "C" fn powersync_db_free(db: RawPowerSyncDatabase) {
    unsafe { db.free() }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_last_error_desc() -> *mut c_char {
    if let Some(err) = LAST_ERROR.take() {
        CString::into_raw(CString::new(format!("{}", err)).unwrap())
    } else {
        null()
    }
    .cast_mut()
}

#[unsafe(no_mangle)]
extern "C" fn powersync_free_str(str: *const c_char) {
    drop(unsafe { CString::from_raw(str.cast_mut()) });
}
