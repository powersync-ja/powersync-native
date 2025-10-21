use crate::connector::{CppConnector, wrap_cpp_connector};
use crate::error::{LAST_ERROR, PowerSyncResultCode};
use crate::schema::RawSchema;
use futures_lite::future;
use http_client::isahc::IsahcClient;
use powersync::env::PowerSyncEnvironment;
use powersync::error::PowerSyncError;
use powersync::ffi::RawPowerSyncDatabase;
use powersync::schema::Schema;
use powersync::{ConnectionPool, LeasedConnection, PowerSyncDatabase};
use rusqlite::Connection;
use rusqlite::ffi::sqlite3;
use std::ffi::{CString, c_char};
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
