use powersync::SyncStatusData;
use powersync::ffi::RawPowerSyncDatabase;
use std::ffi::c_void;
use std::sync::Arc;

#[unsafe(no_mangle)]
pub fn powersync_db_status(db: &RawPowerSyncDatabase) -> *const c_void {
    Arc::into_raw(db.sync_status()) as *const c_void
}

#[unsafe(no_mangle)]
pub fn powersync_status_free(status: *const c_void) {
    drop(unsafe { Arc::from_raw(status as *const SyncStatusData) })
}
