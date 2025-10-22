use powersync::ffi::RawPowerSyncDatabase;
use powersync::{CallbackListenerHandle, SyncStatusData};
use std::ffi::c_void;
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "C" fn powersync_db_status(db: &RawPowerSyncDatabase) -> *mut c_void {
    Arc::into_raw(db.sync_status()) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_status_free(status: *const c_void) {
    drop(unsafe { Arc::from_raw(status as *const SyncStatusData) })
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_db_status_listener<'a>(
    db: &'a RawPowerSyncDatabase,
    listener: extern "C" fn(*const c_void),
    token: *const c_void,
) -> *mut c_void {
    #[derive(Clone)]
    struct PendingListener {
        listener: extern "C" fn(*const c_void),
        token: *const c_void,
    }

    // Safety: We require listeners to be thread-safe in C++.
    unsafe impl Send for PendingListener {}
    unsafe impl Sync for PendingListener {}

    let listener = PendingListener { listener, token };
    let handle: CallbackListenerHandle<'a, ()> = db.install_status_listener(move || {
        let inner = &listener;
        (inner.listener)(inner.token);
    });

    Box::into_raw(Box::new(handle)) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_db_status_listener_clear(listener: *mut c_void) {
    drop(unsafe { Box::from_raw(listener as *mut CallbackListenerHandle<'_, ()>) });
}
