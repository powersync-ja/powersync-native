use crate::crud::StringView;
use powersync::ffi::RawPowerSyncDatabase;
use powersync::{CallbackListenerHandle, SyncStatusData};
use std::ffi::c_void;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

#[repr(C)]
pub struct RawSyncStatus {
    pub connected: bool,
    pub connecting: bool,
    pub downloading: bool,
    pub download_error: StringView,
    pub has_download_error: bool,
    pub uploading: bool,
    pub upload_error: StringView,
    pub has_upload_error: bool,
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_db_status(db: &RawPowerSyncDatabase) -> *mut c_void {
    Arc::into_raw(db.sync_status()) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_status_inspect(
    status: *const c_void,
    inspect: extern "C" fn(token: *mut c_void, status: RawSyncStatus),
    inspect_token: *mut c_void,
) {
    let status = unsafe { &*(status as *const SyncStatusData) };
    let download_error = status.download_error().map(|e| e.to_string());
    let upload_error = status.upload_error().map(|e| e.to_string());

    inspect(
        inspect_token,
        RawSyncStatus {
            connected: status.is_connected(),
            connecting: status.is_connecting(),
            downloading: status.is_downloading(),
            download_error: StringView::view_optional(download_error.as_deref()),
            has_download_error: status.download_error().is_some(),
            uploading: status.is_uploading(),
            upload_error: StringView::view_optional(upload_error.as_deref()),
            has_upload_error: status.upload_error().is_some(),
        },
    );
}

#[repr(C)]
pub struct RawSyncStreamStatus {
    pub name: StringView,
    pub parameters: StringView,
    pub has_parameters: bool,
    pub progress_total: i64,
    pub progress_done: i64,
    pub has_progress: bool,
    pub is_active: bool,
    pub is_default: bool,
    pub has_explicit_subscription: bool,
    pub expires_at: i64,
    pub has_expires_at: bool,
    pub has_synced: bool,
    pub last_synced_at: i64,
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_status_streams(
    status: *const c_void,
    inspect: extern "C" fn(token: *mut c_void, status: &RawSyncStreamStatus),
    inspect_token: *mut c_void,
) {
    let status = unsafe { &*(status as *const SyncStatusData) };
    for stream in status.streams() {
        let desc = stream.subscription.description();
        let progress = stream.progress.as_ref();

        let status = RawSyncStreamStatus {
            name: StringView::view(desc.name),
            parameters: StringView::view_optional(desc.parameters.map(|p| p.get())),
            has_parameters: desc.parameters.is_some(),
            progress_total: progress.map(|p| p.total).unwrap_or_default(),
            progress_done: progress.map(|p| p.downloaded).unwrap_or_default(),
            has_progress: progress.is_some(),
            is_active: stream.subscription.is_active(),
            is_default: stream.subscription.is_default(),
            has_explicit_subscription: stream.subscription.has_explicit_subscription(),
            expires_at: stream
                .subscription
                .expires_at()
                .map(|e| {
                    let time = e.duration_since(UNIX_EPOCH).unwrap();
                    time.as_millis() as i64
                })
                .unwrap_or_default(),
            has_expires_at: stream.subscription.expires_at().is_some(),
            has_synced: stream.subscription.has_synced(),
            last_synced_at: stream
                .subscription
                .last_synced_at()
                .map(|e| {
                    let time = e.duration_since(UNIX_EPOCH).unwrap();
                    time.as_millis() as i64
                })
                .unwrap_or_default(),
        };
        inspect(inspect_token, &status);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_status_clone(status: *const c_void) {
    unsafe { Arc::increment_strong_count(status as *const SyncStatusData) };
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
