use crate::crud::StringView;
use crate::error::PowerSyncResultCode;
use futures_lite::future;
use powersync::StreamSubscription;
use powersync::ffi::RawPowerSyncDatabase;
use std::ffi::c_void;
use std::mem::forget;

#[unsafe(no_mangle)]
pub extern "C" fn powersync_stream_subscription_create(
    db: &RawPowerSyncDatabase,
    name: StringView,
    parameters: StringView,
    has_parameters: bool,
    out_db: &mut *mut c_void,
) -> PowerSyncResultCode {
    let future = db.create_stream_subscription(
        name.as_ref(),
        if has_parameters {
            Some(parameters.as_ref())
        } else {
            None
        },
    );
    let subscription = ps_try!(future::block_on(future));
    *out_db = subscription.into_raw();
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_stream_subscription_clone(subscription: *mut c_void) {
    let subscription = unsafe { StreamSubscription::from_raw(subscription) };
    forget(subscription.clone()); // Increment refcount
    forget(subscription); // Don't decrement refcount from subscription pointer
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_stream_subscription_free(subscription: *mut c_void) {
    drop(unsafe { StreamSubscription::from_raw(subscription) });
}
