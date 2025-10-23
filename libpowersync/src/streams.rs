use powersync::StreamSubscription;
use std::ffi::c_void;
use std::mem::forget;

pub extern "C" fn powersync_stream_subscription_clone(subscription: *mut c_void) {
    let subscription = unsafe { StreamSubscription::from_raw(subscription) };
    forget(subscription.clone()); // Increment refcount
    forget(subscription); // Don't decrement refcount from subscription pointer
}

pub extern "C" fn powersync_stream_subscription_free(subscription: *mut c_void) {
    drop(unsafe { StreamSubscription::from_raw(subscription) });
}
