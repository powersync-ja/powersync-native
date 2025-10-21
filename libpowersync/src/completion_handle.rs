use async_oneshot::{Sender, oneshot};
use powersync::PowerSyncCredentials;
use powersync::error::PowerSyncError;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::ptr::null_mut;

#[repr(transparent)]
pub struct CppCompletionHandle {
    sender: *mut c_void, // Sender<CompletionHandleResult>
}

impl CppCompletionHandle {
    pub fn take_sender(&mut self) -> Option<Box<Sender<CompletionHandleResult>>> {
        if self.sender.is_null() {
            None
        } else {
            let prev_sender = self.sender;
            self.sender = null_mut();
            Some(unsafe { Box::from_raw(prev_sender as *mut _) })
        }
    }
}

pub type CompletionHandleResult = Result<CompletionHandleValue, PowerSyncError>;

pub enum CompletionHandleValue {
    Credentials(PowerSyncCredentials),
    Empty,
}

pub struct RustCompletionHandle {
    receiver: async_oneshot::Receiver<CompletionHandleResult>,
}

impl RustCompletionHandle {
    pub fn new() -> (CppCompletionHandle, RustCompletionHandle) {
        let (sender, receiver) = oneshot();

        let sender = CppCompletionHandle {
            sender: Box::into_raw(Box::new(sender)) as *mut _,
        };
        let receiver = RustCompletionHandle { receiver };
        (sender, receiver)
    }

    pub async fn receive(self) -> CompletionHandleResult {
        self.receiver.await.unwrap_or_else(|_| {
            Err(PowerSyncError::argument_error(
                "Dropped completion handle without completing it.",
            ))
        })
    }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_completion_handle_complete_credentials(
    handle: &mut CppCompletionHandle,
    endpoint: *const c_char,
    token: *const c_char,
) {
    let endpoint = unsafe { CStr::from_ptr(endpoint) }
        .to_str()
        .unwrap()
        .to_owned();
    let token = unsafe { CStr::from_ptr(token) }
        .to_str()
        .unwrap()
        .to_owned();

    if let Some(mut sender) = handle.take_sender() {
        let _ = sender.send(Ok(CompletionHandleValue::Credentials(
            PowerSyncCredentials { endpoint, token },
        )));
    }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_completion_handle_complete_empty(handle: &mut CppCompletionHandle) {
    if let Some(mut sender) = handle.take_sender() {
        let _ = sender.send(Ok(CompletionHandleValue::Empty));
    }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_completion_handle_complete_error_code(
    handle: &mut CppCompletionHandle,
    code: c_int,
) {
    if let Some(mut sender) = handle.take_sender() {
        let _ = sender.send(Err(PowerSyncError::user_error(code)));
    }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_completion_handle_complete_error_msg(
    handle: &mut CppCompletionHandle,
    code: c_int,
    msg: *const c_char,
) {
    let msg = unsafe { CStr::from_ptr(msg) }.to_str().unwrap().to_owned();

    if let Some(mut sender) = handle.take_sender() {
        let _ = sender.send(Err(PowerSyncError::user_error_with_message(code, msg)));
    }
}

#[unsafe(no_mangle)]
extern "C" fn powersync_completion_handle_free(handle: &mut CppCompletionHandle) {
    drop(handle.take_sender());
}
