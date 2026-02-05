use crate::completion_handle::{CompletionHandleValue, CppCompletionHandle, RustCompletionHandle};
use http_client::async_trait;
use powersync::error::PowerSyncError;
use powersync::{BackendConnector, PowerSyncCredentials};

#[repr(C)]
pub struct CppConnector {
    upload_data: unsafe extern "C" fn(*mut CppConnector, CppCompletionHandle),
    fetch_credentials: unsafe extern "C" fn(*mut CppConnector, CppCompletionHandle),
    drop: unsafe extern "C" fn(*mut CppConnector),
}

struct CppConnectorWrapper(*const CppConnector);

unsafe impl Send for CppConnectorWrapper {
    // SAFETY: Requirement in C++, connectors need to be thread-safe.
}
unsafe impl Sync for CppConnectorWrapper {
    // SAFETY: Requirement in C++, connectors need to be thread-safe.
}

impl AsRef<CppConnector> for CppConnectorWrapper {
    fn as_ref(&self) -> &CppConnector {
        unsafe { &*self.0 }
    }
}

/// ## Safety
///
/// The connector pointer must point to a valid connector implementation.
pub unsafe fn wrap_cpp_connector(
    connector: *const CppConnector,
) -> impl BackendConnector + 'static {
    CppConnectorWrapper(connector)
}

#[async_trait]
impl BackendConnector for CppConnectorWrapper {
    async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
        let (send, recv) = RustCompletionHandle::new();
        let connector: &CppConnector = self.as_ref();
        let handler = connector.fetch_credentials;
        unsafe { handler(self.0.cast_mut(), send) };

        let credentials = recv.receive().await?;
        let CompletionHandleValue::Credentials(credentials) = credentials else {
            return Err(PowerSyncError::argument_error(
                "Expected completion with request.",
            ));
        };
        Ok(credentials)
    }

    async fn upload_data(&self) -> Result<(), PowerSyncError> {
        let (send, recv) = RustCompletionHandle::new();
        let connector: &CppConnector = self.as_ref();
        let handler = connector.upload_data;
        unsafe { handler(self.0.cast_mut(), send) };

        let value = recv.receive().await?;
        match value {
            CompletionHandleValue::Empty => Ok(()),
            _ => Err(PowerSyncError::argument_error(
                "Expected completion with empty value.",
            )),
        }
    }
}

impl Drop for CppConnectorWrapper {
    fn drop(&mut self) {
        let connector: &CppConnector = self.as_ref();
        let handler = connector.drop;

        unsafe {
            handler(self.0.cast_mut());
        }
    }
}
