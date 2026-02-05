use std::cell::Cell;
use log::warn;
use powersync::error::PowerSyncError;

thread_local! {
    pub static LAST_ERROR: Cell<Option<PowerSyncError>> = Cell::new(None);
}

#[repr(C)]
pub enum PowerSyncResultCode {
    OK = 0,
    ERROR = 1,
}

impl Default for PowerSyncResultCode {
    fn default() -> Self {
        Self::OK
    }
}

impl Into<PowerSyncResultCode> for PowerSyncError {
    fn into(self) -> PowerSyncResultCode {
        warn!("Returning error: {}", self);
        LAST_ERROR.replace(Some(self.into()));

        PowerSyncResultCode::ERROR
    }
}
