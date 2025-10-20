use crate::db::internal::InnerPowerSyncState;
use crate::error::PowerSyncError;
use crate::sync::coordinator::SyncCoordinator;
use crate::{LeasedConnection, PowerSyncDatabase};
use std::ffi::c_void;
use std::sync::Arc;

/// A [PowerSyncDatabase] reference that can be passed to C.
#[repr(C)]
pub struct RawPowerSyncDatabase {
    sync: *mut c_void,  // SyncCoordinator
    inner: *mut c_void, // InnerPowerSyncState
}

impl RawPowerSyncDatabase {
    /// ## Safety
    ///
    /// Must be valid pointers, and must at most be called once more than [Self::clone_into_db].
    pub unsafe fn consume_as_db(&self) -> PowerSyncDatabase {
        PowerSyncDatabase {
            sync: unsafe { Arc::from_raw(self.sync as *const _) },
            inner: unsafe { Arc::from_raw(self.inner as *mut _) },
        }
    }

    pub fn clone_into_db(&self) -> PowerSyncDatabase {
        unsafe {
            Arc::increment_strong_count(self.sync as *const SyncCoordinator);
            Arc::increment_strong_count(self.inner as *const InnerPowerSyncState);

            self.consume_as_db()
        }
    }

    pub async fn lease_reader<'a>(&'a self) -> Result<impl LeasedConnection + 'a, PowerSyncError> {
        let inner: &'a InnerPowerSyncState = unsafe {
            // Safety: Valid reference by construction of struct
            (self.inner as *const InnerPowerSyncState)
                .as_ref()
                .unwrap_unchecked()
        };

        inner.reader().await
    }

    pub async fn lease_writer<'a>(&'a self) -> Result<impl LeasedConnection + 'a, PowerSyncError> {
        let inner: &'a InnerPowerSyncState = unsafe {
            // Safety: Valid reference by construction of struct
            (self.inner as *const InnerPowerSyncState)
                .as_ref()
                .unwrap_unchecked()
        };

        inner.writer().await
    }

    /// ## Safety
    ///
    /// Must only be called once.
    pub unsafe fn free(self) {
        drop(unsafe { self.consume_as_db() });
    }
}

impl From<PowerSyncDatabase> for RawPowerSyncDatabase {
    fn from(value: PowerSyncDatabase) -> Self {
        Self {
            sync: Arc::into_raw(value.sync) as *mut c_void,
            inner: Arc::into_raw(value.inner) as *mut c_void,
        }
    }
}
