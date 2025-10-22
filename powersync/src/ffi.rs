use crate::db::crud::CrudTransactionStream;
use crate::db::internal::InnerPowerSyncState;
use crate::error::PowerSyncError;
use crate::sync::coordinator::SyncCoordinator;
use crate::util::raw_listener::CallbackListenerHandle;
use crate::{
    BackendConnector, CrudTransaction, LeasedConnection, PowerSyncDatabase, SyncOptions,
    SyncStatusData,
};
use futures_lite::Stream;
use std::collections::HashSet;
use std::ffi::c_void;
use std::sync::Arc;

/// A [PowerSyncDatabase] reference that can be passed to C.
#[repr(C)]
pub struct RawPowerSyncDatabase {
    sync: *mut c_void,  // SyncCoordinator
    inner: *mut c_void, // InnerPowerSyncState
}

struct RawPowerSyncReference<'a> {
    sync: &'a SyncCoordinator,
    inner: &'a InnerPowerSyncState,
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

    fn as_ref(&self) -> RawPowerSyncReference {
        RawPowerSyncReference {
            sync: unsafe {
                // Safety: Valid reference by construction of struct
                (self.sync as *const SyncCoordinator)
                    .as_ref()
                    .unwrap_unchecked()
            },
            inner: unsafe {
                // Safety: Valid reference by construction of struct
                (self.inner as *const InnerPowerSyncState)
                    .as_ref()
                    .unwrap_unchecked()
            },
        }
    }

    pub async fn lease_reader<'a>(&'a self) -> Result<impl LeasedConnection + 'a, PowerSyncError> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        inner.reader().await
    }

    pub async fn lease_writer<'a>(&'a self) -> Result<impl LeasedConnection + 'a, PowerSyncError> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        inner.writer().await
    }

    pub async fn connect(
        &self,
        connector: impl BackendConnector + 'static,
    ) -> Result<(), PowerSyncError> {
        let RawPowerSyncReference { sync, inner } = self.as_ref();
        sync.connect(SyncOptions::new(connector), inner).await;

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), PowerSyncError> {
        let RawPowerSyncReference { sync, inner } = self.as_ref();
        sync.disconnect().await;

        Ok(())
    }

    pub fn sync_status(&self) -> Arc<SyncStatusData> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        inner.status.current_snapshot()
    }

    pub fn install_status_listener<'a>(
        &'a self,
        f: impl Fn() + Send + Sync + 'a,
    ) -> CallbackListenerHandle<'a, ()> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        inner.status.listener(f)
    }

    pub fn install_table_listener<'a>(
        &'a self,
        tables: HashSet<String>,
        f: impl Fn() + Send + Sync + 'a,
    ) -> CallbackListenerHandle<'a, HashSet<String>> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        inner
            .env
            .pool
            .update_notifiers()
            .install_callback(tables, f)
    }

    pub fn crud_transactions<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<CrudTransaction<'a>, PowerSyncError>> + 'a {
        let RawPowerSyncReference { inner, .. } = self.as_ref();
        CrudTransactionStream::new(inner)
    }

    pub async fn complete_crud_items(
        &self,
        last_item_id: i64,
        write_checkpoint: Option<i64>,
    ) -> Result<(), PowerSyncError> {
        let RawPowerSyncReference { inner, .. } = self.as_ref();

        inner
            .complete_crud_items(last_item_id, write_checkpoint)
            .await
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
