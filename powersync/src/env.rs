use std::sync::Arc;

use http_client::HttpClient;

use crate::{db::core_extension::powersync_init_static, error::PowerSyncError};

use super::db::pool::ConnectionPool;

pub struct PowerSyncEnvironment {
    pub(crate) client: Arc<dyn HttpClient>,
    pub(crate) pool: ConnectionPool,
}

impl PowerSyncEnvironment {
    pub fn custom(client: Arc<dyn HttpClient>, pool: ConnectionPool) -> Self {
        Self { client, pool }
    }

    pub fn powersync_auto_extension() -> Result<(), PowerSyncError> {
        let rc = unsafe { powersync_init_static() };
        match rc {
            0 => Ok(()),
            _ => Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rc),
                Some("Loading PowerSync core extension failed".into()),
            )
            .into()),
        }
    }
}
