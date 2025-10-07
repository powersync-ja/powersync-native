use async_trait::async_trait;
use url::Url;

use crate::error::{PowerSyncError, RawPowerSyncError};

#[async_trait]
pub trait BackendConnector: Send + Sync {
    async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError>;
    async fn upload_data(&self) -> Result<(), PowerSyncError>;
}

pub struct PowerSyncCredentials {
    /// PowerSync endpoint, e.g. `https://myinstance.powersync.co`.
    pub endpoint: String,
    /// The token used to authenticate against the PowerSync service.
    pub token: String,
}

impl PowerSyncCredentials {
    pub fn parsed_endpoint(&self) -> Result<Url, PowerSyncError> {
        Ok(Url::parse(&self.endpoint)
            .map_err(|e| RawPowerSyncError::InvalidPowerSyncEndpoint { inner: e })?)
    }
}
