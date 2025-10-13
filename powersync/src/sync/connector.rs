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
        let url = Url::parse(&self.endpoint)
            .map_err(|e| RawPowerSyncError::InvalidPowerSyncEndpoint { inner: e })?;
        if url.cannot_be_a_base() {
            return Err(PowerSyncError::argument_error(format!(
                "URL {} must be a valid base URL",
                self.endpoint
            )));
        }

        Ok(url)
    }
}

#[cfg(test)]
mod test {
    use crate::PowerSyncCredentials;

    fn is_endpoint_valid(endpoint: &str) -> bool {
        PowerSyncCredentials {
            token: "".to_string(),
            endpoint: endpoint.to_string(),
        }
        .parsed_endpoint()
        .is_ok()
    }

    #[test]
    fn endpoint_validation() {
        assert!(!is_endpoint_valid("localhost:8080"));

        assert!(is_endpoint_valid("http://localhost:8080"));
        assert!(is_endpoint_valid("http://localhost:8080/"));
        assert!(is_endpoint_valid("http://localhost:8080/powersync"));
    }
}
