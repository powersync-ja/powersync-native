use async_trait::async_trait;
use url::Url;

use crate::error::{PowerSyncError, RawPowerSyncError};

/// A backend connector is responsible for upload local writes as well as resolving JWTs used to
/// connect to the PowerSync service.
#[async_trait]
pub trait BackendConnector: Send + Sync {
    /// Fetches a fresh JWT from the backend to be used against the PowerSync service.
    async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError>;

    /// Inspects completed CRUD transactions on a database and uploads them.
    async fn upload_data(&self) -> Result<(), PowerSyncError>;
}

/// Credentials used to connect to a PowerSync service instance.
pub struct PowerSyncCredentials {
    /// PowerSync endpoint, e.g. `https://myinstance.powersync.co`.
    pub endpoint: String,
    /// The token used to authenticate against the PowerSync service.
    pub token: String,
}

impl PowerSyncCredentials {
    /// Parses the [Self::endpoint] into a URI.
    pub(crate) fn parsed_endpoint(&self) -> Result<Url, PowerSyncError> {
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
