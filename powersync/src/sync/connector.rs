use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use url::Url;

use crate::error::{PowerSyncError, RawPowerSyncError};

/// Authenticates the PowerSync SDK against a PowerSync service, allowing it to sync changes from a
/// backend source database.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Resolves a JWT to use when connecting to a PowerSync service.
    ///
    /// The SDK does not cache this value and will call this method multiple times while a
    /// database is connected. Implementations should consider caching credentials.
    async fn resolve_credentials(&self) -> Result<Arc<String>, PowerSyncError>;

    /// Invoked by the SDK when the PowerSync service has rejected credentials previously
    /// returned by [Self::resolve_credentials].
    ///
    /// If the ocnnector returns cached tokens, it can use this as a hint to refresh its local
    /// state.
    fn invalidate_credentials(&self) {}

    /// This is optional, and should only return a future for connectors capable of requesting
    /// checkpoints.
    ///
    /// For uploads that are processed asynchronously by a backend (for example through a message
    /// queue): The sync client as part of the PowerSync Rust SDK generates a checkpoint request id
    /// and hands it to your backend via this function, which is responsible for creaeting a
    /// matching checkpoint once the uploads preceeding the request have been processed.
    ///
    /// For more details, see [asynchronous backend uploads](https://docs.powersync.com/client-sdks/advanced/checkpoint-requests#asynchronous-upload-backends).
    ///
    /// To use this connector, using [crate::sync::options::CheckpointMode::Requests] is required.
    /// Note that this requires PowerSync service version 1.24.0 or later.
    fn post_checkpoint_request<'a>(
        &'a self,
        _client_id: &'a str,
        _request_id: i64,
    ) -> Option<Pin<Box<dyn Future<Output = Result<i64, PowerSyncError>> + Send + 'a>>> {
        None
    }
}

/// Uploads local mutations (from `INSERT`, `UPDATE` and `DELETE` statements against the
/// local database) to your source database.
///
/// While simple cases might use a protocol like PostgREST to write directly into the database,
/// a custom backend is commonly used to validate uploaded mutations.
#[async_trait]
pub trait MutationUploader: Send + Sync {
    async fn upload(&self) -> Result<(), PowerSyncError>;
}

/// Allows using async functions and closures (e.g. `|| async { ... }`) as a [MutationUploader].
#[async_trait]
impl<F, Fut> MutationUploader for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), PowerSyncError>> + Send,
{
    async fn upload(&self) -> Result<(), PowerSyncError> {
        self().await
    }
}

/// Credentials used to connect to a PowerSync service instance.
pub struct PowerSyncCredentials<'a> {
    /// PowerSync endpoint, e.g. `https://myinstance.powersync.co`.
    pub endpoint: &'a str,
    /// The token used to authenticate against the PowerSync service.
    pub token: Arc<String>,
}

impl<'a> PowerSyncCredentials<'a> {
    /// Parses the [Self::endpoint] into a URI.
    pub(crate) fn parsed_endpoint(&self, endpoint: &str) -> Result<Url, PowerSyncError> {
        let url = Url::parse(&self.endpoint)
            .map_err(|e| RawPowerSyncError::InvalidPowerSyncEndpoint { inner: e })?;

        url.join(endpoint).map_err(|_| {
            PowerSyncError::argument_error(format!(
                "URL {} must be a valid base URL",
                self.endpoint
            ))
        })
    }
}

#[cfg(test)]
mod test {
    use super::PowerSyncCredentials;
    use std::sync::Arc;

    fn is_endpoint_valid(endpoint: &str) -> bool {
        PowerSyncCredentials {
            token: Arc::new("test".to_string()),
            endpoint,
        }
        .parsed_endpoint("")
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
