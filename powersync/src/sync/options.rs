use std::{sync::Arc, time::Duration};

use futures_lite::future::yield_now;

use crate::{
    MutationUploader,
    env::PowerSyncEnvironment,
    error::PowerSyncError,
    sync::connector::{Authenticator, PowerSyncCredentials},
};

/// Options controlling how PowerSync connects to a sync service.
#[derive(Clone)]
pub struct SyncOptions {
    /// The authenticator to fetch credentials from, if downloading is enabled.
    pub(crate) endpoint: Option<Arc<EndpointAndAuthenticator>>,
    pub(crate) uploader: Option<Arc<dyn MutationUploader>>,
    /// Whether to sync `auto_subscribe: true` streams automatically.
    pub(crate) include_default_streams: bool,
    /// The retry delay between sync iterations on errors.
    pub(crate) retry_delay: Duration,

    /// How to request checkpoints after completing uploads.
    pub(crate) checkpoints: CheckpointMode,
}

impl SyncOptions {
    fn empty() -> Self {
        Self {
            endpoint: None,
            uploader: None,
            include_default_streams: true,
            retry_delay: Duration::from_secs(5),
            checkpoints: CheckpointMode::default(),
        }
    }

    /// Creates new [SyncOptions] with default options given the [Authenticator] and
    /// [MutationUploader].
    pub fn new(
        endpoint: &str,
        authenticator: impl Authenticator + 'static,
        uploader: impl MutationUploader + 'static,
    ) -> Self {
        let mut downloads = Self::download_only(endpoint, authenticator);
        downloads.uploader = Some(Arc::new(uploader));
        downloads
    }

    /// Creates new [SyncOptions] for downloading only.
    pub fn download_only(endpoint: &str, authenticator: impl Authenticator + 'static) -> Self {
        Self {
            endpoint: Some(Arc::new(EndpointAndAuthenticator {
                endpoint: endpoint.to_owned(),
                authenticator: Box::new(authenticator),
            })),
            ..Self::empty()
        }
    }

    /// Creates new sync options for uploading only.
    ///
    /// When connecting with these options, the sync client won't attempt to connect to a PowerSync
    /// service.
    pub fn upload_only(uploader: impl MutationUploader + 'static) -> Self {
        Self {
            uploader: Some(Arc::new(uploader)),
            ..Self::empty()
        }
    }

    /// Whether to sync streams that have `auto_subscribe: true`.
    ///
    /// This is enabled by default.
    pub fn set_include_default_streams(&mut self, include: bool) {
        self.include_default_streams = include;
    }

    /// Configures the delay after a failed sync iteration (the default is 5 seconds).
    pub fn with_retry_delay(&mut self, delay: Duration) {
        self.retry_delay = delay;
    }

    pub(crate) fn retry_delay(
        &self,
        env: &PowerSyncEnvironment,
    ) -> impl Future<Output = ()> + 'static {
        let delay = self.retry_delay;
        let future = if delay > Duration::ZERO {
            Some(env.runtime.delay_once(delay))
        } else {
            None
        };

        async move {
            if let Some(future) = future {
                future.await;
            } else {
                yield_now().await
            }
        }
    }

    /// Configures the [CheckpointMode] used to request checkpoints after completed uploads.
    ///
    /// Using [CheckpointMode::Requests] requires PowerSync service version 1.24.0 or later.
    /// [CheckpointMode::Legacy] is used by default for compatibility with older services.
    pub fn with_checkpoint_mode(&mut self, mode: CheckpointMode) {
        self.checkpoints = mode;
    }
}

pub(crate) struct EndpointAndAuthenticator {
    pub endpoint: String,
    pub authenticator: Box<dyn Authenticator>,
}

impl EndpointAndAuthenticator {
    pub async fn fetch_credentials<'a>(
        &'a self,
    ) -> Result<PowerSyncCredentials<'a>, PowerSyncError> {
        let jwt = self.authenticator.resolve_credentials().await?;
        Ok(PowerSyncCredentials {
            endpoint: &self.endpoint,
            token: jwt,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Uses a legacy endpoint to request checkpoints after uploading data.
    #[default]
    Legacy,
    /// Uses a newer protocol to request checkpoints after uploads with client-generated checkpoint
    /// request ids.
    Requests(RequestsCheckpointMode),
}

/// Options for [CheckpointMode::Requests].
///
/// This can be used to configure the retry delay after which requested checkpoints that have not
/// been synced yet are automatically reposted by the SDK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestsCheckpointMode {
    pub(crate) retry_delay: Duration,
}

impl RequestsCheckpointMode {
    const DEFAULT_RETRY: Duration = Duration::from_secs(10);
    const MIN_RETRY: Duration = Self::DEFAULT_RETRY;
}

impl RequestsCheckpointMode {
    /// Configures the new request checkpoint protocol with a custom retry duration.
    ///
    /// This duration is not the same as [SyncOptions::with_retry_delay] (that refers to errors).
    /// This duration is used by clients to repost checkpoint requests to the service if they have
    /// not been included in a sync response before.
    ///
    /// Retries are used to work around a race condition where network packet reordering when
    /// requesting multiple checkpoints in quick succession could otherwise cause clients to wait
    /// forever for a checkpoint.
    pub fn with_retry_duration(value: Duration) -> Result<Self, PowerSyncError> {
        if value < Self::MIN_RETRY {
            return Err(PowerSyncError::argument_error(format!(
                "Minimum retry delay is 10s, got {value:?}"
            )));
        }

        Ok(Self { retry_delay: value })
    }
}

impl Default for RequestsCheckpointMode {
    fn default() -> Self {
        Self {
            retry_delay: Self::DEFAULT_RETRY,
        }
    }
}
