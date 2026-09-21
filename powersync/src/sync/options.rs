use std::{sync::Arc, time::Duration};

use futures_lite::future::yield_now;

use crate::{env::PowerSyncEnvironment, error::PowerSyncError, sync::connector::BackendConnector};

/// Options controlling how PowerSync connects to a sync service.
#[derive(Clone)]
pub struct SyncOptions {
    /// The connector to fetch credentials from.
    pub(crate) connector: Arc<dyn BackendConnector>,
    /// Whether to sync `auto_subscribe: true` streams automatically.
    pub(crate) include_default_streams: bool,
    /// The retry delay between sync iterations on errors.
    pub(crate) retry_delay: Duration,

    /// How to request checkpoints after completing uploads.
    pub(crate) checkpoints: CheckpointMode,
}

impl SyncOptions {
    /// Creates new [SyncOptions] with default options given the [BackendConnector].
    pub fn new(connector: impl BackendConnector + 'static) -> Self {
        Self {
            connector: Arc::new(connector),
            include_default_streams: true,
            retry_delay: Duration::from_secs(5),
            checkpoints: CheckpointMode::default(),
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
