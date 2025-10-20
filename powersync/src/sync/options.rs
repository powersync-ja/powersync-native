use std::{sync::Arc, time::Duration};

use crate::sync::connector::BackendConnector;

/// Options controlling how PowerSync connects to a sync service.
#[derive(Clone)]
pub struct SyncOptions {
    /// The connector to fetch credentials from.
    pub(crate) connector: Arc<dyn BackendConnector>,
    /// Whether to sync `auto_subscribe: true` streams automatically.
    pub(crate) include_default_streams: bool,
    /// The retry delay between sync iterations on errors.
    pub(crate) retry_delay: Duration,
}

impl SyncOptions {
    /// Creates new [SyncOptions] with default options given the [BackendConnector].
    pub fn new(connector: impl BackendConnector + 'static) -> Self {
        Self {
            connector: Arc::new(connector),
            include_default_streams: true,
            retry_delay: Duration::from_secs(5),
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
}
