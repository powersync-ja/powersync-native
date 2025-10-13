use std::{sync::Arc, time::Duration};

use crate::sync::connector::BackendConnector;

#[derive(Clone)]
pub struct SyncOptions {
    pub(crate) connector: Arc<dyn BackendConnector>,
    pub(crate) include_default_streams: bool,
    pub(crate) retry_delay: Duration,
}

impl SyncOptions {
    pub fn new(connector: impl BackendConnector + 'static) -> Self {
        Self {
            connector: Arc::new(connector),
            include_default_streams: true,
            retry_delay: Duration::from_secs(5),
        }
    }

    pub fn set_include_default_streams(&mut self, include: bool) {
        self.include_default_streams = include;
    }

    pub fn with_retry_delay(&mut self, delay: Duration) {
        self.retry_delay = delay;
    }
}
