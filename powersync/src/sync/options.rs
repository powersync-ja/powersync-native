use std::sync::Arc;

use crate::sync::connector::BackendConnector;

#[derive(Clone)]
pub struct SyncOptions {
    pub(crate) connector: Arc<dyn BackendConnector>,
    pub(crate) include_default_streams: bool,
}

impl SyncOptions {
    pub fn new(connector: impl BackendConnector + 'static) -> Self {
        Self {
            connector: Arc::new(connector),
            include_default_streams: true,
        }
    }

    pub fn set_include_default_streams(&mut self, include: bool) {
        self.include_default_streams = include;
    }
}
