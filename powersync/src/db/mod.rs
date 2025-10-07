use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use event_listener::EventListener;
use futures_lite::{FutureExt, Stream, ready};

use crate::{
    PowerSyncEnvironment, SyncOptions,
    db::{internal::InnerPowerSyncState, pool::LeasedConnection, streams::SyncStream},
    error::PowerSyncError,
    schema::Schema,
    sync::{
        AsyncRequest,
        download::{DownloadActorCommand, run_download_actor},
        status::SyncStatusData,
    },
};

pub mod core_extension;
pub(crate) mod internal;
pub mod pool;
pub mod schema;
pub mod streams;

#[derive(Clone)]
pub struct PowerSyncDatabase {
    inner: Arc<InnerPowerSyncState>,
    download_actor_commands: async_channel::Sender<AsyncRequest<DownloadActorCommand>>,
}

impl PowerSyncDatabase {
    pub fn new(env: PowerSyncEnvironment, schema: Schema<'static>) -> Self {
        let (send_download, receive_download) = async_channel::bounded(1);

        Self {
            inner: Arc::new(InnerPowerSyncState::new(env, schema, receive_download)),
            download_actor_commands: send_download,
        }
    }

    /// Starts an actor responsible for establishing a connection to the sync service when
    /// [Self::connect] is called on the database.
    ///
    /// By exposing this as an entrypoint instead of starting the task on a specific entrypoint,
    /// the SDK stays executor-agnostic and is easier to access from C.
    pub async fn download_actor(&self) {
        run_download_actor(self.inner.clone()).await;
    }

    /// Requests the download actor, started with [Self::download_actor], to start establishing a
    /// connection to the PowerSync service.
    pub async fn connect(&self, options: SyncOptions) {
        self.download_actor_request(DownloadActorCommand::Connect(options))
            .await
    }

    pub async fn disconnect(&self) {
        self.download_actor_request(DownloadActorCommand::Disconnect)
            .await
    }

    async fn download_actor_request(&self, cmd: DownloadActorCommand) {
        let (request, response) = AsyncRequest::new(cmd);
        self.download_actor_commands
            .send(request)
            .await
            .expect("Download actor not running, start it with download_actor()");
        let _ = response.await;
    }

    pub fn status(&self) -> Arc<SyncStatusData> {
        self.inner.status.current_snapshot()
    }

    pub fn watch_status<'a>(&'a self) -> impl Stream<Item = Arc<SyncStatusData>> + 'a {
        struct StreamImpl<'a> {
            db: &'a PowerSyncDatabase,
            last_data: Option<Arc<SyncStatusData>>,
            waiter: Option<EventListener>,
        }

        impl<'a> Stream for StreamImpl<'a> {
            type Item = Arc<SyncStatusData>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                let this = &mut *self;

                let Some(last_data) = &mut this.last_data else {
                    // First poll, return immediately with the initial snapshot.
                    let data = this.db.status();
                    this.last_data = Some(data.clone());
                    return Poll::Ready(Some(data));
                };

                loop {
                    // Are we already waiting? If so, continue.
                    if let Some(waiter) = &mut this.waiter {
                        ready!(waiter.poll(cx));
                        this.waiter = None;

                        let data = this.db.status();
                        *last_data = data.clone();
                        return Poll::Ready(Some(data));
                    }

                    // Wait for previous data to become outdated.
                    let Some(listener) = last_data.listen_for_changes() else {
                        let data = this.db.status();
                        *last_data = data.clone();
                        return Poll::Ready(Some(data));
                    };

                    this.waiter = Some(listener);
                }
            }
        }

        StreamImpl {
            db: self,
            last_data: None,
            waiter: None,
        }
    }

    /// Creates a [SyncStream]
    pub fn sync_stream<'a>(
        &'a self,
        name: &'a str,
        parameters: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> SyncStream<'a> {
        SyncStream::new(self, name, parameters)
    }

    pub async fn reader(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.reader().await
    }

    pub async fn writer(&self) -> Result<impl LeasedConnection, PowerSyncError> {
        self.inner.writer().await
    }
}
