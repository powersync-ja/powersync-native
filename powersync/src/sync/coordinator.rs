use std::{sync::RwLock, time::Duration};

use async_channel::{Receiver, Sender};
use async_oneshot::oneshot;

use crate::{
    SyncOptions,
    sync::{
        download::DownloadActorCommand, streams::ChangedSyncSubscriptions,
        upload::UploadActorCommand,
    },
};

pub struct AsyncRequest<T> {
    pub command: T,
    pub response: async_oneshot::Sender<()>,
}

impl<T> AsyncRequest<T> {
    pub fn new(command: T) -> (Self, async_oneshot::Receiver<()>) {
        let (tx, rx) = oneshot();
        (
            Self {
                command,
                response: tx,
            },
            rx,
        )
    }
}

/// Implements `connect()` and `disconnect()` by dispatching messages to the upload and download
/// actors.
#[derive(Default)]
pub struct SyncCoordinator {
    control_downloads: RwLock<Option<Sender<AsyncRequest<DownloadActorCommand>>>>,
    control_uploads: RwLock<Option<Sender<AsyncRequest<UploadActorCommand>>>>,
    pub(crate) retry_delay: Option<Duration>,
}

impl SyncCoordinator {
    pub async fn connect(&self, options: SyncOptions) {
        let connector = options.connector.clone();
        self.download_actor_request(DownloadActorCommand::Connect(options))
            .await;
        self.upload_actor_request(UploadActorCommand::Connect(connector))
            .await;
    }

    pub async fn disconnect(&self) {
        self.download_actor_request(DownloadActorCommand::Disconnect)
            .await;
        self.upload_actor_request(UploadActorCommand::Disconnect)
            .await;
    }

    /// Requests a round of CRUD uploads.
    pub async fn trigger_crud_uploads(&self) {
        self.upload_actor_request(UploadActorCommand::TriggerCrudUpload)
            .await;
    }

    /// Marks CRUD uploads as complete, allowing the download client to retry if a previous
    /// checkpoint was blocked by pending uploads.
    pub async fn mark_crud_uploads_completed(&self) {
        self.download_actor_request(DownloadActorCommand::CrudUploadComplete)
            .await;
    }

    /// Causes the download actor to call `powersync_offline_sync_status()` and emit those results.
    ///
    /// This is used after adding a new subscription to include it in the sync status even if we're
    /// disconnected.
    /// This is a no-op while connected.
    pub async fn resolve_offline_sync_status(&self) {
        self.download_actor_request(DownloadActorCommand::ResolveOfflineSyncStatusIfNotConnected)
            .await;
    }

    /// Handle the set of active sync stream subscriptions changing.
    ///
    /// This is a no-op if not connected.
    pub async fn handle_subscriptions_changed(&self, update: ChangedSyncSubscriptions) {
        self.download_actor_request(DownloadActorCommand::SubscriptionsChanged(update))
            .await;
    }

    fn install_actor_channel<T>(
        slot: &RwLock<Option<Sender<AsyncRequest<T>>>>,
    ) -> Receiver<AsyncRequest<T>> {
        let mut slot = slot.write().unwrap();
        if slot.is_some() {
            drop(slot);
            panic!("Actor already installed")
        }

        let (send, receive) = async_channel::bounded(1);
        *slot = Some(send);
        receive
    }

    fn obtain_channel<T>(
        slot: &RwLock<Option<Sender<AsyncRequest<T>>>>,
    ) -> Sender<AsyncRequest<T>> {
        let slot = slot.read().unwrap();
        let Some(slot) = &*slot else {
            panic!("Actor has not been registered");
        };

        slot.clone()
    }

    pub fn receive_download_commands(&self) -> Receiver<AsyncRequest<DownloadActorCommand>> {
        Self::install_actor_channel(&self.control_downloads)
    }

    pub fn receive_upload_commands(&self) -> Receiver<AsyncRequest<UploadActorCommand>> {
        Self::install_actor_channel(&self.control_uploads)
    }

    async fn download_actor_request(&self, cmd: DownloadActorCommand) {
        let downloads = Self::obtain_channel(&self.control_downloads);

        let (request, response) = AsyncRequest::new(cmd);
        downloads
            .send(request)
            .await
            .expect("Download actor not running, start it with download_actor()");
        let _ = response.await;
    }

    async fn upload_actor_request(&self, cmd: UploadActorCommand) {
        let uploads = Self::obtain_channel(&self.control_uploads);

        let (request, response) = AsyncRequest::new(cmd);
        uploads
            .send(request)
            .await
            .expect("Upload actor not running, start it with upload_actor()");
        let _ = response.await;
    }
}
