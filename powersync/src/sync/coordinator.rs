use std::sync::RwLock;

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

#[derive(Default)]
pub struct SyncCoordinator {
    control_downloads: RwLock<Option<Sender<AsyncRequest<DownloadActorCommand>>>>,
    control_uploads: RwLock<Option<Sender<AsyncRequest<UploadActorCommand>>>>,
}

impl SyncCoordinator {
    pub async fn connect(&self, options: SyncOptions) {
        self.download_actor_request(DownloadActorCommand::Connect(options))
            .await;
    }

    pub async fn disconnect(&self) {
        self.download_actor_request(DownloadActorCommand::Disconnect)
            .await;
    }

    pub async fn resolve_offline_sync_status(&self) {
        self.download_actor_request(DownloadActorCommand::ResolveOfflineSyncStatusIfNotConnected)
            .await;
    }

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

    pub fn receive_download_commands(&self) -> Receiver<AsyncRequest<DownloadActorCommand>> {
        Self::install_actor_channel(&self.control_downloads)
    }

    pub fn receive_upload_commands(&self) -> Receiver<AsyncRequest<UploadActorCommand>> {
        Self::install_actor_channel(&self.control_uploads)
    }

    async fn download_actor_request(&self, cmd: DownloadActorCommand) {
        let downloads = self.control_downloads.read().unwrap();
        let Some(downloads) = &*downloads else {
            panic!("Download actor has not been registered");
        };

        let (request, response) = AsyncRequest::new(cmd);
        downloads
            .send(request)
            .await
            .expect("Download actor not running, start it with download_actor()");
        let _ = response.await;
    }
}
