use std::sync::RwLock;

use async_channel::{Receiver, Sender};
use async_oneshot::oneshot;

use crate::{SyncOptions, sync::download::DownloadActorCommand};

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

    pub fn receive_download_commands(&self) -> Receiver<AsyncRequest<DownloadActorCommand>> {
        let mut downloads = self.control_downloads.write().unwrap();
        if downloads.is_some() {
            drop(downloads);
            panic!("Download actor already active")
        }

        let (send, receive) = async_channel::bounded(1);
        *downloads = Some(send);
        receive
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
