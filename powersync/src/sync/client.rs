use std::sync::Arc;

use log::warn;

use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    env::PowerSyncTask,
    sync::{
        download::{DownloadClient, DownloadEvent},
        signals::{SyncChannels, SyncSignals},
        upload::crud_upload_loop,
    },
};

/// Spawns tasks connecting to the PowerSync service, driving the upload and download process.
pub fn spawn(
    db: Arc<InnerPowerSyncState>,
    options: SyncOptions,
    signals: Arc<SyncSignals>,
    abort_signal: async_oneshot::Receiver<()>,
) -> PowerSyncTask {
    let (channels, download_receive, uploads_receive) = SyncChannels::create();
    let guard = signals.install_channels(channels.clone());
    let env = &db.env;
    let db = db.clone();

    env.spawn(async move {
        let download = db.env.spawn(download_loop(
            db.clone(),
            channels.clone(),
            options.clone(),
            download_receive,
        ));
        let upload = db.env.spawn(crud_upload_loop(
            db.clone(),
            options,
            channels.clone(),
            uploads_receive,
        ));

        let _ = abort_signal.await;

        download.cancel().await;
        upload.cancel().await;
        drop(guard);
    })
}

async fn download_loop(
    db: Arc<InnerPowerSyncState>,
    channels: SyncChannels,
    options: SyncOptions,
    events: async_channel::Receiver<DownloadEvent>,
) {
    loop {
        let download = DownloadClient::new(db.clone(), &channels, &events, &options);
        let delay_retry = match download.run().await {
            Ok(end) => !end.hide_disconnect,
            Err(e) => {
                warn!("Sync iteration failed, {e}");
                db.status.update(|data| data.set_download_error(e));

                true
            }
        };

        if delay_retry {
            options.retry_delay(&db.env).await
        }
    }
}
