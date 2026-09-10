pub mod http;
mod sync_iteration;

use std::sync::Arc;

use futures_lite::future;
use log::warn;
pub use sync_iteration::{DownloadClient, DownloadEvent};

use crate::{SyncOptions, db::internal::InnerPowerSyncState, sync::coordinator::SyncChannels};

pub async fn download_loop(
    db: Arc<InnerPowerSyncState>,
    channels: SyncChannels,
    options: SyncOptions,
    events: async_channel::Receiver<DownloadEvent>,
) {
    scopeguard::defer! {
        channels.checkpoints.disconnected();
    };

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

        let iteration_ended = channels.checkpoints.download_iteration_ended();
        if delay_retry {
            future::or(options.retry_delay(&db.env), iteration_ended).await
        }
    }
}
