pub mod http;
mod sync_iteration;

use std::sync::Arc;

use log::warn;
pub use sync_iteration::{DownloadClient, DownloadEvent};

use crate::{SyncOptions, db::internal::InnerPowerSyncState, sync::signals::SyncChannels};

pub async fn download_loop(
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
