use async_executor::Executor;
use futures_lite::future;
use futures_lite::future::block_on;
use powersync::{InnerPowerSyncState, PowerSyncDatabase};

/// Runs asynchronous PowerSync tasks on the current thread.
///
/// This blocks the thread until the database is closed.
#[unsafe(no_mangle)]
pub extern "C" fn powersync_run_tasks(db: *const InnerPowerSyncState) {
    let db = unsafe { PowerSyncDatabase::interpret_raw(db) };
    let executor = Executor::new();
    let downloader = executor.spawn(db.download_actor());
    let uploader = executor.spawn(db.upload_actor());

    // The actors will run until the source database is closed, so we wait for that to happen.
    let future = future::or(downloader, uploader);
    block_on(executor.run(future));
}
