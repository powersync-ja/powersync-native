use async_executor::Executor;
use futures_lite::future::block_on;
use powersync::ffi::RawPowerSyncDatabase;

/// Runs asynchronous PowerSync tasks on the current thread.
///
/// This blocks the thread until the database is closed.
#[unsafe(no_mangle)]
pub extern "C" fn powersync_run_tasks(db: &RawPowerSyncDatabase) {
    let tasks = RawPowerSyncDatabase::clone_into_db(db).async_tasks();
    let executor = Executor::new();
    let tasks = tasks.spawn_with(|f| executor.spawn(f));

    // The actors will run until the source database is closed, so we wait for that to happen.
    block_on(executor.run(async move {
        for task in tasks {
            task.await;
        }
    }));
}
