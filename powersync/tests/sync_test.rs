use async_task::Task;
use futures_lite::{StreamExt, future};
use powersync::{PowerSyncDatabase, SyncOptions, SyncStatusData};
use powersync_test_utils::{DatabaseTest, mock_sync_service::TestConnector, sync_line::Checkpoint};
use rusqlite::params;

struct SyncStreamTest {
    test: DatabaseTest,
    db: PowerSyncDatabase,
    download_actor_task: Task<()>,
}

impl SyncStreamTest {
    fn new() -> Self {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();
        let sync_task = test.ex.spawn({
            let db = db.clone();
            async move { db.download_actor().await }
        });

        Self {
            db,
            test,
            download_actor_task: sync_task,
        }
    }

    fn connect(&self) {
        self.connect_options(|_| {});
    }

    fn connect_options(&self, configure: impl FnOnce(&mut SyncOptions)) {
        let mut options = SyncOptions::new(TestConnector);
        configure(&mut options);

        self.run(self.db.connect(options))
    }

    fn run<T>(&self, future: impl Future<Output = T>) -> T {
        future::block_on(self.test.ex.run(future))
    }

    async fn wait_for_status(&self, mut predicate: impl FnMut(&SyncStatusData) -> bool) {
        let mut stream = self.db.watch_status();
        loop {
            let status = stream.next().await.unwrap();
            if predicate(&status) {
                return;
            }
        }
    }

    async fn wait_for_progress(&self, name: &'static str, completed: i64, total: i64) {
        self.wait_for_status(|status| {
            let stream = self.db.sync_stream(name, None);
            let Some(actual) = status.for_stream(&stream) else {
                return false;
            };

            let Some(progress) = actual.progress else {
                return false;
            };

            return progress.total == total && progress.downloaded == completed;
        })
        .await
    }
}

#[test]
fn can_disable_default_stream() {
    let sync = SyncStreamTest::new();
    sync.connect_options(|o| o.set_include_default_streams(false));

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();

        let streams = request.request_data.get("streams").unwrap();

        assert_eq!(
            streams.get("include_defaults").unwrap().as_bool(),
            Some(false)
        );
    });
}

#[test]
fn progress_without_priorities() {
    let sync = SyncStreamTest::new();
    sync.connect();

    sync.run(async {
        let mut oplog_id = 0;
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        // Send checkpoint with 10 ops, progress should be 0/10.
        request
            .send_checkpoint(Checkpoint::single_bucket("a", 10, None))
            .await;
        sync.wait_for_progress("a", 0, 10).await;

        request.bogus_data_line(&mut oplog_id, "a", 10).await;
        sync.wait_for_progress("a", 10, 10).await;

        request.send_checkpoint_complete(oplog_id, None).await;
        sync.wait_for_status(|s| !s.is_downloading()).await;

        // Emit new data, progress should be 0/2 instead of 10/12.
        request
            .send_checkpoint(Checkpoint::single_bucket("a", 12, None))
            .await;
        sync.wait_for_progress("a", 0, 2).await;
        request.bogus_data_line(&mut oplog_id, "a", 2).await;
        sync.wait_for_progress("a", 2, 2).await;

        request.send_checkpoint_complete(oplog_id, None).await;
        sync.wait_for_status(|s| !s.is_downloading()).await;
    });
}
