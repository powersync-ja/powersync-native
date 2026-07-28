use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_task::Task;
use async_trait::async_trait;
use event_listener::Event;
use futures_lite::{StreamExt, future};
use powersync::{
    BackendConnector, PowerSyncCredentials, PowerSyncDatabase, StreamPriority, StreamSubscription,
    StreamSubscriptionOptions, SyncOptions, SyncStatusData, error::PowerSyncError,
};
use powersync_test_utils::{
    DatabaseTest,
    mock_sync_service::TestConnector,
    sync_line::{Checkpoint, SyncLine},
};
use rusqlite::params;
use serde_json::json;
use thiserror::Error;

struct SyncStreamTest {
    test: DatabaseTest,
    db: PowerSyncDatabase,
    tasks: Vec<Task<()>>,
}

impl SyncStreamTest {
    fn new() -> Self {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();

        let tasks = db.async_tasks().spawn_with(|f| test.ex.spawn(f));
        Self { db, test, tasks }
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

            progress.total == total && progress.downloaded == completed
        })
        .await
    }
}

#[test]
fn dropping_database_completes_actors() {
    let sync = SyncStreamTest::new();
    drop(sync.db);

    future::block_on(sync.test.ex.run(async move {
        for task in sync.tasks {
            task.await;
        }
    }));
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
fn subscribes_with_streams() {
    let sync = SyncStreamTest::new();
    let (a, b) = sync
        .run(async {
            let a = sync
                .db
                .sync_stream("foo", Some(&json!({"foo": "a"})))
                .subscribe()
                .await?;
            let b = sync
                .db
                .sync_stream("foo", Some(&json!({"foo": "b"})))
                .subscribe_with(
                    *StreamSubscriptionOptions::default().with_priority(StreamPriority::ONE),
                )
                .await?;
            Ok::<(StreamSubscription, StreamSubscription), PowerSyncError>((a, b))
        })
        .unwrap();
    sync.connect();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        let streams = request
            .request_data
            .get("streams")
            .unwrap()
            .get("subscriptions")
            .unwrap();

        assert_eq!(
            streams,
            &json!([
                {"stream": "foo", "parameters": {"foo": "a"}, "override_priority": null},
                {"stream": "foo", "parameters": {"foo": "b"}, "override_priority": 1},
            ])
        );

        let status = sync.db.status();
        assert!(!status.for_stream(&a).unwrap().subscription.is_active(),);
        assert!(!status.for_stream(&b).unwrap().subscription.is_active(),);
        let mut next_status = sync.db.watch_status().skip(1);
        let status = next_status.next();
        request
            .channel
            .send(SyncLine::Custom(json!({"checkpoint": {
                "last_op_id": "0",
                "streams": [
                    {"name": "foo", "is_default": false, "errors": []}
                ],
                "buckets": [
                    {"bucket": "a", "priority": 3, "checksum": 0, "subscriptions": [
                        {"sub": 0}
                    ]},
                    {"bucket": "b", "priority": 1, "checksum": 0, "subscriptions": [
                        {"sub": 1}
                    ]}
                ],
            }})))
            .await
            .unwrap();

        // Subscriptions should be active now, but not marked as synced.
        let status = status.await.unwrap();
        for subscription in [&a, &b] {
            let status = status.for_stream(subscription).unwrap();
            assert!(status.subscription.is_active());
            assert!(status.subscription.last_synced_at().is_none());
            assert!(status.subscription.has_explicit_subscription());
        }

        // Mark stream a as synced.
        request
            .send_checkpoint_complete(0, Some(StreamPriority::ONE))
            .await;
        let status = next_status.next().await.unwrap();
        assert!(
            status
                .for_stream(&a)
                .unwrap()
                .subscription
                .last_synced_at()
                .is_none()
        );
        assert!(
            status
                .for_stream(&b)
                .unwrap()
                .subscription
                .last_synced_at()
                .is_some()
        );
        b.wait_for_first_sync().await;

        request.send_checkpoint_complete(0, None).await;
        a.wait_for_first_sync().await;
    });
}

#[test]
fn reports_default_streams() {
    let sync = SyncStreamTest::new();
    sync.connect();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        let mut next_status = sync.db.watch_status().skip(1);

        request
            .send_checkpoint(Checkpoint::single_bucket("default_stream", 0, None))
            .await;
        let status = next_status.next().await.unwrap();
        let mut streams = status.streams();
        let stream = streams.next().unwrap();
        assert_eq!(stream.subscription.description().name, "default_stream");
        assert!(stream.subscription.description().parameters.is_none());
        assert!(stream.subscription.is_default());
        assert!(!stream.subscription.has_explicit_subscription());
    });
}

#[test]
fn changes_subscriptions_dynamically() {
    let sync = SyncStreamTest::new();
    sync.connect();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        let subscription = sync.db.sync_stream("a", None).subscribe().await.unwrap();

        // Adding the subscription should reconnect.
        request.channel.closed().await;
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        // The second request should include the new stream.
        let streams = request
            .request_data
            .get("streams")
            .unwrap()
            .get("subscriptions")
            .unwrap();
        assert_eq!(
            streams,
            &json!([
                {"stream": "a", "parameters": null, "override_priority": null},
            ])
        );

        // Unsubscribing should not do anything due to TTL, but it's hard to test that.
        subscription.unsubscribe();
    });
}

#[test]
fn subscriptions_update_while_offline() {
    let sync = SyncStreamTest::new();
    sync.run(async {
        let db = sync.db.clone();
        // Skip the initial status to get updates.
        let next_status = sync
            .test
            .ex
            .spawn(async move { db.watch_status().next().await.unwrap() });

        // Subscribing while offline should add the stream to the subscriptions reported in the
        // status.
        let subscription = sync.db.sync_stream("foo", None).subscribe().await.unwrap();
        let status = next_status.await;
        assert!(status.for_stream(&subscription).is_some());
    });
}

#[test]
fn unsubscribe_all() {
    let sync = SyncStreamTest::new();
    sync.run(async {
        let a = sync.db.sync_stream("a", None).subscribe().await.unwrap();
        sync.db
            .sync_stream("a", None)
            .unsubscribe_all()
            .await
            .unwrap();

        // Despite being active, it should not be requested.
        sync.connect();

        let request = sync.test.http.receive_requests.recv().await.unwrap();
        let streams = request
            .request_data
            .get("streams")
            .unwrap()
            .get("subscriptions")
            .unwrap();

        assert_eq!(streams, &json!([]));
        a.unsubscribe();
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

#[test]
fn upload_retry() {
    struct FailOnFirstUpload {
        db: PowerSyncDatabase,
        counter: Arc<AtomicUsize>,
        completed_second: Arc<Event>,
    }

    #[derive(Error, Debug)]
    #[error("Deliberate failure on first upload")]
    struct FirstUploadFailure;

    #[async_trait]
    impl BackendConnector for FailOnFirstUpload {
        async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
            Ok(PowerSyncCredentials {
                endpoint: "https://rust.unit.test.powersync.com/".to_string(),
                token: "token".to_string(),
            })
        }

        async fn upload_data(&self) -> Result<(), PowerSyncError> {
            let Some(tx) = self.db.next_crud_transaction().await? else {
                return Ok(());
            };

            let old_count = self.counter.fetch_add(1, Ordering::SeqCst);
            if old_count == 0 {
                return Err(PowerSyncError::upload_error(FirstUploadFailure));
            }

            tx.complete().await?;
            self.completed_second.notify(usize::MAX);
            Ok(())
        }
    }

    let sync = SyncStreamTest::new();
    let upload_counter = Arc::new(AtomicUsize::default());
    let event = Arc::new(Event::new());
    let mut options = SyncOptions::new(FailOnFirstUpload {
        db: sync.db.clone(),
        counter: upload_counter.clone(),
        completed_second: event.clone(),
    });
    options.with_retry_delay(Duration::ZERO); // We can't use timers in tests
    sync.run(sync.db.connect(options));

    sync.run(async {
        sync.wait_for_status(|s| s.is_connected()).await;

        // Trigger a crud upload.
        {
            let writer = sync.db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO users (id, name) VALUES (uuid(), 'local user')",
                    params![],
                )
                .unwrap();
        }

        // Wait for the second upload to finish.
        loop {
            let listener = event.listen();
            if upload_counter.load(Ordering::SeqCst) == 2 {
                break;
            };

            listener.await
        }

        sync.wait_for_status(|s| s.upload_error().is_none() && !s.is_uploading())
            .await;

        assert!(sync.db.next_crud_transaction().await.unwrap().is_none());
    });
}

#[test]
fn reports_correct_times() {
    let sync = SyncStreamTest::new();
    sync.connect();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        request
            .send_checkpoint(Checkpoint::single_bucket("a", 0, None))
            .await;
        request.send_checkpoint_complete(0, None).await;
        sync.wait_for_status(|s| !s.is_downloading()).await;

        let stream = sync.db.sync_stream("a", None);
        let status = sync.db.status();
        let status = status
            .for_stream(&stream)
            .expect("should have stream status");
        let last_synced_at = status
            .subscription
            .last_synced_at()
            .expect("should have last synced at");
        let delta = SystemTime::now().duration_since(last_synced_at).unwrap();
        assert!(delta < Duration::from_secs(5));
    });
}
