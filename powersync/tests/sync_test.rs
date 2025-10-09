use async_task::Task;
use futures_lite::{StreamExt, future};
use powersync::{
    PowerSyncDatabase, StreamPriority, StreamSubscription, StreamSubscriptionOptions, SyncOptions,
    SyncStatusData, error::PowerSyncError,
};
use powersync_test_utils::{
    DatabaseTest,
    mock_sync_service::TestConnector,
    sync_line::{BucketChecksum, Checkpoint, StreamDescription, SyncLine},
};
use serde_json::json;

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
            // Call download_actor() synchronously to register the channel.
            let actor = db.download_actor();
            async move { actor.await }
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
        assert_eq!(
            status.for_stream(&a).unwrap().subscription.is_active(),
            false
        );
        assert_eq!(
            status.for_stream(&b).unwrap().subscription.is_active(),
            false
        );
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
