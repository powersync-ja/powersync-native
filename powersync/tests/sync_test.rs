use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use event_listener::Event;
use futures_lite::{
    FutureExt, StreamExt,
    future::{self, yield_now},
};
use powersync::{
    BackendConnector, CheckpointError, CheckpointMode, PowerSyncCredentials, PowerSyncDatabase,
    RequestsCheckpointMode, StreamPriority, StreamSubscription, StreamSubscriptionOptions,
    SyncOptions, SyncStatusData, error::PowerSyncError,
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
}

impl SyncStreamTest {
    fn new() -> Self {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();

        Self { db, test }
    }

    fn connect(&self) {
        self.connect_options(|_| {});
    }

    fn connect_with_checkpoints(&self) {
        self.connect_options(|options| {
            options
                .with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));
        });
    }

    fn connect_options(&self, configure: impl FnOnce(&mut SyncOptions)) {
        let mut options = SyncOptions::new(TestConnector::default());
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
fn fetching_credentials_does_not_hold_the_download_writer_lease() {
    struct WriterUsingConnector {
        entered: async_channel::Sender<()>,
        release: async_channel::Receiver<()>,
    }

    #[async_trait]
    impl BackendConnector for WriterUsingConnector {
        async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
            self.entered.send(()).await.unwrap();
            self.release.recv().await.unwrap();
            Ok(PowerSyncCredentials {
                endpoint: "https://rust.unit.test.powersync.com/".to_string(),
                token: "token".to_string(),
            })
        }

        async fn upload_data(&self) -> Result<(), PowerSyncError> {
            Ok(())
        }
    }

    let sync = SyncStreamTest::new();
    let (entered_tx, entered_rx) = async_channel::bounded(1);
    let (release_tx, release_rx) = async_channel::bounded(1);
    sync.run(sync.db.connect(SyncOptions::new(WriterUsingConnector {
        entered: entered_tx,
        release: release_rx,
    })));

    sync.run(async {
        entered_rx.recv().await.unwrap();
        let writer = future::poll_once(sync.db.writer()).await;
        assert!(
            writer.is_some(),
            "download retained the writer while awaiting credentials"
        );
        drop(writer);
        release_tx.send(()).await.unwrap();
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

#[test]
fn reconnects_on_failure() {
    let sync = SyncStreamTest::new();
    sync.connect_options(|options| {
        options.with_retry_delay(Duration::from_hours(1));
    });

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        // Send a line causing an error
        request
            .channel
            .send(SyncLine::Custom(json!("invalid sync line")))
            .await
            .unwrap();

        sync.wait_for_status(|s| s.download_error().is_some()).await;
    });

    let task = sync.test.ex.spawn({
        let http = sync.test.http.clone();
        async move { http.receive_requests.recv().await }
    });

    // Should reconnect after the configured delay.
    sync.test.advance_time(Duration::from_mins(30));
    assert!(!task.is_finished());
    sync.test.advance_time(Duration::from_mins(30));
    assert!(task.is_finished());
}

#[test]
fn requests_checkpoints_for_updates() {
    struct TestConnector {
        db: PowerSyncDatabase,
    }

    #[async_trait]
    impl BackendConnector for TestConnector {
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

            tx.complete().await?;
            Ok(())
        }
    }

    let sync = SyncStreamTest::new();
    let mut options = SyncOptions::new(TestConnector {
        db: sync.db.clone(),
    });
    options.with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));
    options.with_retry_delay(Duration::ZERO);
    sync.run(sync.db.connect(options));

    {
        let writer = sync.run(sync.db.writer()).unwrap();
        writer
            .execute(
                "INSERT INTO users (id, name) VALUES (uuid(), ?)",
                params!["local user"],
            )
            .unwrap();
    }

    // The local write should eventually be uploaded.
    sync.run(async {
        while sync
            .test
            .http
            .last_checkpoint_request
            .load(Ordering::SeqCst)
            < 2
        {
            yield_now().await;
        }
    });
}

#[test]
fn reports_download_error_when_seeding_checkpoint_fails() {
    let sync = SyncStreamTest::new();
    sync.test
        .http
        .checkpoint_requests_supported
        .store(false, Ordering::SeqCst);

    sync.connect_options(|options| {
        options.with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));
        options.with_retry_delay(Duration::ZERO);
    });
    sync.run(sync.wait_for_status(|s| s.download_error().is_some()));
}

#[test]
fn reposts_current_checkpoint_until_applied() {
    let sync = SyncStreamTest::new();

    sync.connect_options(|options| {
        options.with_checkpoint_mode(CheckpointMode::Requests(
            RequestsCheckpointMode::with_retry_duration(Duration::from_hours(1)).unwrap(),
        ));
    });

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();

        // Because we didn't include the checkpoint in a sync response, it should keep getting
        // requested.
        for i in 2..=10 {
            sync.test.advance_time(Duration::from_hours(1));
            assert_eq!(
                sync.test
                    .http
                    .amount_of_checkpoint_requests
                    .load(Ordering::SeqCst),
                i
            );
        }

        // Finally, include the checkpoint
        request
            .send_checkpoint(Checkpoint {
                last_op_id: 0,
                write_checkpoint: Some(1),
                buckets: vec![],
                streams: vec![],
            })
            .await;
        request.send_checkpoint_complete(0, None).await;
        sync.wait_for_status(|s| !s.is_downloading()).await;

        // After which no further checkpoints should be requested
        sync.test.advance_time(Duration::from_hours(10));
        assert_eq!(
            sync.test
                .http
                .amount_of_checkpoint_requests
                .load(Ordering::SeqCst),
            10
        );
    });
}

#[test]
fn download_is_retried_on_checkpoint_request() {
    struct Connector {
        db: PowerSyncDatabase,
    }

    #[async_trait]
    impl BackendConnector for Connector {
        async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
            Ok(PowerSyncCredentials {
                endpoint: "https://rust.unit.test.powersync.com/".to_string(),
                token: "token".to_string(),
            })
        }

        async fn upload_data(&self) -> Result<(), PowerSyncError> {
            let tx = self.db.next_crud_transaction().await?;
            if let Some(tx) = tx {
                tx.complete().await?;
            }

            Ok(())
        }
    }

    let sync = SyncStreamTest::new();
    let mut options = SyncOptions::new(Connector {
        db: sync.db.clone(),
    });
    options.with_retry_delay(Duration::from_hours(1));
    options.with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));

    // Destroy the initial connection by sending a bogus line.
    sync.run(async {
        sync.db.connect(options).await;

        let request = sync.test.http.receive_requests.recv().await.unwrap();
        request
            .channel
            .send(SyncLine::Custom(json!("invalid sync line")))
            .await
            .unwrap();

        sync.wait_for_status(|s| s.download_error().is_some()).await;

        // Trigger an upload here. Because the upload needs a seeded sync iteration, we should
        // reconnect immediately instead of after the configured delay.
        {
            let writer = sync.db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO users (id, name) VALUES (uuid(), 'local user')",
                    params![],
                )
                .unwrap();
        }

        sync.test.http.receive_requests.recv().await.unwrap();
    });
}

#[test]
fn can_use_checkpoint_method_from_connector() {
    let sync = SyncStreamTest::new();
    let did_request_checkpoint = Event::new();
    let listener = did_request_checkpoint.listen();

    let mut options = SyncOptions::new(TestConnector {
        post_checkpoint_request: Box::new(move |request_id| {
            assert_eq!(request_id, 1);

            did_request_checkpoint.notify(1);
            return Some(async move { Ok(request_id) }.boxed());
        }),
    });
    options.with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));
    options.with_retry_delay(Duration::ZERO);
    sync.run(sync.db.connect(options));

    sync.run(listener);
}

#[test]
fn reconciles_checkpoint_state_on_token_expiry() {
    let sync = SyncStreamTest::new();
    sync.test
        .http
        .last_checkpoint_request
        .store(100, Ordering::SeqCst);

    sync.connect_options(|options| {
        options.with_retry_delay(Duration::ZERO);
        options.with_checkpoint_mode(CheckpointMode::Requests(RequestsCheckpointMode::default()));
    });
    sync.run(async {
        while sync
            .test
            .http
            .last_checkpoint_request
            .load(Ordering::SeqCst)
            == 0
        {
            yield_now().await;
        }

        let request = sync.test.http.receive_requests.recv().await.unwrap();

        // Simulate what would happen if we suddenly switched users after the old token expired.
        // The client expects a checkpoint of 100, for another user the service wouldn't have that
        // counter yet. The client must request a checkpoint with the existing id, allowing the
        // service to recognize that this device + user combo needs higher checkpoint ids.
        sync.test
            .http
            .last_checkpoint_request
            .store(0, Ordering::SeqCst);

        request.send_keepalive(0).await;
        request.channel.close();

        while sync
            .test
            .http
            .amount_of_checkpoint_requests
            .load(Ordering::SeqCst)
            < 2
        {
            yield_now().await;
        }
        assert_eq!(
            sync.test
                .http
                .last_checkpoint_request
                .load(Ordering::SeqCst),
            100
        );
    });
}

#[test]
fn reads_sync_lines_before_checkpoint_requests_are_ready() {
    let sync = SyncStreamTest::new();
    {
        let mut guard = sync.test.http.before_checkpoint_response.lock().unwrap();
        // Make /sync/checkpoint-request never return
        *guard = Box::new(|| future::pending().boxed());
    }

    sync.connect_with_checkpoints();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        request
            .send_checkpoint(Checkpoint {
                last_op_id: 0,
                write_checkpoint: None,
                buckets: vec![],
                streams: vec![],
            })
            .await;
        sync.wait_for_status(|s| s.is_downloading()).await;
    });
}

#[test]
fn request_checkpoint_fails_when_disconnected() {
    let sync = SyncStreamTest::new();
    let checkpoint = sync.run(sync.db.request_checkpoint());

    assert!(matches!(checkpoint, Err(CheckpointError::Disconnected)));
}

#[test]
fn request_checkpoint_fails_when_connected_with_legacy_mode() {
    let sync = SyncStreamTest::new();
    sync.connect();
    let checkpoint = sync.run(sync.db.request_checkpoint());

    assert!(matches!(checkpoint, Err(CheckpointError::Disabled)));
}

#[test]
fn waits_until_data_is_applied() {
    let sync = SyncStreamTest::new();
    sync.connect_with_checkpoints();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        let requested = sync.db.request_checkpoint().await.unwrap();
        request
            .send_checkpoint(Checkpoint {
                last_op_id: 0,
                write_checkpoint: Some(2),
                buckets: vec![],
                streams: vec![],
            })
            .await;
        assert!(!requested.has_synced());

        request.send_checkpoint_complete(0, None).await;
        requested.wait_for_sync().await.unwrap();
        assert!(requested.has_synced());
    });
}

#[test]
fn throws_on_disconnect_but_can_request_again() {
    let sync = SyncStreamTest::new();
    sync.connect_with_checkpoints();

    sync.run(async {
        sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        let requested = sync.db.request_checkpoint().await.unwrap();

        sync.db.disconnect().await;
        assert!(matches!(
            requested.wait_for_sync().await,
            Err(CheckpointError::Disconnected)
        ));

        sync.connect_with_checkpoints();
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        request
            .send_checkpoint(Checkpoint {
                last_op_id: 0,
                write_checkpoint: Some(2),
                buckets: vec![],
                streams: vec![],
            })
            .await;
        request.send_checkpoint_complete(0, None).await;

        requested.wait_for_sync().await.unwrap();
    });
}

#[test]
fn fails_when_reconnecting_with_legacy_mode() {
    let sync = SyncStreamTest::new();
    sync.connect_with_checkpoints();

    sync.run(async {
        sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        let requested = sync.db.request_checkpoint().await.unwrap();

        sync.db.disconnect().await;
        assert!(matches!(
            requested.wait_for_sync().await,
            Err(CheckpointError::Disconnected)
        ));

        // Reconnecting with the legacy checkpoint mode (the default) should mean that the old
        // request can no longer be fulfilled.
        sync.connect();
        sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        assert!(matches!(
            requested.wait_for_sync().await,
            Err(CheckpointError::Disabled)
        ));
    });
}

#[test]
fn fails_on_sync_errors() {
    let sync = SyncStreamTest::new();
    sync.connect_with_checkpoints();

    sync.run(async {
        let request = sync.test.http.receive_requests.recv().await.unwrap();
        sync.wait_for_status(|s| s.is_connected()).await;

        let requested = sync.db.request_checkpoint().await.unwrap();

        request
            .channel
            .send(SyncLine::Custom(json!("invalid sync line")))
            .await
            .unwrap();

        assert!(matches!(
            requested.wait_for_sync().await,
            Err(CheckpointError::StatusError { .. })
        ));
    });
}
