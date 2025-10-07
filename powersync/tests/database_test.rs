use std::sync::Arc;

use async_oneshot::oneshot;
use futures_lite::{StreamExt, future};
use powersync::error::PowerSyncError;
use powersync_test_utils::{DatabaseTest, UserRow, execute, query_all};
use rusqlite::params;
use serde_json::json;

#[test]
fn link_core_extension() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();

        let reader = db.reader().await.unwrap();
        let mut stmt = reader.prepare("SELECT powersync_rs_version();").unwrap();
        let _: String = stmt.query_one(params![], |row| row.get(0)).unwrap();
    });
}

#[test]
fn test_concurrent_reads() {
    let test = DatabaseTest::new();
    let db = Arc::new(test.test_dir_database());

    future::block_on(async {
        let writer = db.writer().await?;
        writer.execute(
            "INSERT INTO users (id, name, email) VALUES (uuid(), ?, ?)",
            params!["steven", "steven@journeyapps.com"],
        )?;
        Ok::<(), PowerSyncError>(())
    })
    .unwrap();

    // Start a long-running writeTransaction
    let (tx_task, wait_for_insert, mut complete_tx) = {
        let (mut notify_insert, wait_for_insert) = oneshot::<()>();
        let (complete_tx, wait_for_tx_done) = oneshot::<()>();

        let db = db.clone();
        let task = test.ex.spawn(async move {
            // Create another user. External readers should not see this user while the transaction
            // is open.
            let writer = db.writer().await?;
            writer.execute("BEGIN", params![])?;
            writer.execute(
                "INSERT INTO users (id, name, email) VALUES (uuid(), ?, ?)",
                params!["steven", "s@journeyapps.com"],
            )?;
            notify_insert.send(()).unwrap();

            wait_for_tx_done.await.unwrap();
            writer.execute("COMMIT", params![])?;
            Ok::<(), PowerSyncError>(())
        });

        (task, wait_for_insert, complete_tx)
    };

    // Make sure to wait for the item to have been created in the transaction.
    future::block_on(test.ex.run(async move { wait_for_insert.await.unwrap() }));

    // Try and read while the write transaction is busy.
    future::block_on(async {
        let reader = db.reader().await.unwrap();
        let rows = UserRow::read_all(&reader).unwrap();

        // The transaction is not committed yet, we should only read 1 user.
        assert_eq!(rows.len(), 1);
    });

    // Let the transaction complete.
    complete_tx.send(()).unwrap();
    future::block_on(test.ex.run(tx_task)).unwrap();

    future::block_on(async {
        let reader = db.reader().await.unwrap();
        let rows = UserRow::read_all(&reader).unwrap();

        // The transaction is not committed yet, we should only read 1 user.
        assert_eq!(rows.len(), 2);
    });
}

#[test]
fn test_table_updates() {
    let test = DatabaseTest::new();
    let db = Arc::new(test.test_dir_database());

    future::block_on(async move {
        let mut stream = db
            .watch_tables(true, ["users"])
            .then(|_| query_all(&db, "SELECT name FROM users", params![]))
            .boxed_local();

        // Initial query.
        assert_eq!(stream.next().await, Some(json!([])));

        execute(
            &db,
            "INSERT INTO users (id, name) VALUES (uuid(), ?)",
            params!["Test"],
        )
        .await;
        assert_eq!(stream.next().await, Some(json!([{"name": "Test"}])));

        {
            let mut writer = db.writer().await.unwrap();
            let writer = writer.transaction().unwrap();

            writer
                .execute(
                    "INSERT INTO users (id, name) VALUES (uuid(), ?)",
                    params!["Test2"],
                )
                .unwrap();
            writer
                .execute(
                    "INSERT INTO users (id, name) VALUES (uuid(), ?)",
                    params!["Test3"],
                )
                .unwrap();

            writer.commit().unwrap();
        }

        assert_eq!(
            stream.next().await,
            Some(json!([{"name": "Test"},{"name": "Test2"},{"name": "Test3"}]))
        );

        {
            let mut writer = db.writer().await.unwrap();
            let writer = writer.transaction().unwrap();

            writer.execute("DELETE FROM users", params![]).unwrap();
            // Transactions we're rolling back should not impact streams.
        }

        execute(
            &db,
            "INSERT INTO users (id, name) VALUES (uuid(), ?)",
            params!["Test4"],
        )
        .await;

        assert_eq!(
            stream.next().await,
            Some(json!([{"name": "Test"},{"name": "Test2"},{"name": "Test3"},{"name": "Test4"}]))
        );
    });
}
