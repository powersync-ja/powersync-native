use futures_lite::{StreamExt, future};
use powersync::PowerSyncDatabase;
use powersync_test_utils::{DatabaseTest, execute, query_all};
use rusqlite::params;
use serde_json::{Value, json};

#[test]
fn insert() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();

        assert_eq!(
            query_all(&db, "SELECT * FROM ps_crud", params![]).await,
            Value::Array(vec![])
        );
        execute(
            &db,
            "INSERT INTO users (id, name) VALUES (?, ?)",
            params!["test", "name"],
        )
        .await;
        assert_eq!(
            query_all(&db, "SELECT data FROM ps_crud", params![]).await,
            json!([
                {"data": r#"{"op":"PUT","id":"test","type":"users","data":{"name":"name"}}"#}
            ])
        );

        let Some(tx) = db.next_crud_transaction().await.unwrap() else {
            panic!("Expected crud transaction");
        };
        assert_eq!(tx.id, Some(1));
        assert_eq!(tx.crud.len(), 1);
    });
}

#[test]
fn crud_transactions() {
    async fn create_transaction(db: &PowerSyncDatabase, amount: usize) {
        let mut writer = db.writer().await.unwrap();
        let writer = writer.transaction().unwrap();

        for _ in 0..amount {
            writer
                .execute("INSERT INTO users (id) VALUES (uuid())", params![])
                .unwrap();
        }

        writer.commit().unwrap();
    }

    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = test.in_memory_database();

        create_transaction(&db, 5).await;
        create_transaction(&db, 10).await;
        create_transaction(&db, 15).await;

        let mut iterator = db.crud_transactions();
        let mut last_tx = None;
        let mut batch = vec![];
        while let Some(mut tx) = iterator.try_next().await.unwrap() {
            batch.append(&mut tx.crud);
            last_tx = Some(tx);

            if batch.len() > 10 {
                break;
            }
        }

        assert_eq!(batch.len(), 15);
        last_tx.unwrap().complete().await.unwrap();

        let remaining = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(remaining.crud.len(), 15);
    });
}
