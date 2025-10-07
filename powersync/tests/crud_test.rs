use futures_lite::future;
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
