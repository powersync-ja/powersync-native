use futures_lite::{StreamExt, future};
use powersync::PowerSyncDatabase;
use powersync::schema::{
    Column, PendingStatement, PendingStatementValue, RawTable, RawTableSchema, Schema, Table,
    TrackPreviousValues,
};
use powersync_test_utils::{DatabaseTest, execute, query_all};
use rusqlite::params;
use serde_json::{Value, json};

#[test]
fn include_metadata() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), {
            let mut schema = Schema::default();
            schema
                .tables
                .push(Table::create("lists", vec![Column::text("name")], |tbl| {
                    tbl.options.track_metadata = true
                }));
            schema
        });

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO lists (id, name, _metadata) VALUES (uuid(), ?, ?)",
                    params!["entry", "so meta"],
                )
                .unwrap();
        }

        let batch = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(batch.crud[0].metadata, Some("so meta".to_string()));
    })
}

#[test]
fn include_old_values() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), {
            let mut schema = Schema::default();
            schema.tables.push(Table::create(
                "lists",
                vec![Column::text("name"), Column::text("content")],
                |tbl| tbl.options.track_previous_values = Some(TrackPreviousValues::all()),
            ));
            schema
        });

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO lists (id, name, content) VALUES (uuid(), ?, ?)",
                    params!["entry", "content"],
                )
                .unwrap();
            writer.execute("DELETE FROM ps_crud", params![]).unwrap();
            writer
                .execute("UPDATE lists SET name = ?", params!["new name"])
                .unwrap();
        }

        let batch = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_string(&batch.crud[0].previous_values).unwrap(),
            "{\"content\":\"content\",\"name\":\"entry\"}"
        );
    })
}

#[test]
fn include_old_values_with_filter() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), {
            let mut schema = Schema::default();
            schema.tables.push(Table::create(
                "lists",
                vec![Column::text("name"), Column::text("content")],
                |tbl| {
                    tbl.options.track_previous_values = Some(TrackPreviousValues {
                        column_filter: Some(vec!["name".into()]),
                        only_when_changed: false,
                    })
                },
            ));
            schema
        });

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO lists (id, name, content) VALUES (uuid(), ?, ?)",
                    params!["entry", "content"],
                )
                .unwrap();
            writer.execute("DELETE FROM ps_crud", params![]).unwrap();
            writer
                .execute(
                    "UPDATE lists SET name = ?, content = ?",
                    params!["new name", "new content"],
                )
                .unwrap();
        }

        let batch = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_string(&batch.crud[0].previous_values).unwrap(),
            "{\"name\":\"entry\"}"
        );
    })
}

#[test]
fn include_old_values_when_changed() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), {
            let mut schema = Schema::default();
            schema.tables.push(Table::create(
                "lists",
                vec![Column::text("name"), Column::text("content")],
                |tbl| {
                    tbl.options.track_previous_values = Some(TrackPreviousValues {
                        column_filter: None,
                        only_when_changed: true,
                    })
                },
            ));
            schema
        });

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO lists (id, name, content) VALUES (uuid(), ?, ?)",
                    params!["entry", "content"],
                )
                .unwrap();
            writer.execute("DELETE FROM ps_crud", params![]).unwrap();
            writer
                .execute("UPDATE lists SET name = ?", params!["new name"])
                .unwrap();
        }

        let batch = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_string(&batch.crud[0].previous_values).unwrap(),
            "{\"name\":\"entry\"}"
        );
    })
}

#[test]
fn ignore_empty_update() {
    future::block_on(async move {
        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), {
            let mut schema = Schema::default();
            schema.tables.push(Table::create(
                "lists",
                vec![Column::text("name"), Column::text("content")],
                |tbl| tbl.options.ignore_empty_updates = true,
            ));
            schema
        });

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute(
                    "INSERT INTO lists (id, name, content) VALUES (uuid(), ?, ?)",
                    params!["entry", "content"],
                )
                .unwrap();
            writer.execute("DELETE FROM ps_crud", params![]).unwrap();
            writer
                .execute("UPDATE lists SET name = ?", params!["entry"])
                .unwrap();
        }

        let batch = db.next_crud_transaction().await.unwrap();
        assert!(batch.is_none());
    })
}

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

#[test]
fn raw_table_clear() {
    future::block_on(async move {
        let mut schema = Schema::default();
        let mut raw_table = RawTable::with_statements(
            "foo",
            PendingStatement {
                sql: "unused".into(),
                params: vec![],
            },
            PendingStatement {
                sql: "unused".into(),
                params: vec![],
            },
        );
        raw_table.clear = Some("DELETE FROM users".into());
        schema.raw_tables.push(raw_table);

        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), schema);

        {
            let writer = db.writer().await.unwrap();
            writer
                .execute("CREATE TABLE users (name TEXT);", params![])
                .unwrap();
            writer
                .execute("INSERT INTO users (name) VALUES (?);", params!["test"])
                .unwrap();
        }

        assert_eq!(
            query_all(&db, "SELECT * FROM users", params![]).await,
            json!([{"name": "test"}]),
        );

        // Running powersync_clear should delete from users
        {
            let writer = db.writer().await.unwrap();
            let mut stmt = writer.prepare("SELECT powersync_clear(0)").unwrap();
            stmt.query_one(params![], |_| Ok(())).unwrap();
        }

        assert_eq!(
            query_all(&db, "SELECT * FROM users", params![]).await,
            json!([]),
        );
    });
}

#[test]
fn raw_table_crud_trigger() {
    future::block_on(async move {
        let mut schema = Schema::default();

        schema.raw_tables.push(RawTable::with_schema("foo", {
            let mut info = RawTableSchema::new("users");
            info.synced_columns = Some(vec!["name".into()]);
            info
        }));
        let serialized_table = serde_json::to_string(&schema.raw_tables[0]).unwrap();

        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), Default::default());

        {
            let mut writer = db.writer().await.unwrap();
            writer
                .execute(
                    "CREATE TABLE users (id TEXT, name TEXT, local_column INTEGER);",
                    params![],
                )
                .unwrap();

            let mut trigger_stmt = writer
                .prepare("SELECT powersync_create_raw_table_crud_trigger(?,?,?)")
                .unwrap();

            for write in &["INSERT", "UPDATE", "DELETE"] {
                trigger_stmt
                    .query_one(
                        params![serialized_table, format!("users_{write}"), write],
                        |_| Ok(()),
                    )
                    .unwrap();
            }
            drop(trigger_stmt);

            let tx = writer.transaction().unwrap();
            tx.execute_batch(
                "\
INSERT INTO users(id, name, local_column) VALUES ('id', 'name', 42);
UPDATE users SET local_column = local_column + 1; -- should not create a ps_crud entry
DELETE FROM users;
",
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let crud = db.next_crud_transaction().await.unwrap().unwrap();
        assert_eq!(crud.crud.len(), 2);
    });
}

#[test]
fn raw_table_rest_column() {
    future::block_on(async move {
        let mut schema = Schema::default();
        let raw_table = RawTable::with_statements(
            "foo",
            PendingStatement {
                sql: "INSERT INTO users (name) VALUES ?".into(),
                params: vec![PendingStatementValue::Rest],
            },
            PendingStatement {
                sql: "unused".into(),
                params: vec![],
            },
        );
        schema.raw_tables.push(raw_table);

        let test = DatabaseTest::new();
        let db = PowerSyncDatabase::new(test.in_memory(), schema);

        // Just use the database to ensure the schema with the rest column has been installed.
        // We don't test the behavior of that here, the core extension has tests for that. This
        // verifies we generate a JSON structure understood by the core extension.
        db.reader().await.unwrap();
    });
}
