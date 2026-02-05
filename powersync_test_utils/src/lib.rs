use std::{sync::Arc, vec};

use async_executor::Executor;
use log::LevelFilter;
use powersync::{
    env::{PowerSyncEnvironment, Timer},
    schema::{Column, Schema, Table},
    *,
};
use rusqlite::{Connection, Params, Row, params};
use serde_json::{Map, Number, Value};
use tempdir::TempDir;

use crate::mock_sync_service::MockSyncService;

pub mod mock_sync_service;
pub mod sync_line;

pub struct DatabaseTest {
    pub dir: TempDir,
    pub http: Arc<MockSyncService>,
    pub ex: Executor<'static>,
}

impl Default for DatabaseTest {
    fn default() -> Self {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::max())
            .is_test(true)
            .try_init();

        Self {
            dir: TempDir::new("powersync_rust").expect("should create test directory"),
            http: Arc::new(MockSyncService::new()),
            ex: Executor::new(),
        }
    }
}

impl DatabaseTest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_test_dir(&self) -> PowerSyncEnvironment {
        PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");

        let db = self.dir.path().to_path_buf().join("test.db");
        let pool = ConnectionPool::open(db).expect("should open pool");
        self.env(pool)
    }

    pub fn test_dir_database(&self) -> PowerSyncDatabase {
        PowerSyncDatabase::new(self.in_test_dir(), Self::default_schema())
    }

    pub fn in_memory(&self) -> PowerSyncEnvironment {
        PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");
        let conn = Connection::open_in_memory().expect("should open connection");

        self.env(ConnectionPool::single_connection(conn))
    }

    pub fn in_memory_database(&self) -> PowerSyncDatabase {
        PowerSyncDatabase::new(self.in_memory(), Self::default_schema())
    }

    fn env(&self, pool: ConnectionPool) -> PowerSyncEnvironment {
        PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");

        struct DisabledTimer;

        impl Timer for DisabledTimer {
            fn delay_once(
                &self,
                _duration: std::time::Duration,
            ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
                panic!("Tests should not run into a delay")
            }
        }

        PowerSyncEnvironment::custom(
            Arc::new(self.http.clone().client()),
            pool,
            Box::new(DisabledTimer),
        )
    }

    pub fn default_schema() -> Schema {
        let mut schema = Schema::default();
        schema.tables.push(UserRow::table());

        schema
    }
}

/// Runs a query and returns rows as a `serde_json` array.
pub async fn query_all(db: &PowerSyncDatabase, sql: &str, params: impl Params) -> Value {
    let reader = db.reader().await.unwrap();
    let mut stmt = reader.prepare(sql).unwrap();
    let column_names: Vec<String> = stmt.column_names().iter().map(|e| e.to_string()).collect();
    let mut rows = stmt.query(params).unwrap();

    let mut completed_rows = vec![];
    while let Some(row) = rows.next().unwrap() {
        let mut parsed = Map::new();
        for (i, name) in column_names.iter().enumerate() {
            let value = match row.get_ref_unwrap(i) {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(e) => Value::Number(e.into()),
                rusqlite::types::ValueRef::Real(e) => Value::Number(Number::from_f64(e).unwrap()),
                rusqlite::types::ValueRef::Text(items) => {
                    Value::String(std::str::from_utf8(items).unwrap().to_string())
                }
                rusqlite::types::ValueRef::Blob(_) => todo!("Not representable as JSON"),
            };

            parsed.insert(name.clone(), value);
        }

        completed_rows.push(Value::Object(parsed));
    }

    Value::Array(completed_rows)
}

pub async fn execute(db: &PowerSyncDatabase, sql: &str, params: impl Params) {
    let writer = db.writer().await.unwrap();
    writer.execute(sql, params).unwrap();
}

#[derive(Clone, Debug)]
pub struct UserRow {
    pub id: String,
    pub name: String,
    pub email: String,
    pub photo_id: Option<String>,
}

impl UserRow {
    pub fn table() -> Table {
        Table::create(
            "users",
            vec![
                Column::text("name"),
                Column::text("email"),
                Column::text("photo_id"),
            ],
            |_| {},
        )
    }

    pub fn from_row(row: &Row) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            email: row.get("email")?,
            photo_id: row.get("photo_id")?,
        })
    }

    pub fn read_all(conn: &Connection) -> Result<Vec<UserRow>, rusqlite::Error> {
        let mut stmt = conn.prepare("SELECT * FROM users")?;
        let rows = stmt.query_map(params![], Self::from_row)?;

        let mut results = vec![];
        for row in rows {
            results.push(row?);
        }

        Ok(results)
    }
}
