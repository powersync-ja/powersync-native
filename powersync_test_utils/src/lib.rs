use std::{sync::Arc, vec};

use async_executor::Executor;
use log::LevelFilter;
use powersync::{
    schema::{Column, Schema, Table},
    *,
};
use rusqlite::{Connection, Row, params};
use tempdir::TempDir;

use crate::mock_sync_service::MockSyncService;

pub mod mock_sync_service;
pub mod sync_line;

pub struct DatabaseTest {
    pub dir: TempDir,
    pub http: Arc<MockSyncService>,
    pub ex: Executor<'static>,
}

impl DatabaseTest {
    pub fn new() -> Self {
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

    pub fn in_test_dir(&self) -> PowerSyncEnvironment {
        PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");

        let db = self.dir.path().to_path_buf().join("test.db");
        let pool = ConnectionPool::open(db).expect("should open pool");
        PowerSyncEnvironment::custom(Box::new(self.http.clone().client()), pool)
    }

    pub fn test_dir_database(&self) -> PowerSyncDatabase {
        return PowerSyncDatabase::new(self.in_test_dir(), Self::default_schema());
    }

    pub fn in_memory(&self) -> PowerSyncEnvironment {
        PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");
        let conn = Connection::open_in_memory().expect("should open connection");

        PowerSyncEnvironment::custom(
            Box::new(self.http.clone().client()),
            ConnectionPool::single_connection(conn),
        )
    }

    pub fn in_memory_database(&self) -> PowerSyncDatabase {
        return PowerSyncDatabase::new(self.in_memory(), Self::default_schema());
    }

    pub fn default_schema() -> Schema<'static> {
        let mut schema = Schema::default();
        schema.tables.push(UserRow::table());

        schema
    }
}

#[derive(Clone, Debug)]
pub struct UserRow {
    pub id: String,
    pub name: String,
    pub email: String,
    pub photo_id: Option<String>,
}

impl UserRow {
    pub fn table() -> Table<'static> {
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
