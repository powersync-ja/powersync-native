use std::sync::Arc;

use async_trait::async_trait;
use http_client::{HttpClient, h1::H1Client, http_types::Request};
use powersync::{
    BackendConnector, ConnectionPool, PowerSyncCredentials, PowerSyncDatabase, SyncOptions,
    env::PowerSyncEnvironment,
    error::PowerSyncError,
    schema::{Column, Schema, Table},
};
use rusqlite::{Connection, params};
use serde::Deserialize;
use tokio::runtime::Runtime;

pub struct TodoEntry {
    pub id: String,
    pub description: String,
    pub completed: bool,
    pub list_id: String,
}

impl TodoEntry {
    fn schema() -> Table<'static> {
        Table::create(
            "todos",
            vec![
                Column::text("description"),
                Column::integer("completed"),
                Column::text("list_id"),
            ],
            |_| {},
        )
    }

    pub fn fetch_all(conn: &Connection) -> Result<Vec<Self>, PowerSyncError> {
        let mut stmt = conn.prepare("SELECT * FROM lists")?;
        let mut rows = stmt.query(params![])?;
        let mut results = vec![];

        while let Some(row) = rows.next()? {
            results.push(Self {
                id: row.get(0)?,
                description: row.get(1)?,
                completed: row.get(2)?,
                list_id: row.get(3)?,
            });
        }

        Ok(results)
    }
}

pub struct TodoList {
    pub id: String,
    pub name: String,
}

impl TodoList {
    fn schema() -> Table<'static> {
        Table::create("lists", vec![Column::text("name")], |_| {})
    }

    pub fn fetch_all(conn: &Connection) -> Result<Vec<Self>, PowerSyncError> {
        let mut stmt = conn.prepare("SELECT * FROM lists")?;
        let mut rows = stmt.query(params![])?;
        let mut results = vec![];

        while let Some(row) = rows.next()? {
            results.push(Self {
                id: row.get(0)?,
                name: row.get(1)?,
            });
        }

        Ok(results)
    }
}

#[derive(Clone)]
pub struct TodoDatabase {
    pub db: PowerSyncDatabase,
    client: Arc<H1Client>,
}

impl TodoDatabase {
    pub fn new(rt: &Runtime) -> Self {
        let conn = Connection::open_in_memory().expect("should open connection");
        let client = Arc::new(H1Client::new());
        let env = PowerSyncEnvironment::custom(
            client.clone(),
            ConnectionPool::single_connection(conn),
            Box::new(PowerSyncEnvironment::default_timer()),
        );
        let mut schema = Schema::default();
        schema.tables.push(TodoList::schema());
        schema.tables.push(TodoEntry::schema());

        let db = PowerSyncDatabase::new(env, schema);
        rt.spawn({
            let db = db.clone();
            async move { db.download_actor().await }
        });
        rt.spawn({
            let db = db.clone();
            async move { db.upload_actor().await }
        });

        Self { db, client }
    }

    pub async fn connect(&self) {
        self.db.connect(SyncOptions::new(self.clone())).await
    }

    pub async fn disconnect(&self) {
        self.db.disconnect().await;
    }

    async fn fetch_credentials_self_hosted(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
        let request = Request::get("http://localhost:6060/api/auth/token");
        let mut response = self.client.send(request).await?;

        #[derive(Deserialize)]
        struct TokenResponse {
            token: String,
        }

        let token: TokenResponse = response.body_json().await?;
        Ok(PowerSyncCredentials {
            endpoint: "http://localhost:8080".to_string(),
            token: token.token,
        })
    }
}

#[async_trait]
impl BackendConnector for TodoDatabase {
    async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
        self.fetch_credentials_self_hosted().await
    }

    async fn upload_data(&self) -> Result<(), PowerSyncError> {
        todo!("upload data")
    }
}
