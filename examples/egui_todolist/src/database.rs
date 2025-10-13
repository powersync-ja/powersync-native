use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use futures_lite::StreamExt;
use http_client::{
    HttpClient,
    http_types::{Mime, Request, StatusCode},
    isahc::IsahcClient,
};
use log::warn;
use powersync::{
    BackendConnector, ConnectionPool, PowerSyncCredentials, PowerSyncDatabase, SyncOptions,
    UpdateType,
    env::PowerSyncEnvironment,
    error::PowerSyncError,
    schema::{Column, Schema, Table},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::runtime::Runtime;

pub struct TodoEntry {
    pub id: String,
    pub description: String,
    pub completed: bool,
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

    pub fn fetch_in_list(conn: &Connection, list_id: &str) -> Result<Vec<Self>, PowerSyncError> {
        let mut stmt = conn.prepare("SELECT * FROM todos WHERE list_id = ?")?;
        let mut rows = stmt.query(params![list_id])?;
        let mut results = vec![];

        while let Some(row) = rows.next()? {
            results.push(Self {
                id: row.get(0)?,
                description: row.get(1)?,
                completed: row.get(2)?,
                //list_id: row.get(3)?,
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
    client: Arc<IsahcClient>,
}

impl TodoDatabase {
    pub fn new(rt: &Runtime) -> Self {
        let conn = Connection::open_in_memory().expect("should open connection");
        let client = Arc::new(IsahcClient::new());
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
        let mut transactions = self.db.crud_transactions();
        let mut last_tx = None;

        while let Some(mut tx) = transactions.try_next().await? {
            #[derive(Serialize)]
            struct BackendEntry {
                op: UpdateType,
                table: String,
                id: String,
                data: Option<Map<String, Value>>,
            }

            #[derive(Serialize)]
            struct BackendBatch {
                batch: Vec<BackendEntry>,
            }

            let mut entries = vec![];
            for crud in std::mem::take(&mut tx.crud) {
                entries.push(BackendEntry {
                    op: crud.update_type,
                    table: crud.table,
                    id: crud.id,
                    data: crud.data,
                });
            }

            let mut request = Request::post("http://localhost:6060/api/data");
            let serialized = serde_json::to_string(&BackendBatch { batch: entries })?;
            request.set_body(serialized);
            request.set_content_type(Mime::from_str("application/json").unwrap());

            let mut response = self.client.send(request).await?;
            if response.status() != StatusCode::Ok {
                let body = response.body_string().await?;
                warn!("Received {} from /api/data: {}", response.status(), body);
            }

            last_tx = Some(tx);
        }

        if let Some(tx) = last_tx {
            tx.complete().await?;
        }

        Ok(())
    }
}
