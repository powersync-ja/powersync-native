use std::pin::Pin;
use std::task::{Context, Poll};

use futures_lite::{FutureExt, Stream, ready};
use pin_project_lite::pin_project;
use rusqlite::params;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::PowerSyncDatabase;
use crate::db::schema::Table;
use crate::error::{PowerSyncError, RawPowerSyncError};

/// All local writes that were made in a specific transaction.
pub struct CrudTransaction<'a> {
    pub(crate) db: &'a PowerSyncDatabase,
    pub last_item_id: i64,
    /// Unique transaction id.
    ///
    /// If null, this contains a list of changes recorded without an explicit transaction
    /// associated.
    pub id: Option<i64>,
    /// List of client-side changes.
    pub crud: Vec<CrudEntry>,
}

impl<'a> CrudTransaction<'a> {
    /// Call to remove the changes from the local queue, once successfully uploaded.
    pub async fn complete(self) -> Result<(), PowerSyncError> {
        self.complete_internal(None).await
    }

    /// Call to remove the changes from the local queue, once successfully uploaded.
    pub async fn complete_with_checkpoint(self, checkpoint: i64) -> Result<(), PowerSyncError> {
        self.complete_internal(Some(checkpoint)).await
    }

    async fn complete_internal(self, checkpoint: Option<i64>) -> Result<(), PowerSyncError> {
        self.db
            .inner
            .complete_crud_items(self.last_item_id, checkpoint)
            .await
    }
}

/// A single client-side change.
pub struct CrudEntry {
    /// Auto-incrementing client-side id.
    ///
    /// Reset whenever the database is re-created.
    pub client_id: i64,
    /// Auto-incrementing transaction id.
    ///
    /// Reset whenever the database is re-created.
    pub transaction_id: i64,
    /// Type of change.
    pub update_type: UpdateType,
    /// Table that contained the change.
    pub table: String,
    /// ID of the changed row.
    pub id: String,
    /// An optional metadata string attached to this entry at the time the write has been issued.
    ///
    /// For tables where [Table::track_metadata] is enabled, a hidden `_metadata` column is added to
    /// this table that can be used during updates to attach a hint to the update thas is preserved
    /// here.
    pub metadata: Option<String>,
    /// Data associated with the change.
    ///
    /// For PUT, this is contains all non-null columns of the row.
    ///
    /// For PATCH, this is contains the columns that changed.
    ///
    /// For DELETE, this is null.
    pub data: Option<Map<String, Value>>,
    /// Old values before an update.
    ///
    /// This is only tracked for tables for which this has been enabled by setting
    /// the [Table::track_previous_values].
    pub previous_values: Option<Map<String, Value>>,
}

impl CrudEntry {
    fn parse(id: i64, tx_id: i64, data: &str) -> Result<Self, PowerSyncError> {
        #[derive(Deserialize)]
        struct CrudData {
            op: UpdateType,
            #[serde(rename = "type")]
            table: String,
            id: String,
            data: Option<Map<String, Value>>,
            metadata: Option<String>,
            old: Option<Map<String, Value>>,
        }

        let data: CrudData = serde_json::from_str(data)?;

        Ok(Self {
            client_id: id,
            transaction_id: tx_id,
            update_type: data.op,
            table: data.table,
            id: data.id,
            metadata: data.metadata,
            data: data.data,
            previous_values: data.old,
        })
    }
}

/// Type of local change.
#[derive(Deserialize)]
pub enum UpdateType {
    /// Insert or replace a row. All non-null columns are included in the data.
    #[serde(rename = "PUT")]
    Put,
    /// Update a row if it exists. All updated columns are included in the data.
    #[serde(rename = "PATCH")]
    Patch,
    /// Delete a row if it exists.
    #[serde(rename = "DELETE")]
    Delete,
}

pub type Boxed<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pin_project! {
    pub(crate) struct CrudTransactionStream<'a> {
        db: &'a PowerSyncDatabase,
        last_item_id: Option<i64>,
        next_tx: Option<Boxed<'a, Result<Option<(i64, CrudTransaction<'a>)>, PowerSyncError>>>
    }
}

impl<'a> CrudTransactionStream<'a> {
    pub fn new(db: &'a PowerSyncDatabase) -> Self {
        Self {
            db,
            last_item_id: None,
            next_tx: None,
        }
    }

    async fn next_transaction(
        db: &'a PowerSyncDatabase,
        last: Option<i64>,
    ) -> Result<Option<(i64, CrudTransaction<'a>)>, PowerSyncError> {
        let last = last.unwrap_or(-1);
        let reader = db.reader().await?;
        let mut stmt = reader.prepare_cached(Self::SQL)?;
        let mut rows = stmt.query(params![last])?;
        let mut crud_entries = vec![];
        let mut last = None::<(i64, i64)>;

        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let tx_id: i64 = row.get(1)?;
            let data = row.get_ref(2)?.as_str().map_err(RawPowerSyncError::from)?;
            last = Some((id, tx_id));

            crud_entries.push(CrudEntry::parse(id, tx_id, data)?);
        }

        Ok(if let Some((id, tx_id)) = last {
            let tx = CrudTransaction {
                db: db,
                id: Some(tx_id),
                last_item_id: id,
                crud: crud_entries,
            };

            Some((id, tx))
        } else {
            None
        })
    }

    const SQL: &'static str = "
WITH RECURSIVE crud_entries AS (
  SELECT id, tx_id, data FROM ps_crud WHERE id = (SELECT min(id) FROM ps_crud WHERE id > ?)
  UNION ALL
  SELECT ps_crud.id, ps_crud.tx_id, ps_crud.data FROM ps_crud
    INNER JOIN crud_entries ON crud_entries.id + 1 = rowid
  WHERE crud_entries.tx_id = ps_crud.tx_id
)
SELECT * FROM crud_entries;
";
}

impl<'a> Stream for CrudTransactionStream<'a> {
    type Item = Result<CrudTransaction<'a>, PowerSyncError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let next_tx = this
            .next_tx
            .get_or_insert_with(|| Self::next_transaction(&this.db, *this.last_item_id).boxed());

        let result = ready!(next_tx.poll(cx));
        *this.next_tx = None;

        return Poll::Ready(match result {
            Ok(None) => None,
            Ok(Some((last_item_id, tx))) => {
                *this.last_item_id = Some(last_item_id);
                Some(Ok(tx))
            }
            Err(e) => Some(Err(e)),
        });
    }
}
