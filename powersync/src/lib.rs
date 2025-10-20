mod db;
pub mod env;
pub mod error;
mod sync;
mod util;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use db::PowerSyncDatabase;
pub use db::crud::{CrudEntry, CrudTransaction, UpdateType};
pub use db::pool::{ConnectionPool, LeasedConnection};
pub use db::streams::StreamSubscription;
pub use db::streams::StreamSubscriptionOptions;
pub use db::streams::SyncStream;
pub use sync::connector::{BackendConnector, PowerSyncCredentials};
pub use sync::options::SyncOptions;
pub use sync::status::SyncStatusData;
pub use sync::stream_priority::StreamPriority;

pub mod schema {
    pub use super::db::schema::*;
}
