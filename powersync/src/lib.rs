mod db;
pub mod env;
mod sync;
mod util;

pub use db::PowerSyncDatabase;
pub use db::crud::{CrudEntry, CrudTransaction};
pub use db::pool::ConnectionPool;
pub use db::streams::StreamSubscription;
pub use db::streams::StreamSubscriptionOptions;
pub use db::streams::SyncStream;
pub use sync::connector::{BackendConnector, PowerSyncCredentials};
pub use sync::options::SyncOptions;
pub use sync::status::SyncStatusData;
pub use sync::stream_priority::StreamPriority;
pub mod error;

pub mod schema {
    pub use super::db::schema::*;
}
