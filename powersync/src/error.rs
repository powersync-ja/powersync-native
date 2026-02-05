use std::error::Error;
use std::io;
use std::sync::Arc;
use std::{borrow::Cow, fmt::Display};

use http_client::http_types::StatusCode;
use rusqlite::Error as SqliteError;
use rusqlite::types::FromSqlError;
use thiserror::Error;

/// A [RawPowerSyncError], but boxed.
///
/// We allocate errors in boxes to avoid large [Result] types (given the large size of the
/// [RawPowerSyncError] enum type).
#[derive(Debug, Clone)]
pub struct PowerSyncError {
    inner: Arc<RawPowerSyncError>,
}

impl PowerSyncError {
    pub(crate) fn argument_error(desc: impl Into<Cow<'static, str>>) -> Self {
        RawPowerSyncError::ArgumentError { desc: desc.into() }.into()
    }
}

impl From<SqliteError> for PowerSyncError {
    fn from(value: SqliteError) -> Self {
        RawPowerSyncError::Sqlite { inner: value }.into()
    }
}

impl From<serde_json::Error> for PowerSyncError {
    fn from(value: serde_json::Error) -> Self {
        RawPowerSyncError::JsonConversion { inner: value }.into()
    }
}

impl From<http_client::Error> for PowerSyncError {
    fn from(value: http_client::Error) -> Self {
        RawPowerSyncError::Http { inner: value }.into()
    }
}

impl From<RawPowerSyncError> for PowerSyncError {
    fn from(value: RawPowerSyncError) -> Self {
        PowerSyncError {
            inner: Arc::new(value),
        }
    }
}

impl Display for PowerSyncError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.inner.fmt(f)
    }
}

impl Error for PowerSyncError {}

/// A structured enumeration of possible errors that can occur in the core extension.
#[derive(Error, Debug)]
pub(crate) enum RawPowerSyncError {
    /// An invalid argument was passed to the PowerSync SDK.
    ///
    /// This is used when the schema passed to the database is invalid.
    #[error("invalid argument: {desc}")]
    ArgumentError { desc: Cow<'static, str> },
    /// An inner SQLite call failed.
    #[error("SQLite: {inner}")]
    Sqlite { inner: SqliteError },
    /// Reading a value from SQLite failed.
    #[error("Reading from SQLite: {inner}")]
    FromSql {
        #[from]
        inner: FromSqlError,
    },
    /// The version of the core extension linked into the application is unexpected.
    ///
    /// This should virtually never happen because the native SDK is responsible for linking it.
    #[error("Invalid version of core extension: {actual}")]
    InvalidCoreExtensionVersion { actual: String },
    /// Wraps `serde_json` errors.
    #[error("Internal error while converting JSON: {inner}")]
    JsonConversion { inner: serde_json::Error },
    /// Used when a backend connector returns an invalid URI as a service URL.
    #[error("Invalid PowerSync endpoint: {inner}")]
    InvalidPowerSyncEndpoint { inner: url::ParseError },
    /// HTTP errors while downloading sync lines.
    #[error("HTTP error: {inner}")]
    Http { inner: http_client::Error },
    /// A generic IO error that occurred in the SDK.
    #[error("IO error: {inner}")]
    IO {
        #[from]
        inner: io::Error,
    },
    #[error("The PowerSync service did not accept credentials returned by connector")]
    InvalidCredentials,
    #[error("Unexpected HTTP status code from PowerSync service: {code}")]
    UnexpectedStatusCode { code: StatusCode },
}
