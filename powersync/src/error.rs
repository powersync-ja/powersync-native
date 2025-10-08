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
    pub fn argument_error(desc: impl Into<Cow<'static, str>>) -> Self {
        RawPowerSyncError::ArgumentError { desc: desc.into() }.into()
    }

    pub fn invalid_core_extension_version(actual: String) -> Self {
        RawPowerSyncError::InvalidCoreExtensionVersion { actual }.into()
    }
}

impl From<SqliteError> for PowerSyncError {
    fn from(value: SqliteError) -> Self {
        return RawPowerSyncError::Sqlite { inner: value }.into();
    }
}

impl From<serde_json::Error> for PowerSyncError {
    fn from(value: serde_json::Error) -> Self {
        RawPowerSyncError::JsonConversion { inner: value }.into()
    }
}

#[cfg(feature = "serde_path_to_error")]
impl From<serde_path_to_error::Error<serde_json::Error>> for PowerSyncError {
    fn from(value: serde_path_to_error::Error<serde_json::Error>) -> Self {
        RawPowerSyncError::JsonConversionWithPath { inner: value }.into()
    }
}

impl From<RawPowerSyncError> for PowerSyncError {
    fn from(value: RawPowerSyncError) -> Self {
        return PowerSyncError {
            inner: Arc::new(value),
        };
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
    /// A user (e.g. the one calling a PowerSync function, likely an SDK) has provided invalid
    /// arguments.
    ///
    /// This always indicates an error in how the core extension is used.
    #[error("invalid argument: {desc}")]
    ArgumentError { desc: Cow<'static, str> },
    /// A user (e.g. the one calling a PowerSync function, likely an SDK) has provided invalid
    /// arguments.
    ///
    /// This always indicates an error in how the core extension is used.
    #[error("SQLite: {inner}")]
    Sqlite { inner: SqliteError },
    #[error("Reading from SQLite: {inner}")]
    FromSql {
        #[from]
        inner: FromSqlError,
    },
    #[error("Invalid version of core extension: {actual}")]
    InvalidCoreExtensionVersion { actual: String },
    #[error("Internal error while converting JSON: {inner}")]
    JsonConversion { inner: serde_json::Error },
    #[error("Internal error while converting JSON: {inner}")]
    #[cfg(feature = "serde_path_to_error")]
    JsonConversionWithPath {
        inner: serde_path_to_error::Error<serde_json::Error>,
    },
    #[error("Invalid PowerSync endpoint: {inner}")]
    InvalidPowerSyncEndpoint { inner: url::ParseError },
    #[error("HTTP error: {inner}")]
    Http { inner: http_client::Error },
    #[error("IO error: {inner}")]
    IO {
        #[from]
        inner: io::Error,
    },
    #[error("Internal error: {inner}")]
    Internal { inner: Cow<'static, str> },
    #[error("The PowerSync service did not accept credentials returned by connector")]
    InvalidCredentials,
    #[error("Unexpected HTTP status code from PowerSync service: {code}")]
    UnexpectedStatusCode { code: StatusCode },
}
