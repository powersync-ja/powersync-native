use std::{fmt::Display, str::FromStr};

use rusqlite::{Connection, params};

use crate::error::{PowerSyncError, RawPowerSyncError};

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct CoreExtensionVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl CoreExtensionVersion {
    /// The minimum version of the core extension supported by the native SDK.
    pub const MINIMUM: Self = Self::new(0, 4, 7);
    pub const MAXIMUM_EXCLUSIVE: Self = Self::new(0, 5, 0);

    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn validate(&self) -> Result<(), PowerSyncError> {
        if self < &Self::MINIMUM || self >= &Self::MAXIMUM_EXCLUSIVE {
            Err(RawPowerSyncError::InvalidCoreExtensionVersion {
                actual: format!("Expected version ^{}, got {}", Self::MINIMUM, self),
            }
            .into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_from_db(conn: &Connection) -> Result<Self, PowerSyncError> {
        let version =
            conn.prepare("SELECT powersync_rs_version()")?
                .query_row(params![], |row| {
                    let value = row.get_ref(0)?;
                    value
                        .as_str()?
                        .parse::<Self>()
                        .map_err(|_| rusqlite::Error::InvalidQuery)
                })?;

        version.validate()?;
        Ok(version)
    }
}

impl FromStr for CoreExtensionVersion {
    type Err = PowerSyncError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Versions are formatted as `x.y.z/hash`.
        let mut components = s.split(['.', '/']).take(3).map(|s| s.parse::<u32>());

        let mut next_component = || {
            components
                .next()
                .ok_or_else(|| RawPowerSyncError::InvalidCoreExtensionVersion {
                    actual: s.to_string(),
                })
                .and_then(|r| {
                    r.map_err(|_| RawPowerSyncError::InvalidCoreExtensionVersion {
                        actual: s.to_string(),
                    })
                })
        };

        let major = next_component()?;
        let minor = next_component()?;
        let patch = next_component()?;

        Ok(Self::new(major, minor, patch))
    }
}

impl Display for CoreExtensionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}
