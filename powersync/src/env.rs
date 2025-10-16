use std::{pin::Pin, sync::Arc, time::Duration};

use http_client::HttpClient;

use crate::{db::core_extension::powersync_init_static, error::PowerSyncError};

use super::db::pool::ConnectionPool;

/// All external dependencies required for the PowerSync SDK.
///
/// This includes the [HttpClient] used to connect to the PowerSync Service, the [ConnectionPool]
/// used to run queries against the local SQLite database and a [Timer] implementing an executor-
/// independent way to delay futures.
pub struct PowerSyncEnvironment {
    /// The [HttpClient] used to connect to the sync service.
    pub(crate) client: Arc<dyn HttpClient>,
    /// The [ConnectionPool] used to obtain connections for queries asynchronously.
    pub(crate) pool: ConnectionPool,
    /// The [Timer] implementation used to delay sync iterations after errors.
    pub(crate) timer: Box<dyn Timer + Send + Sync>,
}

impl PowerSyncEnvironment {
    pub fn custom(
        client: Arc<dyn HttpClient>,
        pool: ConnectionPool,
        timer: Box<dyn Timer + Send + Sync>,
    ) -> Self {
        Self {
            client,
            pool,
            timer,
        }
    }

    /// Calls `sqlite3_auto_extension` with the statically-linked core extension.
    ///
    /// This needs to be invoked before using the PowerSync SDK. It can safely be called multiple
    /// times.
    pub fn powersync_auto_extension() -> Result<(), PowerSyncError> {
        let rc = unsafe { powersync_init_static() };
        match rc {
            0 => Ok(()),
            _ => Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rc),
                Some("Loading PowerSync core extension failed".into()),
            )
            .into()),
        }
    }

    /// A [Timer] implementation based on [async_io::Timer].
    #[cfg(feature = "smol")]
    pub fn async_io_timer() -> impl Timer {
        use async_io::Timer as PlatformTimer;

        struct AsyncIoTimer;
        impl Timer for AsyncIoTimer {
            fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
                use futures_lite::FutureExt;

                async move {
                    PlatformTimer::after(duration).await;
                }
                .boxed()
            }
        }
        AsyncIoTimer
    }

    /// A [Timer] implementation based on [tokio::time::sleep].
    #[cfg(feature = "tokio")]
    pub fn tokio_timer() -> impl Timer {
        use tokio::time::sleep;

        struct TokioTimer;
        impl Timer for TokioTimer {
            fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
                use futures_lite::FutureExt;

                sleep(duration).boxed()
            }
        }
        TokioTimer
    }
}

/// An implementation of a timer as part of an event loop or async runtime hosting the PowerSync
/// SDK.
///
/// Because the native PowerSync SDK is executor-agnostic, it can't use a builtin function to retry
/// sync after a delay to recover from errors. This trait, as part of the [PowerSyncEnvironment],
/// is thus used to schedule the delay.
pub trait Timer {
    /// Returns a future that returns [Poll::Pending] when being polled the first time and schedules
    /// the context's waker to be woken after the specified `duration`.
    fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}
