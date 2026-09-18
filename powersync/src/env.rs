use super::db::pool::ConnectionPool;
use crate::error::{PowerSyncError, RawPowerSyncError};
use crate::http::HttpClient;
use async_task::Task;
use futures_lite::FutureExt;
use futures_lite::future::Boxed;
use num_traits::FromPrimitive;
use pin_project_lite::pin_project;
use powersync_core::powersync_init_static;
use powersync_sqlite_nostd::ResultCode;
use std::sync::Arc;
use std::{pin::Pin, time::Duration};

/// All external dependencies required for the PowerSync SDK.
///
/// This includes the [HttpClient] used to connect to the PowerSync Service, the [ConnectionPool]
/// used to run queries against the local SQLite database and a [Timer] implementing an executor-
/// independent way to delay futures.
pub struct PowerSyncEnvironment {
    /// The [HttpClient] used to connect to the sync service.
    pub(crate) client: Box<dyn HttpClient>,
    /// The [ConnectionPool] used to obtain connections for queries asynchronously.
    pub(crate) pool: ConnectionPool,
    /// The [Timer] implementation used to delay sync iterations after errors.
    pub(crate) runtime: Arc<dyn AsyncRuntime>,
}

impl PowerSyncEnvironment {
    pub fn custom<C: HttpClient, T: AsyncRuntime>(
        client: C,
        pool: ConnectionPool,
        runtime: T,
    ) -> Self {
        Self {
            client: Box::new(client),
            pool,
            runtime: Arc::new(runtime),
        }
    }

    pub(crate) fn spawn(&self, f: impl Future<Output = ()> + Send + 'static) -> PowerSyncTask {
        self.runtime.spawn(f.boxed())
    }

    /// Calls `sqlite3_auto_extension` with the statically-linked core extension.
    ///
    /// This needs to be invoked before using the PowerSync SDK. It can safely be called multiple
    /// times.
    pub fn powersync_auto_extension() -> Result<(), PowerSyncError> {
        match powersync_init_static() {
            0 => Ok(()),
            code => Err(RawPowerSyncError::RawSqlite {
                code: ResultCode::from_i32(code).unwrap(),
                context: "Loading PowerSync core extension failed".into(),
            }
            .into()),
        }
    }

    /// An [AsyncRuntime] implementation based on `async_task` and [async_io::Timer].
    #[cfg(feature = "smol")]
    pub fn async_io() -> impl Timer {
        use async_io::Timer as PlatformTimer;

        struct AsyncIoRuntime;
        impl AsyncIoRuntime for AsyncIoTimer {
            fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
                use futures_lite::FutureExt;

                async move {
                    PlatformTimer::after(duration).await;
                }
                .boxed()
            }

            fn spawn(&self, task: Boxed<()>) -> PowerSyncTask<()> {
                tokio::spawn(task).into()
            }
        }
        AsyncIoRuntime
    }

    /// An [AsyncRuntime] implementation based on tokio.
    #[cfg(feature = "tokio")]
    pub fn tokio() -> impl AsyncRuntime {
        use tokio::time::sleep;

        struct TokioRuntime;

        impl AsyncRuntime for TokioRuntime {
            fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
                use futures_lite::FutureExt;

                sleep(duration).boxed()
            }

            fn spawn(&self, task: Boxed<()>) -> PowerSyncTask<()> {
                tokio::spawn(task).into()
            }
        }
        TokioRuntime
    }
}

/// An implementation of an asynchronous executor and timer for the PowerSync SDK.
///
/// Because the native PowerSync SDK is executor-agnostic, it can't use a builtin spawn function to
/// start background sync task or to schedule a delay to recover from errors.
///
/// This trait, as part of the [PowerSyncEnvironment], is thus used to schedule the delay.
pub trait AsyncRuntime: Send + Sync + 'static {
    /// Returns a future that returns [Poll::Pending] when being polled the first time and schedules
    /// the context's waker to be woken after the specified `duration`.
    fn delay_once(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn spawn(&self, task: Boxed<()>) -> PowerSyncTask<()>;
}

pub struct PowerSyncTask<T = ()> {
    raw: RawPowerSyncTask<T>,
}

impl<T> PowerSyncTask<T> {
    pub fn cancel(self) {
        match self.raw {
            #[cfg(feature = "tokio")]
            RawPowerSyncTask::Tokio { task } => {
                task.abort();
            }
            RawPowerSyncTask::AsyncTask { task } => {
                // async_task cancels tasks when their handle is dropped.
                drop(task)
            }
        }
    }

    pub async fn cancel_and_join(self) -> Option<T> {
        match self.raw {
            #[cfg(feature = "tokio")]
            RawPowerSyncTask::Tokio { task } => {
                task.abort();

                match task.await {
                    Ok(e) => Some(e),
                    Err(e) => {
                        if e.is_cancelled() {
                            None
                        } else {
                            std::panic::resume_unwind(e.into_panic())
                        }
                    }
                }
            }
            RawPowerSyncTask::AsyncTask { task } => task.cancel().await,
        }
    }

    pub async fn join(self) -> T {
        match self.raw {
            #[cfg(feature = "tokio")]
            RawPowerSyncTask::Tokio { task } => task.await.expect("Task should complete"),
            RawPowerSyncTask::AsyncTask { task } => task.await,
        }
    }
}

// We can't use cfg macros in pin_project
#[cfg(feature = "tokio")]
pin_project! {
    #[project = RawPowerSyncTaskProj]
    enum RawPowerSyncTask<T> {
        Tokio {
            #[pin] task: tokio::task::JoinHandle<T>,
        },
        AsyncTask {
            #[pin] task: Task<T>
        },
    }
}

#[cfg(not(feature = "tokio"))]
pin_project! {
    enum RawPowerSyncTask<T> {
        AsyncTask {
            #[pin] task: Task<T>
        },
    }
}

impl<T> From<Task<T>> for PowerSyncTask<T> {
    fn from(value: Task<T>) -> Self {
        Self {
            raw: RawPowerSyncTask::AsyncTask { task: value },
        }
    }
}

#[cfg(feature = "tokio")]
impl<T> From<tokio::task::JoinHandle<T>> for PowerSyncTask<T> {
    fn from(value: tokio::task::JoinHandle<T>) -> Self {
        Self {
            raw: RawPowerSyncTask::Tokio { task: value },
        }
    }
}
