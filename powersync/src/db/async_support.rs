use futures_lite::future::Boxed;
#[cfg(feature = "tokio")]
use tokio::{runtime::Runtime, spawn, task::JoinHandle};

/// A collection of tasks, represented as pending futures, that should be started concurrently for
/// PowerSync to work.
pub struct AsyncDatabaseTasks {
    download: Boxed<()>,
    upload: Boxed<()>,
}

impl AsyncDatabaseTasks {
    pub(crate) fn new(download: Boxed<()>, upload: Boxed<()>) -> Self {
        Self { download, upload }
    }

    /// Spawns pending futures.
    ///
    /// This invokes the `spawner` function with each future that the PowerSync SDK would like to
    /// run asynchronously. The function is responsible for continuously polling the futures.
    /// The futures will complete once all [PowerSyncDatabase]s have been closed, so spawned tasks
    /// don't have to be cancelled or dropped manually.
    pub fn spawn_with<T>(self, mut spawner: impl FnMut(Boxed<()>) -> T) -> Vec<T> {
        vec![spawner(self.download), spawner(self.upload)]
    }

    /// Spawns pending futures as tokio tasks on the given [Runtime].
    #[cfg(feature = "tokio")]
    pub fn spawn_with_tokio_runtime(self, runtime: &Runtime) -> Vec<JoinHandle<()>> {
        self.spawn_with(|f| runtime.spawn(f))
    }

    /// Spawns pending futures as tokio tasks on the current [Runtime].
    #[cfg(feature = "tokio")]
    pub fn spawn_with_tokio(self) -> Vec<JoinHandle<()>> {
        self.spawn_with(|f| spawn(f))
    }
}
