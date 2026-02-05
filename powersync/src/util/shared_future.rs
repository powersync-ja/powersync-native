use std::cell::UnsafeCell;
use std::sync::atomic::Ordering;
use std::{mem::MaybeUninit, sync::Arc};

use atomic_enum::atomic_enum;
use event_listener::Event;
use scopeguard::ScopeGuard;

/// A future that can be initialized with an async block through [SharedFuture::run].
///
/// [SharedFuture::run] will invoke an async initialization callback once, other invocations will
/// wait on that future to complete.
pub struct SharedFuture<T> {
    data: Arc<SharedFutureData<T>>,
}

struct SharedFutureData<T> {
    state: AtomicSharedFutureEnum,
    notify: Event,
    result: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Sync> Sync for SharedFutureData<T> {}

impl<T> SharedFuture<T> {
    pub fn new() -> Self {
        Self {
            data: Arc::new(SharedFutureData::idle()),
        }
    }

    pub async fn run<F: Future<Output = T>>(&self, f: impl FnOnce() -> F) -> &T {
        let data = &self.data;
        loop {
            let result = data.state.compare_exchange_weak(
                SharedFutureEnum::Idle,
                SharedFutureEnum::RunningFuture,
                Ordering::SeqCst,
                Ordering::Relaxed,
            );

            match result {
                Ok(_) => {
                    // We've just transitioned the state from Idle to RunningFuture, so this
                    // invocation is responsible for initializing the value.
                    let guard = scopeguard::guard(data.clone(), |state| {
                        // If e.g. the future is dropped without completing, we need to run the
                        // initializer again next time.
                        state.state.store(SharedFutureEnum::Idle, Ordering::SeqCst);
                        state.notify.notify(usize::MAX);
                    });

                    let future = async move {
                        let data = &*guard;
                        let res = MaybeUninit::new(f().await);
                        unsafe {
                            // Safety: This write is guarded by the shared future being in state
                            // RunningFuture.
                            data.result.get().write(res)
                        };

                        data.state
                            .store(SharedFutureEnum::Completed, Ordering::SeqCst);
                        data.notify.notify(usize::MAX);

                        // Defuse the guard since we've just completed the future.
                        ScopeGuard::into_inner(guard);
                    };

                    future.await;
                    break unsafe {
                        // Safety: We've just completed the future.
                        data.assume_completed()
                    };
                }
                Err(SharedFutureEnum::Idle) => continue,
                Err(SharedFutureEnum::RunningFuture) => {
                    let listener = data.notify.listen();
                    if data.state.load(Ordering::SeqCst) == SharedFutureEnum::RunningFuture {
                        // The future is already running, wait for that to complete before checking
                        // again.
                        listener.await;
                    }
                }
                Err(SharedFutureEnum::Completed) => break unsafe { data.assume_completed() },
            }
        }
    }
}

impl<T> SharedFutureData<T> {
    fn idle() -> Self {
        Self {
            state: AtomicSharedFutureEnum::new(SharedFutureEnum::Idle),
            notify: Event::new(),
            result: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    unsafe fn assume_completed(&self) -> &T {
        let result = unsafe {
            // Safety: Must only be called when the shared future is fully completed, at which point
            // this value is no longer written to.
            self.result.get().as_ref().unwrap_unchecked()
        };
        unsafe {
            // Safety: Must only be called when the shared future is fully completed.
            result.assume_init_ref()
        }
    }
}

impl<T> Drop for SharedFutureData<T> {
    fn drop(&mut self) {
        if self.state.get() == SharedFutureEnum::Completed {
            let data = self.result.get_mut();
            unsafe { data.assume_init_drop() };
        }
    }
}

#[atomic_enum]
#[derive(PartialEq, Eq)]
enum SharedFutureEnum {
    Idle = 0,
    RunningFuture = 1,
    Completed = 2,
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, pin::pin, task::Poll};

    use futures_lite::future;
    use futures_test::task::noop_context;

    use crate::util::SharedFuture;

    #[test]
    fn initializes_once() {
        let mut did_call_second = false;
        let shared = SharedFuture::<usize>::new();
        let first = shared.run(|| async { 0 });
        let second = shared.run(|| async {
            did_call_second = true;
            1
        });

        assert_eq!(*future::block_on(first), 0);
        assert_eq!(*future::block_on(second), 0);
        assert!(!did_call_second);
    }

    #[test]
    fn dropped_initializer() {
        let mut did_call_second = false;
        let shared = SharedFuture::<usize>::new();
        let first = shared.run(|| async {
            future::yield_now().await;
            0
        });
        let second = shared.run(|| async {
            did_call_second = true;
            1
        });

        let mut ctx = noop_context();
        {
            let first = pin!(first);
            assert_eq!(first.poll(&mut ctx), Poll::Pending);
            // Drop the first future.
        }

        assert_eq!(*future::block_on(second), 1);
        assert!(did_call_second);
    }

    #[test]
    fn drops_value() {
        let dropped = RefCell::new(false);
        struct RaiseOnDrop<'a>(&'a RefCell<bool>);
        impl Drop for RaiseOnDrop<'_> {
            fn drop(&mut self) {
                self.0.replace(true);
            }
        }

        let shared = SharedFuture::<RaiseOnDrop>::new();
        future::block_on(shared.run(|| async { RaiseOnDrop(&dropped) }));

        assert!(!*dropped.borrow());
        drop(shared);
        assert!(*dropped.borrow());
    }
}
