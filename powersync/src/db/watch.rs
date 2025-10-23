use crate::util::raw_listener::{CallbackListenerHandle, CallbackListeners};
use event_listener::{Event, EventListener};
use futures_lite::{FutureExt, Stream, ready};
use std::sync::Weak;
use std::{
    collections::HashSet,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Default)]
pub struct TableNotifiers {
    active: Mutex<Vec<Arc<TableListenerState>>>,
    callback_based: CallbackListeners<HashSet<String>>,
}

impl TableNotifiers {
    pub fn notify_updates(&self, updates: &HashSet<String>) {
        let guard = self.active.lock().unwrap();

        for listener in &*guard {
            if listener.tables.intersection(updates).next().is_some() {
                listener.mark_dirty();
            }
        }

        self.callback_based
            .notify_listeners(|filter| filter.intersection(updates).next().is_some());
    }

    /// Invokes [listener] for each reported change on [tables] until the returned
    /// [CallbackListenerHandle] is dropped.
    pub fn install_callback<'a>(
        &'a self,
        tables: HashSet<String>,
        listener: impl Fn() + Send + Sync + 'a,
    ) -> CallbackListenerHandle<'a, HashSet<String>> {
        self.callback_based.listen(tables, listener)
    }

    /// Returns a [Stream] emitting an empty event every time one of the tables updates.
    pub fn listen(
        self: &Arc<Self>,
        emit_initially: bool,
        tables: HashSet<String>,
    ) -> impl Stream<Item = ()> + 'static {
        let listener = Arc::new(TableListenerState {
            notifiers: Arc::downgrade(self),
            tables,
            notifer: Event::new(),
            dirty: AtomicBool::new(emit_initially),
        });

        {
            let mut guard = self.active.lock().unwrap();
            guard.push(listener.clone());
        }

        struct PendingListener {
            state: Arc<TableListenerState>,
            current_waiter: Option<EventListener>,
        }

        impl Stream for PendingListener {
            type Item = ();

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                let this = &mut *self;

                loop {
                    if let Some(waiter) = &mut this.current_waiter {
                        ready!(waiter.poll(cx));
                        this.current_waiter = None;
                    };

                    if this.state.clear_dirty_flag() {
                        return Poll::Ready(Some(()));
                    }

                    let waiter = this.state.notifer.listen();
                    if this.state.clear_dirty_flag() {
                        return Poll::Ready(Some(()));
                    }

                    this.current_waiter = Some(waiter);
                }
            }
        }

        impl Drop for PendingListener {
            fn drop(&mut self) {
                if let Some(notifiers) = self.state.notifiers.upgrade() {
                    let mut guard = notifiers.active.lock().unwrap();
                    guard.retain(|listener| !Arc::ptr_eq(listener, &self.state));
                }
            }
        }

        PendingListener {
            state: listener,
            current_waiter: None,
        }
    }
}

pub struct TableListenerState {
    notifiers: Weak<TableNotifiers>,
    tables: HashSet<String>,
    notifer: Event,
    dirty: AtomicBool,
}

impl TableListenerState {
    /// If the dirty flag is set, clears it and returns `true`. Otherwise returns `false`.
    fn clear_dirty_flag(&self) -> bool {
        self.dirty.fetch_and(false, Ordering::SeqCst)
    }

    fn mark_dirty(&self) {
        if !self.dirty.fetch_or(true, Ordering::SeqCst) {
            // Not marked as dirty before, so notify pending listeners, if any.
            self.notifer.notify(usize::MAX);
        }
    }
}

#[cfg(test)]
mod test {
    use futures_lite::StreamExt;
    use std::{
        collections::HashSet,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use crate::db::watch::TableNotifiers;

    #[test]
    fn notify() {
        let notifier = Arc::new(TableNotifiers::default());
        let mut noop = Context::from_waker(Waker::noop());

        let mut set = HashSet::new();
        set.insert("a".to_string());

        let mut stream = notifier.listen(false, set.clone());
        assert_eq!(stream.poll_next(&mut noop), Poll::Pending);

        notifier.notify_updates(&set);
        assert_eq!(stream.poll_next(&mut noop), Poll::Ready(Some(())));
        assert_eq!(stream.poll_next(&mut noop), Poll::Pending);
    }

    #[test]
    fn emit_initial() {
        let notifier = Arc::new(TableNotifiers::default());
        let mut noop = Context::from_waker(Waker::noop());

        let mut stream = notifier.listen(true, Default::default());
        assert_eq!(stream.poll_next(&mut noop), Poll::Ready(Some(())));
    }

    #[test]
    fn stream_drop() {
        let notifiers = Arc::new(TableNotifiers::default());
        let stream = notifiers.listen(false, Default::default());

        {
            let guard = notifiers.active.lock().unwrap();
            assert_eq!(guard.len(), 1);
        }

        drop(stream);

        {
            let guard = notifiers.active.lock().unwrap();
            assert_eq!(guard.len(), 0);
        }
    }
}
