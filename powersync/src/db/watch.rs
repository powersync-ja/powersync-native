use event_listener::{Event, EventListener};
use futures_lite::{FutureExt, Stream, ready};
use std::mem::take;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashSet,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Default)]
pub struct TableNotifiers {
    active: Mutex<Vec<Arc<TableListenerState>>>,
}

impl TableNotifiers {
    pub fn notify_updates(&self, updates: &HashSet<String>) {
        let guard = self.active.lock().unwrap();

        for listener in &*guard {
            listener.dispatch_updates(updates);
        }
    }

    /// Returns a [Stream] emitting an empty event every time one of the tables updates.
    pub fn listen(
        self: &Arc<Self>,
        config: ListenerConfiguration,
    ) -> impl Stream<Item = HashSet<String>> + 'static {
        let listener = Arc::new(TableListenerState {
            notifiers: self.clone(),
            notifier: Event::new(),
            config: config.0,
        });

        {
            let mut guard = self.active.lock().unwrap();
            guard.push(listener.clone());
        }

        TableListener {
            state: listener,
            current_waiter: None,
        }
    }
}

struct TableListener {
    state: Arc<TableListenerState>,
    current_waiter: Option<EventListener>,
}

impl Stream for TableListener {
    type Item = HashSet<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        loop {
            if let Some(waiter) = &mut this.current_waiter {
                ready!(waiter.poll(cx));
                this.current_waiter = None;
            };

            if let Some(consumed_updates) = this.state.take_updates() {
                return Poll::Ready(Some(consumed_updates));
            }

            let waiter = this.state.notifier.listen();
            if let Some(consumed_updates) = this.state.take_updates() {
                return Poll::Ready(Some(consumed_updates));
            }

            this.current_waiter = Some(waiter);
        }
    }
}

impl Drop for TableListener {
    fn drop(&mut self) {
        let mut guard = self.state.notifiers.active.lock().unwrap();
        guard.retain(|listener| !Arc::ptr_eq(listener, &self.state));
    }
}

pub struct TableListenerState {
    notifiers: Arc<TableNotifiers>,
    notifier: Event,
    config: ListenerConfigurationInner,
}

pub struct ListenerConfiguration(ListenerConfigurationInner);

enum ListenerConfigurationInner {
    IfMatches(EmitIfMatches),
    All(EmitAll),
}

impl ListenerConfiguration {
    pub fn if_matches(filter: HashSet<String>, emit_initially: bool) -> Self {
        Self(ListenerConfigurationInner::IfMatches(EmitIfMatches {
            filter,
            dirty: AtomicBool::new(emit_initially),
        }))
    }

    pub fn all() -> Self {
        Self(ListenerConfigurationInner::All(EmitAll {
            outstanding_events: Default::default(),
        }))
    }
}

impl ListenerConfigurationInner {
    fn take_updates(&self) -> Option<HashSet<String>> {
        match self {
            Self::IfMatches(matches) => {
                if matches.dirty.fetch_and(false, Ordering::SeqCst) {
                    // Was marked as dirty before, so emit an update
                    Some(Default::default())
                } else {
                    None
                }
            }
            Self::All(all) => {
                let mut state = all.outstanding_events.lock().unwrap();
                if state.is_empty() {
                    return None;
                }

                Some(take(state.deref_mut()))
            }
        }
    }
}

/// Match table updates against a set of tables to filter again. Emit an empty notification every
/// time any filtered table is updated.
struct EmitIfMatches {
    filter: HashSet<String>,
    dirty: AtomicBool,
}

/// Emit all table updates. Updates are buffered into a set if the downstream consumer can't keep
/// up.
struct EmitAll {
    outstanding_events: Mutex<HashSet<String>>,
}

impl TableListenerState {
    /// If there is an outstanding event, consumes and returns it.
    ///
    /// For listeners that emit events when a matched table is updated, the event is always going to
    /// be an empty set (which doesn't require an allocation to create). For a listener that matches
    /// all tables, we need to emit the actual tables that have been updated in the meantime.
    fn take_updates(&self) -> Option<HashSet<String>> {
        self.config.take_updates()
    }

    fn dispatch_updates(&self, updates: &HashSet<String>) {
        match self.config {
            ListenerConfigurationInner::IfMatches(ref filter) => {
                if !filter.filter.is_disjoint(updates)
                    && !filter.dirty.fetch_or(true, Ordering::SeqCst)
                {
                    // Not marked as dirty before, so notify pending listeners, if any.
                    self.notifier.notify(usize::MAX);
                }
            }
            ListenerConfigurationInner::All(ref all) => {
                let mut guard = all.outstanding_events.lock().unwrap();
                let empty_before = guard.is_empty();
                for affected in updates {
                    guard.insert(affected.clone());
                }

                if empty_before {
                    self.notifier.notify(usize::MAX);
                }
            }
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

    use crate::db::watch::{ListenerConfiguration, TableNotifiers};

    #[test]
    fn notify() {
        let notifier = Arc::new(TableNotifiers::default());
        let mut noop = Context::from_waker(Waker::noop());

        let mut set = HashSet::new();
        set.insert("a".to_string());

        let mut stream = notifier.listen(ListenerConfiguration::if_matches(set.clone(), false));
        assert_eq!(stream.poll_next(&mut noop), Poll::Pending);

        notifier.notify_updates(&set);
        assert_eq!(
            stream.poll_next(&mut noop),
            Poll::Ready(Some(Default::default()))
        );
        assert_eq!(stream.poll_next(&mut noop), Poll::Pending);
    }

    #[test]
    fn report_tables() {
        let notifier = Arc::new(TableNotifiers::default());
        let mut noop = Context::from_waker(Waker::noop());

        let mut stream = notifier.listen(ListenerConfiguration::all());
        assert_eq!(stream.poll_next(&mut noop), Poll::Pending);

        notifier.notify_updates(&{
            let mut set = HashSet::new();
            set.insert("foo".to_string());
            set
        });
        notifier.notify_updates(&{
            let mut set = HashSet::new();
            set.insert("bar".to_string());
            set
        });

        let Poll::Ready(Some(event)) = stream.poll_next(&mut noop) else {
            panic!("Expected stream to emit event")
        };
        assert_eq!(event.len(), 2); // Should emit foo and bar events at once
    }

    #[test]
    fn emit_initial() {
        let notifier = Arc::new(TableNotifiers::default());
        let mut noop = Context::from_waker(Waker::noop());

        let mut stream =
            notifier.listen(ListenerConfiguration::if_matches(Default::default(), true));
        assert_eq!(
            stream.poll_next(&mut noop),
            Poll::Ready(Some(Default::default()))
        );
    }

    #[test]
    fn stream_drop() {
        let notifiers = Arc::new(TableNotifiers::default());
        let stream = notifiers.listen(ListenerConfiguration::all());

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
