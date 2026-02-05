use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct CallbackListeners<K> {
    raw_listeners: Mutex<Vec<CallbackListener<K>>>,
}

impl<K> CallbackListeners<K> {
    pub fn listen<'a, F: Fn() + Send + Sync + 'a>(
        &'a self,
        key: K,
        callback: F,
    ) -> CallbackListenerHandle<'a, K> {
        let mut raw_listeners = self.raw_listeners.lock().unwrap();
        let listener = Arc::new(callback);
        let listener = unsafe {
            // Safety: We fake the 'static lifetime here, the listener will not get invoked
            // after the CallbackListenerHandle<'a> completes and that guarantees lifetime 'a
            // to be kept alive.
            std::mem::transmute::<
                Arc<dyn Fn() + Send + Sync + 'a>,
                Arc<dyn Fn() + Send + Sync + 'static>,
            >(listener)
        };
        let deactivated = Arc::new(AtomicBool::new(false));

        raw_listeners.push(CallbackListener {
            key,
            listener: listener.clone(),
            deactivated: deactivated.clone(),
        });
        CallbackListenerHandle {
            group: self,
            listener,
            deactivated,
        }
    }

    pub fn notify_listeners(&self, mut filter: impl FnMut(&K) -> bool) {
        let mut raw_listeners = self.raw_listeners.lock().unwrap();

        raw_listeners.retain(|listener| {
            if filter(&listener.key) {
                (listener.listener)();

                // Drop the listener if it has deactivated itself in response to the event.
                !listener.deactivated.load(Ordering::SeqCst)
            } else {
                true
            }
        });
    }

    pub fn notify_all(&self) {
        self.notify_listeners(|_| true)
    }
}

struct CallbackListener<K> {
    key: K,
    listener: Arc<dyn Fn() + Send + Sync + 'static>,
    deactivated: Arc<AtomicBool>,
}

pub struct CallbackListenerHandle<'a, K> {
    group: &'a CallbackListeners<K>,
    listener: Arc<dyn Fn() + Send + Sync + 'a>,
    deactivated: Arc<AtomicBool>,
}

impl<K> Drop for CallbackListenerHandle<'_, K> {
    fn drop(&mut self) {
        self.deactivated.store(true, Ordering::SeqCst);

        if let Ok(mut raw_listeners) = self.group.raw_listeners.try_lock() {
            // Not currently notifying listeners, remove listener from waiters.
            raw_listeners.retain(|listener| !Arc::ptr_eq(&listener.listener, &self.listener))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::util::raw_listener::CallbackListeners;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn notify() {
        let events = AtomicUsize::new(0);
        let listeners = CallbackListeners::default();
        let listener = listeners.listen((), || {
            events.fetch_add(1, Ordering::SeqCst);
        });

        listeners.notify_all();
        assert_eq!(events.load(Ordering::SeqCst), 1);
        listeners.notify_all();
        assert_eq!(events.load(Ordering::SeqCst), 2);

        drop(listener);
        listeners.notify_all();
        assert_eq!(events.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn drop_on_notify() {
        let events = AtomicUsize::new(0);
        let listeners = CallbackListeners::default();
        let listener = Arc::new(Mutex::new(None));

        {
            let mut guard = listener.lock().unwrap();
            let listener = listener.clone();
            let events = &events;
            *guard = Some(listeners.listen((), move || {
                events.store(1, Ordering::SeqCst);

                let mut listener = listener.lock().unwrap();
                // Drop self
                drop(listener.take());
            }));
        }

        listeners.notify_all();
        assert_eq!(events.load(Ordering::SeqCst), 1);

        // Notifying should have dropped the listener
        listeners.notify_all();
        assert_eq!(events.load(Ordering::SeqCst), 1);
    }
}
