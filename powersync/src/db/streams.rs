use std::{
    cell::Cell,
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use rusqlite::params;

use crate::{
    PowerSyncDatabase, StreamPriority,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::streams::{
        ChangedSyncSubscriptions, StreamDescription, StreamKey, SubscribeToStream,
        SubscriptionChangeRequest,
    },
    util::SerializedJsonObject,
};

/// Tracks all sync streams that currently have at least one active [StreamSubscription].
#[derive(Default)]
pub struct SyncStreamTracker {
    streams: Mutex<HashMap<StreamKey, Weak<StreamSubscriptionGroup>>>,
}

impl SyncStreamTracker {
    pub fn collect_active_streams(&self) -> Vec<StreamKey> {
        let streams = self.streams.lock().unwrap();
        streams.keys().cloned().collect()
    }

    fn reference_stream(
        &self,
        db: &Arc<InnerPowerSyncState>,
        key: &StreamKey,
    ) -> (
        Arc<StreamSubscriptionGroup>,
        Option<ChangedSyncSubscriptions>,
    ) {
        let mut streams = self.streams.lock().unwrap();

        if let Some(existing) = streams.get(key) {
            if let Some(active) = existing.upgrade() {
                return (active, None);
            }
        }

        let entry = Arc::new(StreamSubscriptionGroup {
            db: db.clone(),
            self_: Cell::default(),
            key: key.clone(),
        });

        let weak_entry = Arc::downgrade(&entry);
        entry.self_.set(Some(weak_entry.clone()));
        streams.insert(key.clone(), weak_entry);

        let all = streams.keys().cloned().collect();
        (entry, Some(ChangedSyncSubscriptions(all)))
    }
}

pub struct SyncStream<'a> {
    db: &'a PowerSyncDatabase,
    name: &'a str,
    parameters: Option<Box<SerializedJsonObject>>,
}

impl<'a> SyncStream<'a> {
    pub fn new(
        db: &'a PowerSyncDatabase,
        name: &'a str,
        parameters: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Self {
        Self {
            db,
            name,
            parameters: parameters.map(SerializedJsonObject::from_value),
        }
    }

    async fn subscription_command<'b>(
        &self,
        cmd: &SubscriptionChangeRequest<'b>,
    ) -> Result<(), PowerSyncError> {
        let serialized = serde_json::to_string(cmd)?;

        let mut writer = self.db.writer().await?;
        let writer = writer.transaction()?;

        {
            let mut stmt = writer.prepare_cached("SELECT powersync_control(?, ?)")?;
            let mut rows = stmt.query(params!["subscriptions", serialized])?;

            // Ignore results.
            while let Some(_) = rows.next()? {}
        }

        writer.commit()?;
        Ok(())
    }

    pub async fn subscribe(&self) -> Result<StreamSubscription, PowerSyncError> {
        self.subscribe_with(Default::default()).await
    }

    pub async fn subscribe_with(
        &self,
        options: StreamSubscriptionOptions,
    ) -> Result<StreamSubscription, PowerSyncError> {
        // First, inform the core extension about the new subscription.
        let desc: StreamDescription = self.into();
        self.subscription_command(&SubscriptionChangeRequest::Subscribe(SubscribeToStream {
            stream: desc,
            ttl: options.ttl,
            priority: options.priority,
        }))
        .await?;
        self.db.inner.sync.resolve_offline_sync_status().await;

        let (stream, changed) = self
            .db
            .inner
            .current_streams
            .reference_stream(&self.db.inner, &desc.into());

        if let Some(changed) = changed {
            self.db
                .inner
                .sync
                .handle_subscriptions_changed(changed)
                .await;
        }

        Ok(StreamSubscription { group: stream })
    }

    pub async fn unsubscribe_all(&self) -> Result<(), PowerSyncError> {
        let desc: StreamDescription = self.into();

        {
            let mut streams = self.db.inner.current_streams.streams.lock().unwrap();
            streams.remove(&desc.into());
        }

        self.subscription_command(&SubscriptionChangeRequest::Unsubscribe(desc))
            .await?;
        Ok(())
    }
}

impl<'a> Into<StreamDescription<'a>> for &'a SyncStream<'a> {
    fn into(self) -> StreamDescription<'a> {
        StreamDescription {
            name: self.name,
            parameters: match self.parameters.as_ref() {
                Some(params) => Some(&**params),
                None => None,
            },
        }
    }
}

/// Options customizing a stream subscription, passed to [SyncStream::subscribe_with].
#[derive(Default, Clone, Copy)]

pub struct StreamSubscriptionOptions {
    ttl: Option<Duration>,
    priority: Option<StreamPriority>,
}

impl StreamSubscriptionOptions {
    pub fn with_ttl(&mut self, ttl: Duration) -> &mut Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn with_priority(&mut self, priority: StreamPriority) -> &mut Self {
        self.priority = Some(priority);
        self
    }
}

struct StreamSubscriptionGroup {
    db: Arc<InnerPowerSyncState>,
    key: StreamKey,
    self_: Cell<Option<Weak<Self>>>,
}

unsafe impl Sync for StreamSubscriptionGroup {
    // Safety: self_ is only used on initialization or in drop. In both cases we have an exclusive
    // reference to the group.
}

impl Drop for StreamSubscriptionGroup {
    fn drop(&mut self) {
        let mut tracker = self.db.current_streams.streams.lock().unwrap();
        if let Some(group) = tracker.get(&self.key) {
            if let Some(key) = self.self_.take() {
                if Weak::ptr_eq(&key, group) {
                    tracker.remove(&self.key);
                }
            }
        };
    }
}

pub struct StreamSubscription {
    group: Arc<StreamSubscriptionGroup>,
}

impl StreamSubscription {
    /// Returns a future that completes once this stream has fully synced at least once.
    pub async fn wait_for_first_sync(&self) {
        self.group
            .db
            .wait_for_status(|data| {
                let Some(status) = data.for_stream(self) else {
                    return false;
                };

                status.subscription.has_synced()
            })
            .await
    }

    pub fn unsubscribe(self) {
        drop(self);
    }
}

impl<'a> Into<StreamDescription<'a>> for &'a StreamSubscription {
    fn into(self) -> StreamDescription<'a> {
        StreamDescription {
            name: &self.group.key.name,
            parameters: self.group.key.parameters.as_ref().map(|params| &**params),
        }
    }
}
