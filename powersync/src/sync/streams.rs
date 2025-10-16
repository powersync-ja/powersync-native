use std::time::{Duration, SystemTime};

use serde::Serialize;
use serde_with::{DurationSeconds, serde_as};

use crate::{
    StreamPriority, sync::instruction::ActiveStreamSubscription, util::SerializedJsonObject,
};

#[derive(Debug, Serialize, Hash, PartialEq, Eq, Clone)]
pub(crate) struct StreamKey {
    pub(crate) name: String,
    pub(crate) parameters: Option<Box<SerializedJsonObject>>,
}

#[derive(Serialize, Clone, Copy)]
pub struct StreamDescription<'a> {
    /// The name of the stream as it appears in the stream definition for the PowerSync service.
    pub name: &'a str,
    /// Parameters used to subscribe to the stream, if any.
    ///
    /// The same stream can be subscribed to multiple times with different parameters.
    #[serde(rename = "params")]
    pub parameters: Option<&'a SerializedJsonObject>,
}

impl From<StreamDescription<'_>> for StreamKey {
    fn from(val: StreamDescription<'_>) -> Self {
        StreamKey {
            name: val.name.to_string(),
            parameters: val.parameters.map(|obj| obj.to_owned()),
        }
    }
}

/// A request sent from a PowerSync SDK to alter the subscriptions managed by this client.
#[derive(Serialize)]
pub enum SubscriptionChangeRequest<'a> {
    #[serde(rename = "subscribe")]
    Subscribe(SubscribeToStream<'a>),

    /// Explicitly unsubscribes from a stream. This corresponds to the `unsubscribeAll()` API in the
    /// SDKs.
    ///
    /// Unsubscribing a single stream subscription happens internally in the SDK by reducing its
    /// refcount. Once no references are remaining, it's no longer listed in
    /// [StartSyncStream.active_streams] which will cause it to get unsubscribed after its TTL.
    #[serde(rename = "unsubscribe")]
    Unsubscribe(StreamDescription<'a>),
}

#[serde_as]
#[derive(Serialize)]
pub struct SubscribeToStream<'a> {
    pub stream: StreamDescription<'a>,
    #[serde_as(as = "Option<DurationSeconds>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<StreamPriority>,
}

/// Information about a subscribed sync stream.
///
/// This includes the [StreamDescription] along with information about the current sync status.
pub struct StreamSubscriptionDescription<'a> {
    pub(crate) core: &'a ActiveStreamSubscription,
}

impl<'a> StreamSubscriptionDescription<'a> {
    /// The [StreamDescription] representing this stream.
    pub fn description(&'_ self) -> StreamDescription<'_> {
        StreamDescription {
            name: &self.core.name,
            parameters: self.core.parameters.as_deref(),
        }
    }

    /// Whether this stream is active, meaning that the subscription has been acknowledged by the
    /// sync service.
    pub fn is_active(&self) -> bool {
        self.core.active
    }

    /// Whether this stream subscription is included by default, regardless of whether the stream
    /// has explicitly been subscribed to or not.
    ///
    /// Default streams are created by applying `auto_subscribe: true` in their definition on the
    /// sync service.
    ///
    /// It's possible for both [Self::is_default] and [Self::has_explicit_subscription] to be true
    /// at the same time. This happens when a default stream was subscribed explicitly.
    pub fn is_default(&self) -> bool {
        self.core.is_default
    }

    /// Whether this stream has been subcribed to explicitly.
    ///
    /// It's possible for both [Self::is_default] and [Self::has_explicit_subscription] to be true
    /// at the same time. This happens when a default stream was subscribed explicitly.
    pub fn has_explicit_subscription(&self) -> bool {
        self.core.has_explicit_subscription
    }

    ///For sync streams that have a time-to-live, the current time at which the stream would expire
    /// if not subscribed to again.
    pub fn expires_at(&self) -> Option<SystemTime> {
        Some(self.core.expires_at?.into())
    }

    /// Whether this stream subscription has been synced at least once.
    pub fn has_synced(&self) -> bool {
        self.core.last_synced_at.is_some()
    }

    /// If [Self::has_synced] is true, the last time data from this stream has been synced.
    pub fn last_synced_at(&self) -> Option<SystemTime> {
        Some(self.core.last_synced_at?.into())
    }
}

impl<'a> From<&'a StreamSubscriptionDescription<'a>> for StreamDescription<'a> {
    fn from(val: &'a StreamSubscriptionDescription<'a>) -> Self {
        val.description()
    }
}
pub struct ChangedSyncSubscriptions(pub Vec<StreamKey>);
