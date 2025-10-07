use std::time::SystemTime;

use serde::Serialize;
use serde_json::value::RawValue;

use crate::{sync::instruction::ActiveStreamSubscription, util::SerializedJsonObject};

#[derive(Debug, Serialize)]
pub(crate) struct StreamKey {
    pub(crate) name: String,
    pub(crate) parameters: Option<Box<RawValue>>,
}

#[derive(Serialize)]
pub struct StreamDescription<'a> {
    /// The name of the stream as it appears in the stream definition for the PowerSync service.
    pub name: &'a str,
    /// Parameters used to subscribe to the stream, if any.
    ///
    /// The same stream can be subscribed to multiple times with different parameters.
    pub parameters: Option<&'a SerializedJsonObject>,
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
            parameters: self.core.parameters.as_ref().map(|b| &**b),
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

impl<'a> Into<StreamDescription<'a>> for &'a StreamSubscriptionDescription<'a> {
    fn into(self) -> StreamDescription<'a> {
        self.description()
    }
}
