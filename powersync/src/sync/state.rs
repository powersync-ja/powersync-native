use std::sync::Mutex;

use crate::{error::PowerSyncError, sync::checkpoint::CheckpointError};
use async_broadcast::{InactiveReceiver, Sender, broadcast};
use event_listener::{Event, EventListener};

#[derive(Default, Debug, Clone)]
enum CheckpointState {
    Disconnected,
    #[default]
    Pending,
    DidSeed(Result<(), PowerSyncError>),
}

pub struct CheckpointStateSignals {
    state_and_waiter: Mutex<(CheckpointState, Option<Event>)>,
    state_channel: (Sender<CheckpointState>, InactiveReceiver<CheckpointState>),
}

impl Default for CheckpointStateSignals {
    fn default() -> Self {
        let (mut sender, receiver) = broadcast(1);
        sender.set_overflow(true);

        Self {
            state_and_waiter: Default::default(),
            state_channel: (sender, receiver.deactivate()),
        }
    }
}

impl CheckpointStateSignals {
    fn notify_state_channel(&self, state: CheckpointState) {
        let _ = self.state_channel.0.try_broadcast(state);
    }

    fn update_status(&self, state: CheckpointState) {
        let mut waiters = self.state_and_waiter.lock().unwrap();
        waiters.0 = state.clone();
        self.notify_state_channel(state);
    }

    /// Marks the current download iteration as ended, blocking new checkpoint requests until the
    /// seed was performed in the next iteration.
    ///
    /// Returns a receiver that will receive an event when another actor waits for checkpoints,
    /// which allows resuming immediately.
    pub fn download_iteration_ended(&self) -> EventListener {
        // Checkpoint waiters called after this should be able to resume the download iteration.
        let event = Event::default();
        let listener = event.listen();

        let mut waiters = self.state_and_waiter.lock().unwrap();
        *waiters = (CheckpointState::Pending, Some(event));
        self.notify_state_channel(CheckpointState::Pending);

        listener
    }

    /// Marks the sync client as disconnected, failing all outstanding checkpoint requests and
    /// preventing new ones.
    pub fn disconnected(&self) {
        self.update_status(CheckpointState::Disconnected);
    }

    pub fn mark_checkpoints_ready(&self, result: Result<(), PowerSyncError>) {
        self.update_status(CheckpointState::DidSeed(result));
    }

    /// Waits until a download iteration is active and has seeded the checkpoint state, meaning that
    /// checkpoint ids can safely be allocated.
    pub async fn wait_for_checkpoint_requests_ready(
        &self,
        wake_download_loop: bool,
    ) -> Result<(), CheckpointError> {
        let mut state = {
            let waiters = self.state_and_waiter.lock().unwrap();
            waiters.0.clone()
        };
        let mut receiver = self.state_channel.1.activate_cloned();

        loop {
            match state {
                CheckpointState::DidSeed(result) => {
                    break result.map_err(|e| CheckpointError::StatusError { cause: e });
                }
                CheckpointState::Disconnected => break Err(CheckpointError::Disconnected),
                CheckpointState::Pending => {
                    if wake_download_loop {
                        let mut waiters = self.state_and_waiter.lock().unwrap();
                        if let Some(sender) = waiters.1.take() {
                            sender.notify(1);
                        }
                    }
                }
            }

            state = receiver
                .recv_direct()
                .await
                .map_err(|_| CheckpointError::Disconnected)?;
        }
    }
}

#[cfg(test)]
mod test {
    use std::task::Poll;

    use futures_lite::{FutureExt, future::block_on};
    use futures_test::task::noop_context;

    use crate::{error::PowerSyncError, sync::checkpoint::CheckpointError};

    use super::CheckpointStateSignals;

    #[test]
    fn mark_ready_success() {
        let signals = CheckpointStateSignals::default();
        signals.mark_checkpoints_ready(Ok(()));
        block_on(signals.wait_for_checkpoint_requests_ready(false)).unwrap();
    }

    #[test]
    fn mark_ready_failure() {
        let signals = CheckpointStateSignals::default();
        signals.mark_checkpoints_ready(Err(PowerSyncError::argument_error("test")));
        block_on(signals.wait_for_checkpoint_requests_ready(false)).unwrap_err();
    }

    #[test]
    fn mark_disconnected() {
        let signals = CheckpointStateSignals::default();

        signals.disconnected();
        assert!(matches!(
            block_on(signals.wait_for_checkpoint_requests_ready(false)).unwrap_err(),
            CheckpointError::Disconnected
        ))
    }

    #[test]
    fn supports_concurrent_waiters() {
        let signals = CheckpointStateSignals::default();
        let mut a = signals
            .wait_for_checkpoint_requests_ready(true)
            .boxed_local();
        let mut b = signals
            .wait_for_checkpoint_requests_ready(true)
            .boxed_local();

        assert!(matches!(a.poll(&mut noop_context()), Poll::Pending));
        assert!(matches!(b.poll(&mut noop_context()), Poll::Pending));

        signals.mark_checkpoints_ready(Ok(()));
        block_on(a).unwrap();
        block_on(b).unwrap();
    }

    #[test]
    fn waits_for_checkpoint_waiter_is_notified() {
        let signals = CheckpointStateSignals::default();
        let notified = signals.download_iteration_ended();

        let mut future = signals
            .wait_for_checkpoint_requests_ready(true)
            .boxed_local();
        assert!(matches!(future.poll(&mut noop_context()), Poll::Pending));

        block_on(notified);
        signals.mark_checkpoints_ready(Ok(()));
        block_on(future).unwrap();
    }

    #[test]
    fn does_not_notify_waiter_when_wake_download_loop_is_false() {
        let signals = CheckpointStateSignals::default();
        let mut listener = signals.download_iteration_ended().boxed_local();

        assert!(matches!(listener.poll(&mut noop_context()), Poll::Pending));

        let mut future = signals
            .wait_for_checkpoint_requests_ready(false)
            .boxed_local();
        assert!(matches!(future.poll(&mut noop_context()), Poll::Pending));
        assert!(matches!(listener.poll(&mut noop_context()), Poll::Pending));
    }
}
