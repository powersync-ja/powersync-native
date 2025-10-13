use std::sync::Arc;

use futures_lite::{
    FutureExt,
    future::{self, Boxed},
};
use log::warn;
use serde_json::Map;

use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::{
        coordinator::AsyncRequest,
        download::sync_iteration::{DownloadClient, DownloadEvent, StartDownloadIteration},
        instruction::CloseSyncStream,
        streams::ChangedSyncSubscriptions,
    },
};

/// A command sent from a database to the download actor.
pub enum DownloadActorCommand {
    Connect(SyncOptions),
    Disconnect,
    ResolveOfflineSyncStatusIfNotConnected,
    SubscriptionsChanged(ChangedSyncSubscriptions),
    CrudUploadComplete,
}

pub struct DownloadActor {
    state: DownloadActorState,
    commands: async_channel::Receiver<AsyncRequest<DownloadActorCommand>>,
    db: Arc<InnerPowerSyncState>,
    options: Option<SyncOptions>,
}

impl DownloadActor {
    pub fn new(db: Arc<InnerPowerSyncState>) -> Self {
        let commands = db.sync.receive_download_commands();

        Self {
            state: DownloadActorState::Idle,
            commands,
            db,
            options: None,
        }
    }

    pub async fn run(&mut self) {
        while !self.state.is_stopped() {
            self.handle_event().await;
        }
    }

    fn start_iteration(&mut self, options: SyncOptions) {
        let (send_events, receive_event) = async_channel::bounded(1);
        let start = StartDownloadIteration {
            parameters: serde_json::Value::Object(Map::new()),
            schema: self.db.schema.clone(),
            include_defaults: options.include_default_streams,
            active_streams: self.db.current_streams.collect_active_streams(),
        };
        let future = DownloadClient::new(self.db.clone(), receive_event)
            .run(options)
            .boxed();
        send_events
            .try_send(DownloadEvent::Start(start))
            .expect("should send start message");

        self.state = DownloadActorState::Running {
            iteration: future,
            send_events,
        };
    }

    async fn handle_event(&mut self) {
        match &mut self.state {
            DownloadActorState::Idle => {
                // When we're idle, the only thing that can trigger a connection is a connect()
                // call.
                let Ok(mut command) = self.commands.recv().await else {
                    self.state = DownloadActorState::Stopped;
                    return;
                };

                match command.command {
                    DownloadActorCommand::Connect(options) => {
                        self.options = Some(options.clone());
                        self.start_iteration(options);
                        let _ = command.response.send(());
                    }
                    DownloadActorCommand::ResolveOfflineSyncStatusIfNotConnected => {
                        let res = async {
                            let writer = self.db.writer().await?;
                            self.db
                                .status
                                .update(|s| s.resolve_offline_state(&writer))?;

                            Ok::<(), PowerSyncError>(())
                        }
                        .await;
                        if let Err(e) = res {
                            warn!("Could not resolve offline sync state: {e}")
                        }
                    }
                    DownloadActorCommand::Disconnect
                    | DownloadActorCommand::SubscriptionsChanged(_)
                    | DownloadActorCommand::CrudUploadComplete => {
                        // Not connected, nothing to do.
                    }
                }
            }
            DownloadActorState::Running {
                send_events,
                iteration,
            } => {
                // The only thing that triggers a state transition is for the current iteration to
                // end. That can happen due to network errors, but also if disconnect() is called.
                // So we have to listen for both.
                enum Event {
                    ForwardedMessage,
                    SyncIterationComplete(CloseSyncStream),
                    SyncIterationError(PowerSyncError),
                }

                let forwarding_request = async {
                    match self.commands.recv().await {
                        Ok(command) => match command.command {
                            DownloadActorCommand::Connect(_) => {
                                // We're already connected, do nothing.
                                // TODO: Compare options and potentially reconnect
                            }
                            DownloadActorCommand::ResolveOfflineSyncStatusIfNotConnected => {
                                // We're connected, so nothing we'd have to do.
                            }
                            DownloadActorCommand::SubscriptionsChanged(changed) => {
                                let _ = send_events
                                    .send(DownloadEvent::UpdateSubscriptions { keys: changed.0 })
                                    .await;
                            }
                            DownloadActorCommand::CrudUploadComplete => {
                                let _ = send_events.send(DownloadEvent::CompletedUpload).await;
                            }
                            DownloadActorCommand::Disconnect => {
                                let _ = send_events.send(DownloadEvent::Stop).await;
                            }
                        },
                        Err(_) => {
                            // There are no remaining instances of the PowerSync database left,
                            // close the stream.
                            let _ = send_events.send(DownloadEvent::Stop).await;
                        }
                    }

                    Event::ForwardedMessage
                };

                let iteration_done = async {
                    match iteration.await {
                        Ok(e) => Event::SyncIterationComplete(e),
                        Err(e) => {
                            warn!("Sync iteration failed, {e}");
                            Event::SyncIterationError(e)
                        }
                    }
                };

                let event = future::race(forwarding_request, iteration_done).await;
                match event {
                    Event::ForwardedMessage => {
                        // Message was handled, we can go on immediately.
                    }
                    Event::SyncIterationComplete(close) => {
                        let timeout = if close.hide_disconnect {
                            async {}.boxed()
                        } else {
                            let db = self.db.clone();

                            async move { db.sync_iteration_delay().await }.boxed()
                        };

                        self.state = DownloadActorState::WaitingForReconnect { timeout }
                    }
                    Event::SyncIterationError(e) => {
                        self.db.status.update(|status| status.set_download_error(e));
                        let db = self.db.clone();
                        self.state = DownloadActorState::WaitingForReconnect {
                            timeout: async move { db.sync_iteration_delay().await }.boxed(),
                        }
                    }
                }
            }
            DownloadActorState::WaitingForReconnect { timeout } => {
                // Either the timeout expires, in which case we reconnect, or a disconnect is
                // requested.
                enum Event {
                    DisconnectRequested,
                    TimeoutExpired,
                }

                let commands = self.commands.clone();
                let disconnect_requested = async move {
                    Self::wait_for_disconnect_request(&commands).await;
                    Event::DisconnectRequested
                };

                let timeout_expired = async {
                    timeout.await;
                    Event::TimeoutExpired
                };

                match future::race(disconnect_requested, timeout_expired).await {
                    Event::DisconnectRequested => {
                        self.state = DownloadActorState::Idle;
                    }
                    Event::TimeoutExpired => {
                        self.start_iteration(self.options.as_ref().unwrap().clone());
                    }
                }
            }
            DownloadActorState::Stopped => panic!("No further state transitions after stopped"),
        };
    }

    /// Polls on the given channel until we receive a command indicating that the actor should
    /// disconnect.
    async fn wait_for_disconnect_request(
        commands: &async_channel::Receiver<AsyncRequest<DownloadActorCommand>>,
    ) {
        loop {
            match commands.recv().await {
                Ok(command) => match command.command {
                    DownloadActorCommand::Connect(_)
                    | DownloadActorCommand::SubscriptionsChanged(_)
                    | DownloadActorCommand::ResolveOfflineSyncStatusIfNotConnected
                    | DownloadActorCommand::CrudUploadComplete => {
                        continue;
                    }
                    DownloadActorCommand::Disconnect => {
                        return;
                    }
                },
                Err(_) => {
                    // No clients left, treat that as a disconnect request and clean up resources.
                    return;
                }
            }
        }
    }
}

enum DownloadActorState {
    Idle,
    Running {
        send_events: async_channel::Sender<DownloadEvent>,
        iteration: Boxed<Result<CloseSyncStream, PowerSyncError>>,
    },
    WaitingForReconnect {
        timeout: Boxed<()>,
    },
    Stopped,
}

impl DownloadActorState {
    fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped)
    }
}
