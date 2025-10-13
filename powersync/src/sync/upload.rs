use std::{collections::HashSet, sync::Arc};

use futures_lite::{
    FutureExt, StreamExt,
    future::{self, Boxed},
};
use log::{debug, info, warn};
use rusqlite::{Connection, params};

use crate::{
    BackendConnector,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::{
        MAX_OP_ID, coordinator::AsyncRequest, download::http::write_checkpoint,
        status::UploadStatus,
    },
};

pub enum UploadActorCommand {
    Connect(Arc<dyn BackendConnector>),
    TriggerCrudUpload,
    Disconnect,
}

pub struct UploadActor {
    state: UploadActorState,
    commands: async_channel::Receiver<AsyncRequest<UploadActorCommand>>,
    db: Arc<InnerPowerSyncState>,
}

impl UploadActor {
    pub fn new(db: Arc<InnerPowerSyncState>) -> Self {
        let commands = db.sync.receive_upload_commands();

        Self {
            state: UploadActorState::Idle,
            commands,
            db,
        }
    }

    pub async fn run(&mut self) {
        while !self.state.is_stopped() {
            self.handle_event().await
        }
    }

    fn connected_state(
        db: &Arc<InnerPowerSyncState>,
        connector: Arc<dyn BackendConnector>,
    ) -> ConnectedUploadActor {
        let mut tables = HashSet::new();
        tables.insert("ps_crud".to_string());

        let stream = db.env.pool.update_notifiers().listen(false, tables);
        ConnectedUploadActor {
            connector,
            crud_stream: stream.boxed(),
        }
    }

    async fn state_transition_from_command_while_uploading(
        commands: &async_channel::Receiver<AsyncRequest<UploadActorCommand>>,
        db: &Arc<InnerPowerSyncState>,
    ) -> Option<UploadActorState> {
        match commands.recv().await {
            Ok(command) => match command.command {
                UploadActorCommand::TriggerCrudUpload => {
                    // Already in progress, don't start another.
                    None
                }
                UploadActorCommand::Connect(connector) => {
                    // TODO: Only abort if the connector has changed?
                    Some(UploadActorState::Connected(Self::connected_state(
                        db, connector,
                    )))
                }
                UploadActorCommand::Disconnect => Some(UploadActorState::Idle),
            },
            Err(_) => {
                // There are no remaining instances of the PowerSync database left.
                Some(UploadActorState::Stopped)
            }
        }
    }

    async fn handle_event(&mut self) {
        let mut old_state = std::mem::replace(&mut self.state, UploadActorState::Idle);

        self.state = match old_state {
            UploadActorState::Idle => {
                // Wait for a connect() call
                let Ok(mut command) = self.commands.recv().await else {
                    self.state = UploadActorState::Stopped;
                    return;
                };

                match command.command {
                    UploadActorCommand::Connect(connector) => {
                        let _ = command.response.send(());
                        UploadActorState::Connected(Self::connected_state(&self.db, connector))
                    }
                    UploadActorCommand::TriggerCrudUpload => {
                        // We can't upload because we're not connector
                        old_state
                    }
                    UploadActorCommand::Disconnect => {
                        // Not connected, nothing to do.
                        old_state
                    }
                }
            }
            UploadActorState::Connected(mut state) => {
                enum Transition {
                    StartUpload,
                    Abort(UploadActorState),
                }

                let trigger_by_crud_change = async {
                    state.crud_stream.next().await;
                    Transition::StartUpload
                };

                let trigger_by_command = async {
                    let Ok(mut command) = self.commands.recv().await else {
                        self.state = UploadActorState::Stopped;
                        return Transition::StartUpload;
                    };

                    let _ = command.response.send(());

                    match command.command {
                        UploadActorCommand::Connect(connector) => Transition::Abort(
                            UploadActorState::Connected(Self::connected_state(&self.db, connector)),
                        ),
                        UploadActorCommand::TriggerCrudUpload => Transition::StartUpload,
                        UploadActorCommand::Disconnect => Transition::Abort(UploadActorState::Idle),
                    }
                };

                match future::race(trigger_by_crud_change, trigger_by_command).await {
                    Transition::StartUpload => self.start_upload(state),
                    Transition::Abort(state) => state,
                }
            }
            UploadActorState::RunningUpload { ref mut result } => {
                // A state transition can happen when the current upload is finished or when we
                // receive a disconnect call.

                let request =
                    Self::state_transition_from_command_while_uploading(&self.commands, &self.db);

                let upload_done = async {
                    let (result, state) = result.await;

                    match result {
                        Ok(_) => Some(UploadActorState::Connected(state)),
                        Err(e) => {
                            warn!("CRUD uploads failed, will retry, {e}");
                            self.db
                                .status
                                .update(|s| s.set_upload_state(UploadStatus::Error(e)));
                            let db = self.db.clone();

                            Some(UploadActorState::WaitingForReconnect {
                                timeout: async move {
                                    db.sync_iteration_delay().await;
                                    state
                                }
                                .boxed(),
                            })
                        }
                    }
                };

                match future::race(request, upload_done).await {
                    None => old_state,
                    Some(state) => state,
                }
            }
            UploadActorState::WaitingForReconnect { ref mut timeout } => {
                // Either the timeout expires, in which case we reconnect, or a disconnect is
                // requested.
                let request =
                    Self::state_transition_from_command_while_uploading(&self.commands, &self.db);

                let timeout_expired = async {
                    let state = timeout.await;
                    Some(UploadActorState::Connected(state))
                };

                match future::race(request, timeout_expired).await {
                    None => old_state,
                    Some(state) => state,
                }
            }
            UploadActorState::Stopped => panic!("No further state transitions after stopped"),
            _ => todo!(),
        };
    }

    fn start_upload(&self, state: ConnectedUploadActor) -> UploadActorState {
        let db = self.db.clone();
        UploadActorState::RunningUpload {
            result: async move {
                let mut upload = CrudUpload {
                    connector: state.connector.as_ref(),
                    db,
                };
                let result = upload.run().await;

                (result, state)
            }
            .boxed(),
        }
    }
}

enum UploadActorState {
    Idle,
    Connected(ConnectedUploadActor),
    RunningUpload {
        result: Boxed<(Result<(), PowerSyncError>, ConnectedUploadActor)>,
    },
    WaitingForReconnect {
        timeout: Boxed<ConnectedUploadActor>,
    },
    Stopped,
}

impl UploadActorState {
    fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped)
    }
}

struct ConnectedUploadActor {
    /// The connector to use when uploading changes.
    connector: Arc<dyn BackendConnector>,
    /// A stream emitting changes when the `ps_crud` table is updated locally.
    crud_stream: futures_lite::stream::Boxed<()>,
}

struct CrudUpload<'a> {
    connector: &'a dyn BackendConnector,
    db: Arc<InnerPowerSyncState>,
}

impl<'a> CrudUpload<'a> {
    pub async fn run(&mut self) -> Result<(), PowerSyncError> {
        let mut last_item_id = None::<i64>;

        while let Some(item) = self.oldest_crud_item_id().await? {
            if last_item_id == Some(item) {
                warn!("{}", Self::DUPLICATE_ITEM_WARNING);
                return Err(PowerSyncError::argument_error(
                    "Delaying due to previously encountered CRUD item.",
                ));
            }

            last_item_id = Some(item);
            self.db
                .status
                .update(|data| data.set_upload_state(UploadStatus::Uploading));
            self.connector.upload_data().await?;
        }

        // Uploading is completed, advance write checkpoint.
        if let Some(advance_target) = self.sequence_for_checkpoint().await? {
            let write_checkpoint = self.get_write_checkpoint().await?;
            advance_target.complete(write_checkpoint, &self.db).await?;
        }

        Ok(())
    }

    async fn oldest_crud_item_id(&self) -> Result<Option<i64>, PowerSyncError> {
        let reader = self.db.reader().await?;
        Self::read_oldest_crud_item_id(&reader)
    }

    async fn get_write_checkpoint(&self) -> Result<i64, PowerSyncError> {
        let client_id = {
            let reader = self.db.reader().await?;
            let mut stmt = reader.prepare("SELECT powersync_client_id()")?;
            let id: String = stmt.query_one(params![], |row| row.get(0))?;
            id
        };

        let credentials = self.connector.fetch_credentials().await?;
        write_checkpoint(&self.db, &client_id, credentials).await
    }

    fn read_oldest_crud_item_id(conn: &Connection) -> Result<Option<i64>, PowerSyncError> {
        let mut stmt = conn.prepare("SELECT id FROM ps_crud ORDER BY id LIMIT 1")?;
        let mut rows = stmt.query(params![])?;

        Ok(match rows.next()? {
            None => None,
            Some(row) => Some(row.get(0)?),
        })
    }

    fn ps_crud_sequence(conn: &Connection) -> Result<Option<i64>, PowerSyncError> {
        let mut seq_before = conn.prepare("SELECT seq FROM sqlite_sequence WHERE name = ?")?;
        let mut seq_before = seq_before.query(params!["ps_crud"])?;
        let Some(row) = seq_before.next()? else {
            return Ok(None);
        };

        Ok(row.get(0)?)
    }

    async fn sequence_for_checkpoint(
        &self,
    ) -> Result<Option<PendingCheckpointRequest>, PowerSyncError> {
        let reader = self.db.reader().await?;
        {
            let mut stmt =
                reader.prepare("SELECT 1 FROM ps_buckets WHERE name = ? AND target_op = ?")?;
            let mut rows = stmt.query(params!["$local", MAX_OP_ID])?;
            if rows.next()?.is_none() {
                // Nothing to update.
                return Ok(None);
            }
        }

        let seq_before = Self::ps_crud_sequence(&reader)?;
        Ok(if let Some(seq_before) = seq_before {
            Some(PendingCheckpointRequest {
                crud_sequence: seq_before,
            })
        } else {
            None
        })
    }

    const DUPLICATE_ITEM_WARNING: &'static str = "
Potentially previously uploaded CRUD entries are still present in the upload queue.
Make sure to handle uploads and complete CRUD transactions or batches by calling and awaiting their
`complete()` method.
The next upload iteration will be delayed.";
}

struct PendingCheckpointRequest {
    crud_sequence: i64,
}

impl PendingCheckpointRequest {
    pub async fn complete(
        self,
        op_id: i64,
        db: &InnerPowerSyncState,
    ) -> Result<(), PowerSyncError> {
        info!("Updating target to checkpoint {}", self.crud_sequence);

        let mut writer = db.writer().await?;
        let writer = writer.transaction()?;

        if CrudUpload::read_oldest_crud_item_id(&writer)?.is_some() {
            warn!("ps_crud is not empty, won't advance target");
            return Ok(());
        }

        let seq_after =
            CrudUpload::ps_crud_sequence(&writer)?.expect("sqlite sequence should not be empty");

        if seq_after != self.crud_sequence {
            debug!(
                "Sequence on ps_crud changed while fetching checkpoint. From {} to {}",
                self.crud_sequence, seq_after
            );
            return Ok(());
        }

        writer.execute(
            "UPDATE ps_buckets SET target_op = ? WHERE name = ?",
            params![op_id, "$local"],
        )?;

        writer.commit()?;
        Ok(())
    }
}
