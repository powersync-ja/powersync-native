use std::sync::Arc;

use futures_lite::FutureExt;
use futures_lite::future::Boxed;
use futures_lite::{StreamExt, future, stream::Boxed as BoxedStream};
use log::{debug, info, trace, warn};
use powersync_sqlite_nostd::{Destructor, ManagedStmt, ResultCode};
use serde::Serialize;
use serde_json::Map;
use serde_json::value::RawValue;

use crate::BackendConnector;
use crate::db::connection::{SqliteConnection, TransactionGuard};
use crate::schema::SchemaOrCustom;
use crate::sync::coordinator::SyncChannels;
use crate::sync::download::http::checkpoint_request;
use crate::sync::instruction::CheckpointRequestPayload;
use crate::sync::options::CheckpointMode;
use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::{
        download::http::sync_stream,
        instruction::{CloseSyncStream, Instruction, LogSeverity},
        streams::StreamKey,
    },
};

pub struct DownloadClient<'a> {
    db: Arc<InnerPowerSyncState>,
    channels: &'a SyncChannels,
    receive_commands: &'a async_channel::Receiver<DownloadEvent>,
    options: &'a SyncOptions,

    stream: Option<BoxedStream<Result<DownloadEvent, PowerSyncError>>>,
    checkpoint_seed: Option<Boxed<Result<(), PowerSyncError>>>,
}

impl<'a> DownloadClient<'a> {
    pub fn new(
        db: Arc<InnerPowerSyncState>,
        channels: &'a SyncChannels,
        events: &'a async_channel::Receiver<DownloadEvent>,
        options: &'a SyncOptions,
    ) -> Self {
        Self {
            db,
            channels,
            receive_commands: events,
            options,
            stream: None,
            checkpoint_seed: None,
        }
    }

    pub async fn run(mut self) -> Result<CloseSyncStream, PowerSyncError> {
        let start = StartDownloadIteration {
            parameters: serde_json::Value::Object(Map::new()),
            schema: self.db.schema.clone(),
            include_defaults: self.options.include_default_streams,
            active_streams: self.db.current_streams.collect_active_streams(),
            checkpoint_mode: CoreCheckpointMode::from(&self.options.checkpoints),
        };
        if let Some(end) = self.handle_event(DownloadEvent::Start(start)).await? {
            return Ok(end);
        }

        loop {
            let event = match &mut self.stream {
                Some(stream) => {
                    future::or(
                        Self::receive_command(&self.receive_commands),
                        future::or(
                            Self::receive_on_stream(stream),
                            Self::wait_for_checkpoint_seed(&mut self.checkpoint_seed),
                        ),
                    )
                    .await
                }
                None => Self::receive_command(&self.receive_commands).await,
            }?;

            if let Some(end) = self.handle_event(event).await? {
                return Ok(end);
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: DownloadEvent,
    ) -> Result<Option<CloseSyncStream>, PowerSyncError> {
        trace!("Handling event {event:?}");
        let instructions = {
            let mut conn = self.db.writer().await?;
            event.invoke_control(conn.sqlite_connection_mut())?
        };

        for instr in instructions {
            trace!("Handling instruction {instr:?}");

            match instr {
                Instruction::LogLine { severity, line } => match severity {
                    LogSeverity::Debug => debug!("{}", line),
                    LogSeverity::Info => info!("{}", line),
                    LogSeverity::Warning => warn!("{}", line),
                },
                Instruction::UpdateSyncStatus { status } => {
                    self.db.status.update(|s| s.update_from_core(status))
                }
                Instruction::EstablishSyncStream {
                    request,
                    checkpoint_request,
                } => {
                    trace!("Establishing sync stream with {request}");

                    if let Some(seed_request) = checkpoint_request {
                        let state = self.channels.checkpoints.clone();
                        let connector = self.options.connector.clone();
                        let db = Arc::clone(&self.db);

                        self.checkpoint_seed = Some(
                            async move {
                                let result =
                                    Self::seed_checkpoint_state(db, connector, seed_request).await;

                                state.mark_checkpoints_ready(result.clone());
                                result
                            }
                            .boxed(),
                        );
                    }

                    Self::establish_sync_stream(
                        Arc::clone(&self.db),
                        &mut self.stream,
                        request,
                        self.options,
                    )
                    .await?;

                    // Trigger a crud upload after establishing a sync stream.
                    self.channels.trigger_crud_upload();
                }
                Instruction::FetchCredentials { .. } => {
                    // TODO: Pre-fetching credentials
                    // If did_expire is true, the core extension will also emit a stop
                    // instruction. So we don't have to handle that separately.
                }
                Instruction::CloseSyncStream(close) => {
                    return Ok(Some(close));
                }
                Instruction::FlushFileSystem {} => {
                    // Not applicable outside of Dart web.
                }
                Instruction::DidCompleteSync {} => self
                    .db
                    .status
                    .update(|status| status.clear_download_errors()),
            }
        }

        Ok(None)
    }

    async fn establish_sync_stream(
        db: Arc<InnerPowerSyncState>,
        stream: &mut Option<BoxedStream<Result<DownloadEvent, PowerSyncError>>>,
        request: Box<RawValue>,
        options: &SyncOptions,
    ) -> Result<(), PowerSyncError> {
        let credentials = options.connector.fetch_credentials().await?;
        let request = request.get().to_string();

        *stream = Some(sync_stream(db, credentials, request).boxed());
        Ok(())
    }

    async fn seed_checkpoint_state(
        db: Arc<InnerPowerSyncState>,
        connector: Arc<dyn BackendConnector>,
        request: CheckpointRequestPayload,
    ) -> Result<(), PowerSyncError> {
        let credentials = connector.fetch_credentials().await?;
        let response = checkpoint_request(&db, &request, credentials).await?;
        db.seed_checkpoint_request_id(response).await?;

        Ok(())
    }

    async fn receive_command(
        channel: &async_channel::Receiver<DownloadEvent>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(channel.recv().await.unwrap_or(DownloadEvent::Stop))
    }

    async fn receive_on_stream(
        stream: &mut BoxedStream<Result<DownloadEvent, PowerSyncError>>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(stream
            .try_next()
            .await?
            .unwrap_or(DownloadEvent::ResponseStreamEnd))
    }

    async fn wait_for_checkpoint_seed<F: Future<Output = Result<(), PowerSyncError>> + Unpin>(
        future: &mut Option<F>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        if let Some(future) = future {
            future.await?;
        }

        // This doesn't generate events, we should just poll the future.
        future::pending().await
    }
}

/// An event that triggers the downloading client to advance.
///
/// This is typically a received line from the PowerSync service, but local events are also
/// included.
#[derive(Debug)]
pub enum DownloadEvent {
    /// `connect()` has been called and we need to start establishing a connection.
    Start(StartDownloadIteration),
    /// `disconnect()` has been called or the token has expired.
    Stop,
    /// A textual JSON sync line has been received from the service.
    TextLine { data: String },
    /// A binary BSON sync line has been received from the service.
    BinaryLine { data: Vec<u8> },
    /// A CRUD upload was completed, so the client can re-try applying data.
    CompletedUpload,
    /// HTTP response headers for the sync response have been received, meaning that the sync status
    /// can be set to connected.
    ConnectionEstablished,
    /// The sync response stream has ended.
    ResponseStreamEnd,
    /// Active subscriptions for the application have changed, which might require a reconnect.
    UpdateSubscriptions { keys: Vec<StreamKey> },
}

impl DownloadEvent {
    fn into_powersync_control_argument(self) -> (&'static str, PowerSyncControlArgument) {
        use PowerSyncControlArgument::*;

        match self {
            DownloadEvent::Start(start_download_iteration) => {
                let serialized = serde_json::to_string(&start_download_iteration)
                    .expect("should serialize to string");
                ("start", String(serialized))
            }
            DownloadEvent::Stop => ("stop", Null),
            DownloadEvent::TextLine { data } => ("line_text", String(data)),
            DownloadEvent::BinaryLine { data } => ("line_binary", Bytes(data)),
            DownloadEvent::CompletedUpload => ("completed_upload", Null),
            DownloadEvent::ConnectionEstablished => ("connection", StaticString("established")),
            DownloadEvent::ResponseStreamEnd => ("connection", StaticString("end")),
            DownloadEvent::UpdateSubscriptions { keys } => {
                let serialized = serde_json::to_string(&keys).expect("should serialize to string");
                ("update_subscriptions", String(serialized))
            }
        }
    }

    /// Forwards the event to the core extension, and returns instructions that the SDK needs to
    /// perform.
    pub fn invoke_control(
        self,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Instruction>, PowerSyncError> {
        let tx = TransactionGuard::new(conn)?;

        let instructions = {
            let (op, arg) = self.into_powersync_control_argument();
            let stmt = tx.inner.prepare("SELECT powersync_control(?, ?)")?;

            stmt.bind_text(1, op, Destructor::STATIC)?;
            // SAFETY: `arg` was declared before `stmt`, so it outlives `stmt` on every exit.
            unsafe { arg.bind_to(&stmt, 2)? };

            if let ResultCode::ROW = stmt.step()? {
                serde_json::from_str(stmt.column_text(0).map_err(|_| {
                    PowerSyncError::argument_error("Could not read powersync_control instructions")
                })?)?
            } else {
                panic!("Expected a row") // Can't happen, scalar select
            }
        };

        tx.commit()?;
        Ok(instructions)
    }
}

enum PowerSyncControlArgument {
    Null,
    StaticString(&'static str),
    String(String),
    Bytes(Vec<u8>),
}

impl PowerSyncControlArgument {
    /// # Safety
    ///
    /// The argument must outlive `stmt`.
    unsafe fn bind_to(&self, stmt: &ManagedStmt, index: i32) -> Result<(), ResultCode> {
        match self {
            PowerSyncControlArgument::Null => stmt.bind_null(index),
            PowerSyncControlArgument::StaticString(str) => {
                stmt.bind_text(index, str, Destructor::STATIC)
            }
            PowerSyncControlArgument::String(str) => stmt.bind_text(index, str, Destructor::STATIC),
            PowerSyncControlArgument::Bytes(bytes) => {
                stmt.bind_blob(index, bytes, Destructor::STATIC)
            }
        }?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct StartDownloadIteration {
    pub parameters: serde_json::Value,
    pub schema: Arc<SchemaOrCustom>,
    pub include_defaults: bool,
    pub active_streams: Vec<StreamKey>,
    pub checkpoint_mode: CoreCheckpointMode,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoreCheckpointMode {
    Legacy,
    Requests,
}

impl From<&CheckpointMode> for CoreCheckpointMode {
    fn from(value: &CheckpointMode) -> Self {
        match value {
            CheckpointMode::Legacy => Self::Legacy,
            CheckpointMode::Requests(_) => Self::Requests,
        }
    }
}
