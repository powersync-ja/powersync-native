use std::sync::Arc;

use futures_lite::{StreamExt, future, stream::Boxed as BoxedStream};
use log::{debug, info, trace, warn};
use rusqlite::{
    Connection, ToSql, params,
    types::{ToSqlOutput, ValueRef},
};
use serde::Serialize;
use serde_json::value::RawValue;

use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    schema::Schema,
    sync::{
        download::http::sync_stream,
        instruction::{CloseSyncStream, Instruction, LogSeverity},
        streams::StreamKey,
    },
};

pub struct DownloadClient {
    db: Arc<InnerPowerSyncState>,
    stream: Option<BoxedStream<Result<DownloadEvent, PowerSyncError>>>,
    receive_commands: async_channel::Receiver<DownloadEvent>,
}

impl DownloadClient {
    pub fn new(
        db: Arc<InnerPowerSyncState>,
        events: async_channel::Receiver<DownloadEvent>,
    ) -> Self {
        Self {
            db,
            stream: None,
            receive_commands: events,
        }
    }

    pub async fn run(mut self, options: SyncOptions) -> Result<CloseSyncStream, PowerSyncError> {
        'event: loop {
            let event = match &mut self.stream {
                Some(stream) => {
                    future::or(
                        Self::receive_command(&self.receive_commands),
                        Self::receive_on_stream(stream),
                    )
                    .await
                }
                None => Self::receive_command(&self.receive_commands).await,
            }?;

            trace!("Handling event {event:?}");
            let mut conn = self.db.writer().await?;

            for instr in event.invoke_control(&mut conn)? {
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
                    Instruction::EstablishSyncStream { request } => {
                        trace!("Establishing sync stream with {request}");
                        Self::establish_sync_stream(
                            Arc::clone(&self.db),
                            &mut self.stream,
                            request,
                            &options,
                        )
                        .await?;

                        // Trigger a crud upload after establishing a sync stream.
                        self.db.sync.trigger_crud_uploads().await;
                    }
                    Instruction::FetchCredentials { .. } => {
                        // TODO: Pre-fetching credentials
                        // If did_expire is true, the core extension will also emit a stop
                        // instruction. So we don't have to handle that separately.
                    }
                    Instruction::CloseSyncStream(close) => {
                        break 'event Ok(close);
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
        }
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

    async fn receive_command(
        channel: &async_channel::Receiver<DownloadEvent>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(match channel.recv().await {
            Ok(event) => event,
            Err(_) => DownloadEvent::Stop,
        })
    }

    async fn receive_on_stream(
        stream: &mut BoxedStream<Result<DownloadEvent, PowerSyncError>>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(stream
            .try_next()
            .await?
            .unwrap_or(DownloadEvent::ResponseStreamEnd))
    }
}

/// An event that triggers the downloading client to advance.
///
/// This is typically a received line from the PowerSync service, but local events are also
/// included.
#[derive(Debug)]
pub enum DownloadEvent {
    Start(StartDownloadIteration),
    Stop,
    TextLine {
        data: String,
    },
    BinaryLine {
        data: Vec<u8>,
    },
    /// A CRUD upload was completed, so the client can re-try applying data.
    CompletedUpload,
    ConnectionEstablished,
    ResponseStreamEnd,
    UpdateSubscriptions {
        keys: Vec<StreamKey>,
    },
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
    pub fn invoke_control(self, conn: &mut Connection) -> Result<Vec<Instruction>, PowerSyncError> {
        let tx = conn.transaction()?;

        let instructions = {
            let mut stmt = tx.prepare_cached("SELECT powersync_control(?, ?)")?;
            let (op, arg) = self.into_powersync_control_argument();

            let mut rows = stmt.query(params![op, arg])?;
            let Some(row) = rows.next()? else {
                return Err(rusqlite::Error::QueryReturnedNoRows)?;
            };

            let instructions = row.get_ref(0)?.as_str().map_err(|_| {
                PowerSyncError::argument_error("Could not read powersync_control instructions")
            })?;

            serde_json::from_str(instructions)?
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

impl ToSql for PowerSyncControlArgument {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            PowerSyncControlArgument::Null => ValueRef::Null,
            PowerSyncControlArgument::StaticString(str) => ValueRef::Text(str.as_bytes()),
            PowerSyncControlArgument::String(str) => ValueRef::Text(str.as_bytes()),
            PowerSyncControlArgument::Bytes(items) => ValueRef::Blob(items),
        }))
    }
}

#[derive(Debug, Serialize)]
pub struct StartDownloadIteration {
    pub parameters: serde_json::Value,
    pub schema: Arc<Schema>,
    pub include_defaults: bool,
    pub active_streams: Vec<StreamKey>,
}
