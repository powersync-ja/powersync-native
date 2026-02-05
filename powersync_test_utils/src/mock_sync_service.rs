use std::{
    io::Write,
    str::FromStr,
    sync::{Arc, Mutex},
    task::Poll,
};

use async_trait::async_trait;
use futures_lite::{AsyncBufRead, AsyncRead, StreamExt, ready};
use http_client::{
    Body, Error, HttpClient, Request, Response,
    http_types::{Mime, StatusCode},
};
use pin_project_lite::pin_project;
use powersync::{BackendConnector, PowerSyncCredentials, StreamPriority, error::PowerSyncError};
use serde::Serialize;
use serde_json::json;

use crate::sync_line::{Checkpoint, DataLine, OplogEntry, SyncLine};

pub struct MockSyncService {
    pub receive_requests: async_channel::Receiver<PendingSyncResponse>,
    send_requests: async_channel::Sender<PendingSyncResponse>,
    pub write_checkpoints: Mutex<Box<dyn Fn() -> WriteCheckpointResponse + Send>>,
}

impl Default for MockSyncService {
    fn default() -> Self {
        let (send_request, recv_request) = async_channel::unbounded();

        Self {
            receive_requests: recv_request,
            send_requests: send_request,
            write_checkpoints: Mutex::new(Box::new(|| {
                WriteCheckpointResponse::new("10".to_string())
            })),
        }
    }
}

impl MockSyncService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn client(self: Arc<Self>) -> impl HttpClient {
        struct MockClient {
            service: Arc<MockSyncService>,
        }

        impl std::fmt::Debug for MockClient {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("MockClient").finish()
            }
        }

        #[async_trait]
        impl HttpClient for MockClient {
            async fn send(&self, req: Request) -> Result<Response, Error> {
                match req.url().path() {
                    "/sync/stream" => Ok(self.service.sync_stream(req).await),
                    "/write-checkpoint2.json" => {
                        Ok(self.service.generate_write_checkpoint_response())
                    }
                    _ => Ok(MockSyncService::generate_bad_request()),
                }
            }
        }

        MockClient { service: self }
    }

    async fn sync_stream(&self, mut req: Request) -> Response {
        let body: serde_json::Value = req.body_json().await.unwrap();
        let (send, recv) = async_channel::bounded(1);

        let mut response = Response::new(StatusCode::Ok);
        response.set_body(Body::from_reader(
            Box::pin(MockSyncLinesResponse {
                receive: recv,
                pending_line: None,
            }),
            None,
        ));

        self.send_requests
            .send(PendingSyncResponse {
                request_data: body,
                channel: send,
            })
            .await
            .unwrap();

        response
    }

    fn generate_write_checkpoint_response(&self) -> Response {
        let data = { self.write_checkpoints.lock().unwrap()() };
        let mut response = Response::new(StatusCode::Ok);
        response.set_content_type(Mime::from_str("application/json").unwrap());
        response.set_body(serde_json::to_string(&data).unwrap());
        response
    }

    fn generate_bad_request() -> Response {
        Response::new(StatusCode::BadRequest)
    }
}

pub struct PendingSyncResponse {
    pub request_data: serde_json::Value,
    pub channel: async_channel::Sender<SyncLine<'static>>,
}

impl PendingSyncResponse {
    pub async fn send_checkpoint(&self, checkpoint: Checkpoint<'static>) {
        self.channel
            .send(SyncLine::Checkpoint(checkpoint))
            .await
            .unwrap();
    }

    pub async fn send_checkpoint_complete(&self, last_op_id: i64, prio: Option<StreamPriority>) {
        let msg = SyncLine::Custom(match prio {
            Some(prio) => json!({"partial_checkpoint_complete": {
                "priority": prio.priority_number(),
                "last_op_id": last_op_id.to_string(),
            }}),
            None => json!({"checkpoint_complete": {
                "last_op_id": last_op_id.to_string(),
            }}),
        });

        self.channel.send(msg).await.unwrap()
    }

    pub async fn bogus_data_line(&self, last_id: &mut i64, bucket: &'static str, amount: usize) {
        let mut oplog = vec![];
        for _ in 0..amount {
            let id = *last_id;
            *last_id = id + 1;

            oplog.push(OplogEntry {
                checksum: 0,
                op_id: id,
                op: crate::sync_line::OpType::PUT,
                object_id: Some(id.to_string()),
                object_type: Some(bucket),
                subkey: None,
                data: Some("{}"),
            });
        }

        let data = SyncLine::Data(DataLine {
            bucket,
            data: oplog,
        });
        self.channel.send(data).await.unwrap()
    }
}

pin_project! {
    struct MockSyncLinesResponse {
        #[pin]
        receive: async_channel::Receiver<SyncLine<'static>>,
        pending_line: Option<PendingLine>,
    }
}

struct PendingLine {
    line: Vec<u8>,
    offset: usize,
}

impl AsyncRead for MockSyncLinesResponse {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();

        // Find a pending line to emit.
        let line = {
            match &mut this.pending_line {
                Some(line) => line,
                None => {
                    let line = ready!(this.receive.poll_next(cx));
                    match line {
                        None => return Poll::Ready(Ok(0)),
                        Some(line) => {
                            let mut writer = Vec::new();
                            serde_json::to_writer(&mut writer, &line).unwrap();
                            writer.push(b'\n');

                            this.pending_line.insert(PendingLine {
                                line: writer,
                                offset: 0,
                            })
                        }
                    }
                }
            }
        };

        let available = line.line.len() - line.offset;
        assert!(available > 0);

        let written = buf.write(&line.line[line.offset..]).unwrap();
        line.offset += written;

        if written == available {
            *this.pending_line = None;
        }
        Poll::Ready(Ok(written))
    }
}

impl AsyncBufRead for MockSyncLinesResponse {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        let mut this = self.project();

        // Find a pending line to emit.
        let line = {
            let pending = this.pending_line;
            match pending {
                Some(line) => line,
                None => {
                    let line = ready!(this.receive.poll_next(cx));
                    match line {
                        None => return Poll::Ready(Ok(&[])),
                        Some(line) => {
                            let mut writer = Vec::new();
                            serde_json::to_writer(&mut writer, &line).unwrap();
                            writer.push(b'\n');

                            pending.insert(PendingLine {
                                line: writer,
                                offset: 0,
                            })
                        }
                    }
                }
            }
        };

        let available = line.line.len() - line.offset;
        assert!(available > 0);

        let data = &line.line[line.offset..];

        Poll::Ready(Ok(data))
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let mut this = self.project();
        if let Some(pending) = &mut this.pending_line {
            pending.offset += amt;
            if pending.offset >= pending.line.len() {
                *this.pending_line = None;
            }
        }
    }
}

#[derive(Serialize)]
pub struct WriteCheckpointResponse {
    data: WriteCheckpointResponseData,
}

impl WriteCheckpointResponse {
    pub fn new(checkpoint: String) -> Self {
        Self {
            data: WriteCheckpointResponseData {
                write_checkpoint: checkpoint,
            },
        }
    }
}

#[derive(Serialize)]
pub struct WriteCheckpointResponseData {
    pub write_checkpoint: String,
}

pub struct TestConnector;

#[async_trait]
impl BackendConnector for TestConnector {
    async fn fetch_credentials(&self) -> Result<PowerSyncCredentials, PowerSyncError> {
        Ok(PowerSyncCredentials {
            endpoint: "https://rust.unit.test.powersync.com/".to_string(),
            token: "token".to_string(),
        })
    }

    async fn upload_data(&self) -> Result<(), PowerSyncError> {
        Ok(())
    }
}
