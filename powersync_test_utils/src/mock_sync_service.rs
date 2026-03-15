use crate::sync_line::{Checkpoint, DataLine, OplogEntry, SyncLine};
use async_trait::async_trait;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt, ready, stream};
use pin_project_lite::pin_project;
use powersync::http::{HttpClient, Request, Response, ResponseBody};
use powersync::{BackendConnector, PowerSyncCredentials, StreamPriority, error::PowerSyncError};
use serde::Serialize;
use serde_json::json;
use std::pin::Pin;
use std::task::Context;
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};

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
            async fn send(&self, req: Request) -> Result<Response, PowerSyncError> {
                match req.url.path() {
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

    async fn sync_stream(&self, req: Request) -> Response {
        let body: serde_json::Value =
            serde_json::from_slice(&req.body.unwrap_or_default()).unwrap();

        let (send, recv) = async_channel::bounded(1);
        let response = Response {
            status: 200,
            body: ResponseBody {
                reader: MockSyncLinesResponse { receive: recv }.boxed(),
                length: None,
            },
            content_type: Some("application/json".to_string()),
        };

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
        let data = Bytes::from(serde_json::to_vec(&data).unwrap());

        Response {
            status: 200,
            content_type: Some("application/json".to_string()),
            body: ResponseBody {
                length: Some(data.len() as u64),
                reader: stream::once(Ok(data)).boxed(),
            },
        }
    }

    fn generate_bad_request() -> Response {
        Response {
            status: 400,
            content_type: None,
            body: ResponseBody {
                reader: stream::empty().boxed(),
                length: Some(0),
            },
        }
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
    }
}

impl Stream for MockSyncLinesResponse {
    type Item = Result<Bytes, PowerSyncError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let line = ready!(this.receive.poll_next(cx)).map(|line| {
            let mut writer = Vec::new();
            serde_json::to_writer(&mut writer, &line).unwrap();
            writer.push(b'\n');
            Ok(Bytes::from(writer))
        });

        Poll::Ready(line)
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
