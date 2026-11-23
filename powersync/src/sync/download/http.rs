use std::borrow::Cow;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::Authenticator;
use crate::env::AsyncRuntime;
use crate::http::{Request, Response, ResponseStream};
use crate::sync::checkpoint::CheckpointError;
use crate::sync::instruction::CheckpointRequestPayload;
use crate::sync::options::EndpointAndAuthenticator;
use crate::util::LineSplitter;
use crate::{
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::download::sync_iteration::DownloadEvent,
    util::BsonObjects,
};
use futures_lite::{Stream, StreamExt, future, stream};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};

/// Requests a stream of [DownloadEvent]s (more specifically text or binary lines) by opening a
/// connection to the PowerSync service.
///
/// With an `idle_timeout`, the stream fails when the service sends nothing for that long.
pub fn sync_stream(
    db: Arc<InnerPowerSyncState>,
    authenticator: Arc<EndpointAndAuthenticator>,
    request_body: String,
    idle_timeout: Option<Duration>,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let runtime = db.env.runtime.clone();
    let response = async move {
        let auth = authenticator.fetch_credentials().await?;

        let request = Request {
            method: "POST",
            url: auth.parsed_endpoint("sync/stream")?,
            headers: {
                let mut headers: Vec<(&str, Cow<'_, str>)> = vec![];
                headers.push(("Content-Type", "application/json".into()));
                headers.push(("Authorization", format!("Token {}", auth.token).into()));
                headers.push((
                    "Accept",
                    "application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8".into(),
                ));

                headers
            },
            body: Some(request_body.into_bytes()),
        };

        let response = db.env.client.send(request).await?;
        check_ok(response.status, authenticator.authenticator.as_ref())?;

        Ok::<Response, PowerSyncError>(response)
    };

    let stream = stream::once_future(response);

    StreamExt::flat_map(stream, move |response| match response {
        Err(error) => stream::once(Err(error)).boxed(),
        Ok(mut response) => {
            if let Some(timeout) = idle_timeout {
                response.body.reader =
                    with_idle_timeout(response.body.reader, runtime.clone(), timeout);
            }

            stream::once(Ok(DownloadEvent::ConnectionEstablished))
                .chain(response_to_lines(response))
                .boxed()
        }
    })
}

/// Requests a write checkpoint from the sync service.
pub async fn write_checkpoint(
    db: &InnerPowerSyncState,
    client_id: &str,
    authenticator: &EndpointAndAuthenticator,
) -> Result<i64, PowerSyncError> {
    let auth = authenticator.fetch_credentials().await?;
    let mut url = auth.parsed_endpoint("write-checkpoint2.json")?;
    url.set_query(Some(&format!("client_id={}", client_id)));

    let request = Request {
        method: "GET",
        url,
        headers: {
            let mut headers: Vec<(&str, Cow<'_, str>)> = vec![];
            headers.push(("Content-Type", "application/json".into()));
            headers.push(("Authorization", format!("Token {}", auth.token).into()));
            headers.push(("Accept", "application/json".into()));

            headers
        },
        body: None,
    };

    let response = db.env.client.send(request).await?;
    check_ok(response.status, authenticator.authenticator.as_ref())?;

    #[derive(Deserialize)]
    struct WriteCheckpointResponse {
        data: WriteCheckpointData,
    }

    #[serde_as]
    #[derive(Deserialize)]
    struct WriteCheckpointData {
        #[serde_as(as = "DisplayFromStr")]
        write_checkpoint: i64,
    }

    let body_bytes = response.body.read_fully().await?;

    let response: WriteCheckpointResponse = serde_json::from_slice(&body_bytes)?;
    Ok(response.data.write_checkpoint)
}

/// Posts a checkpoint request to the PowerSync sync service.
pub async fn checkpoint_request(
    db: &InnerPowerSyncState,
    authenticator: &EndpointAndAuthenticator,
    body: &CheckpointRequestPayload,
) -> Result<i64, PowerSyncError> {
    if let Some(future) = authenticator
        .authenticator
        .post_checkpoint_request(&body.client_id, body.checkpoint_request_id)
    {
        return future.await;
    }

    let auth = authenticator.fetch_credentials().await?;
    let url = auth.parsed_endpoint("sync/checkpoint-request")?;

    let body = serde_json::to_vec(body)?;
    let request = Request {
        method: "POST",
        url,
        headers: {
            let mut headers: Vec<(&str, Cow<'_, str>)> = vec![];
            headers.push(("Content-Type", "application/json".into()));
            headers.push(("Authorization", format!("Token {}", auth.token).into()));
            headers.push(("Accept", "application/json".into()));

            headers
        },
        body: Some(body),
    };

    let response = db.env.client.send(request).await?;
    if response.status == 404 {
        return Err(RawPowerSyncError::Checkpoint {
            error: CheckpointError::InstanceNotSupported,
        }
        .into());
    }

    check_ok(response.status, authenticator.authenticator.as_ref())?;

    #[derive(Deserialize)]
    struct CheckpointRequestResponse {
        data: CheckpointData,
    }

    #[serde_as]
    #[derive(Deserialize)]
    struct CheckpointData {
        #[serde_as(as = "DisplayFromStr")]
        checkpoint_request_id: i64,
    }

    let body_bytes = response.body.read_fully().await?;

    let response: CheckpointRequestResponse = serde_json::from_slice(&body_bytes)?;
    Ok(response.data.checkpoint_request_id)
}

fn check_ok(code: u16, authenticator: &dyn Authenticator) -> Result<(), PowerSyncError> {
    match code {
        200 => Ok(()),
        401 => {
            authenticator.invalidate_credentials();
            Err(RawPowerSyncError::InvalidCredentials.into())
        }
        _ => Err(RawPowerSyncError::UnexpectedStatusCode { code }.into()),
    }
}

/// Fails the stream when the underlying response doesn't emit a chunk within `timeout`.
///
/// The timer only runs while a chunk is awaited, so time spent processing lines doesn't count
/// towards it. After a timeout, the stream ends.
fn with_idle_timeout(
    reader: ResponseStream,
    runtime: Arc<dyn AsyncRuntime>,
    timeout: Duration,
) -> ResponseStream {
    stream::unfold(Some(reader), move |reader| {
        let runtime = runtime.clone();

        async move {
            let mut reader = reader?;
            let next_chunk = async { Some(reader.next().await) };
            let timed_out = async {
                runtime.delay_once(timeout).await;
                None
            };

            match future::or(next_chunk, timed_out).await {
                Some(Some(chunk)) => Some((chunk, Some(reader))),
                Some(None) => None,
                None => {
                    let error = io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no data received on the sync stream in {timeout:?}"),
                    );
                    Some((Err(RawPowerSyncError::from(error).into()), None))
                }
            }
        }
    })
    .boxed()
}

/// Reads sync lines from an HTTP response stream.
///
/// For JSON responses, this splits at newline chars. For BSON responses, this tracks the length
/// prefix to split at objects.
fn response_to_lines(
    response: Response,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let is_bson = match &response.content_type {
        None => false,
        Some(value) => value.contains("vnd.powersync.bson-stream"),
    };

    if is_bson {
        BsonObjects::new(response.body.reader)
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::BinaryLine { data: line }),
                Err(e) => Err(e),
            })
            .boxed()
    } else {
        LineSplitter::from(response.body.reader)
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::TextLine { data: line }),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::Pin, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use futures_lite::{StreamExt, future};
    use rusqlite::Connection;

    use super::*;
    use crate::{
        db::{internal::InnerPowerSyncState, pool::ConnectionPool},
        env::{AsyncRuntime, PowerSyncEnvironment},
        http::{HttpClient, Request, ResponseBody},
        schema::Schema,
    };

    struct FailingClient;

    #[async_trait]
    impl HttpClient for FailingClient {
        async fn send(&self, _request: Request) -> Result<Response, PowerSyncError> {
            Err(PowerSyncError::argument_error("offline"))
        }
    }

    struct StatusClient(u16);

    #[async_trait]
    impl HttpClient for StatusClient {
        async fn send(&self, _request: Request) -> Result<Response, PowerSyncError> {
            Ok(Response {
                status: self.0,
                content_type: Some("application/x-ndjson".to_string()),
                body: ResponseBody {
                    reader: stream::empty().boxed(),
                    length: Some(0),
                },
            })
        }
    }

    struct UnusedTimer;

    impl AsyncRuntime for UnusedTimer {
        fn delay_once(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(future::pending())
        }

        fn spawn(&self, _task: future::Boxed<()>) -> crate::env::PowerSyncTask<()> {
            panic!("Unsupported in tests")
        }
    }

    fn first_event(client: impl HttpClient) -> Result<Option<DownloadEvent>, PowerSyncError> {
        PowerSyncEnvironment::powersync_auto_extension().unwrap();
        let pool = ConnectionPool::single_connection(Connection::open_in_memory().unwrap());
        let environment = PowerSyncEnvironment::custom(client, pool, UnusedTimer);
        let db = Arc::new(InnerPowerSyncState::new(
            environment,
            Schema::default().into(),
        ));
        let endpoint = "https://rust.unit.test.powersync.com/";

        struct StaticAuthenticator;

        #[async_trait]
        impl Authenticator for StaticAuthenticator {
            async fn resolve_credentials(&self) -> Result<Arc<String>, PowerSyncError> {
                Ok(Arc::new("token".to_string()))
            }
        }

        let authenticator = Arc::new(EndpointAndAuthenticator {
            endpoint: endpoint.to_string(),
            authenticator: Box::new(StaticAuthenticator),
        });
        let mut events = Box::pin(sync_stream(db, authenticator, "{}".to_string(), None));

        future::block_on(events.as_mut().try_next())
    }

    #[test]
    fn transport_error_does_not_report_connection_established() {
        assert!(first_event(FailingClient).is_err());
    }

    #[test]
    fn unsuccessful_status_does_not_report_connection_established() {
        assert!(first_event(StatusClient(401)).is_err());
        assert!(first_event(StatusClient(500)).is_err());
    }

    #[test]
    fn successful_response_reports_connection_before_stream_events() {
        assert!(matches!(
            first_event(StatusClient(200)),
            Ok(Some(DownloadEvent::ConnectionEstablished))
        ));
    }
}
