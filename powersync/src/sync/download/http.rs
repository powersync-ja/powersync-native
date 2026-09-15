use std::borrow::Cow;
use std::sync::Arc;

use crate::http::{Request, Response};
use crate::util::LineSplitter;
use crate::{
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::{connector::PowerSyncCredentials, download::sync_iteration::DownloadEvent},
    util::BsonObjects,
};
use futures_lite::{Stream, StreamExt, stream};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};

/// Requests a stream of [DownloadEvent]s (more specifically text or binary lines) by opening a
/// connection to the PowerSync service.
pub fn sync_stream(
    db: Arc<InnerPowerSyncState>,
    auth: PowerSyncCredentials,
    request_body: String,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let response = async move {
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
        check_ok(response.status)?;

        Ok::<Response, PowerSyncError>(response)
    };

    let stream = stream::once_future(response);

    StreamExt::flat_map(stream, |response| match response {
        Err(error) => stream::once(Err(error)).boxed(),
        Ok(response) => stream::once(Ok(DownloadEvent::ConnectionEstablished))
            .chain(response_to_lines(Ok(response)))
            .boxed(),
    })
}

/// Requests a write checkpoint from the sync service.
pub async fn write_checkpoint(
    db: &InnerPowerSyncState,
    client_id: &str,
    auth: PowerSyncCredentials,
) -> Result<i64, PowerSyncError> {
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
    check_ok(response.status)?;

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

fn check_ok(code: u16) -> Result<(), PowerSyncError> {
    match code {
        200 => Ok(()),
        401 => Err(RawPowerSyncError::InvalidCredentials.into()),
        _ => Err(RawPowerSyncError::UnexpectedStatusCode { code }.into()),
    }
}

/// Reads sync lines from an HTTP response stream.
///
/// For JSON responses, this splits at newline chars. For BSON responses, this tracks the length
/// prefix to split at objects.
fn response_to_lines(
    response: Result<Response, PowerSyncError>,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let response = match response {
        Ok(res) => res,
        Err(e) => return stream::once(Err::<DownloadEvent, PowerSyncError>(e)).boxed(),
    };

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
        env::{PowerSyncEnvironment, Timer},
        http::{HttpClient, Request, ResponseBody},
        schema::Schema,
        sync::coordinator::SyncCoordinator,
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

    impl Timer for UnusedTimer {
        fn delay_once(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(future::pending())
        }
    }

    fn first_event(client: impl HttpClient) -> Result<Option<DownloadEvent>, PowerSyncError> {
        PowerSyncEnvironment::powersync_auto_extension().unwrap();
        let pool = ConnectionPool::single_connection(Connection::open_in_memory().unwrap());
        let environment = PowerSyncEnvironment::custom(client, pool, &UnusedTimer);
        let coordinator = Arc::new(SyncCoordinator::default());
        let db = Arc::new(InnerPowerSyncState::new(
            environment,
            Schema::default().into(),
            &coordinator,
        ));
        let credentials = PowerSyncCredentials {
            endpoint: "https://rust.unit.test.powersync.com/".to_string(),
            token: "token".to_string(),
        };
        let mut events = Box::pin(sync_stream(db, credentials, "{}".to_string()));

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
