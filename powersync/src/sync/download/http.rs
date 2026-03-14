use std::sync::Arc;

use crate::http::{Request, ResponseBody};
use crate::util::LineSplitter;
use crate::{
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::{connector::PowerSyncCredentials, download::sync_iteration::DownloadEvent},
    util::BsonObjects,
};
use futures_lite::{Stream, StreamExt, stream};
use http::header::{IntoHeaderName, InvalidHeaderValue};
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
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
            method: Method::POST,
            url: auth.parsed_endpoint("sync/stream")?,
            headers: {
                let mut header = HeaderMap::<HeaderValue>::default();
                add_header(&mut header, "Content-Type", "application/json")?;
                add_header(
                    &mut header,
                    "Authorization",
                    format!("Token {}", auth.token),
                )?;
                add_header(
                    &mut header,
                    "Accept",
                    "application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8",
                )?;
                header
            },
            body: Some(request_body.into_bytes()),
        };

        let response = db.env.client.send(request).await?;
        check_ok(response.status())?;

        Ok::<Response<ResponseBody>, PowerSyncError>(response)
    };

    let stream = stream::once_future(response);

    StreamExt::flat_map(stream, |response| {
        let items = response_to_lines(response);

        stream::once(Ok(DownloadEvent::ConnectionEstablished)).chain(items)
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
        method: Method::GET,
        url,
        headers: {
            let mut header = HeaderMap::<HeaderValue>::default();
            add_header(&mut header, "Content-Type", "application/json")?;
            add_header(
                &mut header,
                "Authorization",
                format!("Token {}", auth.token),
            )?;
            add_header(&mut header, "Accept", "application/json")?;
            header
        },
        body: None,
    };

    let response = db.env.client.send(request).await?;
    check_ok(response.status())?;

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

    let body = response.into_body();
    let body_bytes = body.read_fully().await?;

    let response: WriteCheckpointResponse = serde_json::from_slice(&body_bytes)?;
    Ok(response.data.write_checkpoint)
}

fn add_header(
    headers: &mut HeaderMap,
    key: impl IntoHeaderName,
    value: impl TryInto<HeaderValue, Error = InvalidHeaderValue>,
) -> Result<(), PowerSyncError> {
    let value: HeaderValue = value
        .try_into()
        .map_err(|_| RawPowerSyncError::ArgumentError {
            desc: "Could not convert to header value".into(),
        })?;
    headers.append(key, value);
    Ok(())
}

fn check_ok(code: StatusCode) -> Result<(), PowerSyncError> {
    match code.as_u16() {
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
    response: Result<Response<ResponseBody>, PowerSyncError>,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let response = match response {
        Ok(res) => res,
        Err(e) => return stream::once(Err::<DownloadEvent, PowerSyncError>(e)).boxed(),
    };

    let content_type = response.headers().get("Content-Type");
    let is_bson = match content_type {
        None => false,
        Some(value) => match value.to_str() {
            Err(_) => false,
            Ok(value) => value.contains("vnd.powersync.bson-stream"),
        },
    };

    let body = response.into_body().reader;

    if is_bson {
        BsonObjects::new(body)
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::BinaryLine { data: line }),
                Err(e) => Err(e),
            })
            .boxed()
    } else {
        LineSplitter::from(body)
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::TextLine { data: line }),
                Err(e) => Err(e),
            })
            .boxed()
    }
}
