use std::{str::FromStr, sync::Arc};

use futures_lite::{AsyncBufReadExt, Stream, StreamExt, stream};
use http_client::{HttpClient, Request, Response, http_types::Mime};

use crate::{
    db::internal::InnerPowerSyncState,
    error::{PowerSyncError, RawPowerSyncError},
    sync::{connector::PowerSyncCredentials, download::sync_iteration::DownloadEvent},
    util::BsonObjects,
};

pub fn sync_stream(
    db: Arc<InnerPowerSyncState>,
    auth: PowerSyncCredentials,
    request_body: String,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let response = async move {
        let url = auth.parsed_endpoint()?;
        let url = url.join("sync/stream").unwrap();
        let json = Mime::from_str("application/json").unwrap();

        let mut request = Request::post(url);
        request.set_content_type(json);
        request.append_header("Authorization", format!("Token {}", auth.token));
        request.append_header(
            "Accept",
            "application/vnd.powersync.bson-stream;q=0.9,application/x-ndjson;q=0.8",
        );
        request.set_body(request_body);

        let response = db
            .env
            .client
            .send(request)
            .await
            .map_err(|e| RawPowerSyncError::Http { inner: e })?;

        Ok::<Response, PowerSyncError>(response)
    };

    let stream = stream::once_future(response);

    StreamExt::flat_map(stream, |response| {
        let items = response_to_lines(response);

        stream::once(Ok(DownloadEvent::ConnectionEstablished)).chain(items)
    })
}

fn response_to_lines(
    response: Result<Response, PowerSyncError>,
) -> impl Stream<Item = Result<DownloadEvent, PowerSyncError>> {
    let response = match response {
        Ok(res) => res,
        Err(e) => return stream::once(Err::<DownloadEvent, PowerSyncError>(e)).boxed(),
    };

    let is_bson = match response.content_type() {
        Some(mime)
            if mime.basetype() == "application"
                && mime.subtype() == "vnd.powersync.bson-stream" =>
        {
            false
        }
        _ => false,
    };

    if is_bson {
        BsonObjects::new(response)
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::BinaryLine { data: line }),
                Err(e) => Err(RawPowerSyncError::IO { inner: e }.into()),
            })
            .boxed()
    } else {
        response
            .lines()
            .map(|event| match event {
                Ok(line) => Ok(DownloadEvent::TextLine { data: line }),
                Err(e) => Err(RawPowerSyncError::IO { inner: e }.into()),
            })
            .boxed()
    }
}
