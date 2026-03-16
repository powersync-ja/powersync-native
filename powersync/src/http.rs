use crate::error::PowerSyncError;
use async_trait::async_trait;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
#[cfg(feature = "reqwest")]
use reqwest::Client as ReqwestClient;
use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

/// An asynchronous HTTP client used internally by the PowerSync SDK to connect to a PowerSync
/// service.
///
/// An implementation of this is required when constructing PowerSync databases. When the `reqwest`
/// feature is enabled, this crate provides an implementation for `reqwest::Client`.
#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    /// Sends an HTTP request.
    ///
    /// Implementations are required to support streamed responses.
    async fn send(&self, req: Request) -> Result<Response, PowerSyncError>;
}

/// A request sent by the PowerSync SDK.
#[non_exhaustive]
pub struct Request {
    pub method: &'static str,
    pub url: Url,
    pub headers: Vec<(&'static str, Cow<'static, str>)>,
    /// This crate doesn't use streaming request bodies, so requests will contain the full request
    /// body as a byte vector.
    pub body: Option<Vec<u8>>,
}

/// Information about http responses used by the PowerSync SDK.
pub struct Response {
    /// The HTTP status code of the response.
    pub status: u16,
    pub content_type: Option<String>,
    /// The streamed response body.
    pub body: ResponseBody,
}

pub type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, PowerSyncError>> + Send + 'static>>;

pub struct ResponseBody {
    pub reader: ResponseStream,
    pub length: Option<u64>,
}

impl ResponseBody {
    pub(crate) async fn read_fully(mut self) -> Result<Vec<u8>, PowerSyncError> {
        let mut body = match self.length {
            None => Vec::new(),
            Some(length) => Vec::with_capacity(length as usize),
        };

        while let Some(chunk) = self.reader.try_next().await? {
            body.extend_from_slice(&chunk)
        }

        Ok(body)
    }
}

#[cfg(feature = "reqwest")]
#[async_trait]
impl HttpClient for ReqwestClient {
    async fn send(&self, req: Request) -> Result<Response, PowerSyncError> {
        use crate::error::RawPowerSyncError;
        use std::str::FromStr;

        let req = {
            let mut wrapped = reqwest::Request::new(
                reqwest::Method::from_str(req.method)
                    .map_err(|_| PowerSyncError::argument_error("Invalid HTTP verb"))?,
                req.url,
            );
            for (key, value) in req.headers {
                wrapped.headers_mut().insert(
                    key,
                    value
                        .as_ref()
                        .try_into()
                        .map_err(|_| PowerSyncError::argument_error("Invalid header value"))?,
                );
            }
            if let Some(body) = req.body {
                *wrapped.body_mut() = Some(reqwest::Body::from(body));
            }

            wrapped
        };

        let response = self.execute(req).await.map_err(RawPowerSyncError::from)?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());
        let content_length = response.content_length();
        let stream = response
            .bytes_stream()
            .map(|item| item.map_err(|e| RawPowerSyncError::from(e).into()));

        let body = ResponseBody {
            reader: stream.boxed(),
            length: content_length,
        };

        Ok(Response {
            status,
            content_type,
            body,
        })
    }
}

#[async_trait]
impl<C: HttpClient> HttpClient for Arc<C> {
    async fn send(&self, req: Request) -> Result<Response, PowerSyncError> {
        let inner = Arc::as_ref(self);
        inner.send(req).await
    }
}
