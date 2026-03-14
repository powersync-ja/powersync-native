use crate::error::PowerSyncError;
use async_trait::async_trait;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
use http::{HeaderMap, Method, Response};
#[cfg(feature = "reqwest")]
use reqwest::Client as ReqwestClient;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

/// An asynchronous HTTP client used internally by the PowerSync SDK to connect to a PowerSync
/// service.
///
/// An implementation of this is required when constructing PowerSync databases. This crate provides
/// two implementations:
#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    /// Sends an HTTP request.
    ///
    /// Implementations are required to support streamed responses.
    async fn send(&self, req: Request) -> Result<Response<ResponseBody>, PowerSyncError>;
}

pub struct Request {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    /// This crate doesn't use streaming request bodies, so requests will contain the full request
    /// body as a byte vector.
    pub body: Option<Vec<u8>>,
}

pub type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, PowerSyncError>> + Send + 'static>>;

pub struct ResponseBody {
    pub reader: ResponseStream,
    pub length: Option<u64>,
}

impl ResponseBody {
    pub async fn read_fully(mut self) -> Result<Vec<u8>, PowerSyncError> {
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
    async fn send(&self, req: Request) -> Result<Response<ResponseBody>, PowerSyncError> {
        use crate::error::RawPowerSyncError;

        let req = {
            let mut wrapped = reqwest::Request::new(req.method, req.url);
            *wrapped.headers_mut() = req.headers;
            if let Some(body) = req.body {
                *wrapped.body_mut() = Some(reqwest::Body::from(body));
            }

            wrapped
        };

        let mut response = self
            .execute(req)
            .await
            .map_err(|e| RawPowerSyncError::from(e))?;
        let mut builder = Response::builder().status(response.status());
        if let Some(headers) = builder.headers_mut() {
            *headers = std::mem::take(response.headers_mut());
        }

        let content_length = response.content_length();
        let stream = response
            .bytes_stream()
            .map(|item| item.map_err(|e| RawPowerSyncError::from(e).into()));

        let body = ResponseBody {
            reader: stream.boxed(),
            length: content_length,
        };

        builder
            .body(body)
            .map_err(|e| RawPowerSyncError::Http { inner: e }.into())
    }
}

#[async_trait]
impl<C: HttpClient> HttpClient for Arc<C> {
    async fn send(&self, req: Request) -> Result<Response<ResponseBody>, PowerSyncError> {
        let inner = Arc::as_ref(self);
        inner.send(req).await
    }
}
