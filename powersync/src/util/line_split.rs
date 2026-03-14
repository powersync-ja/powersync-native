use crate::error::{PowerSyncError, RawPowerSyncError};
use crate::http::ResponseStream;
use futures_lite::{Stream, StreamExt};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub struct LineSplitter {
    stream: Option<ResponseStream>,
    unfinished_line: String,
}

impl From<ResponseStream> for LineSplitter {
    fn from(value: ResponseStream) -> Self {
        Self {
            stream: Some(value),
            unfinished_line: Default::default(),
        }
    }
}

impl Stream for LineSplitter {
    type Item = Result<String, PowerSyncError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut LineSplitter = &mut *self;

        loop {
            while let Some(idx) = this.unfinished_line.find("\n") {
                // Split into line including the \n, and the rest
                let remainder = this.unfinished_line.split_off(idx + 1);
                let mut completed_line = mem::replace(&mut this.unfinished_line, remainder);
                // Remove \n from the completed line.
                completed_line.pop();

                return Poll::Ready(Some(Ok(completed_line)));
            }

            let Some(stream) = &mut this.stream else {
                return Poll::Ready(None);
            };

            let buffer = match ready!(stream.poll_next(cx)) {
                None => {
                    this.stream = None;
                    return Poll::Ready(if this.unfinished_line.is_empty() {
                        None
                    } else {
                        // End of stream, but we had a pending line. Emit that now.
                        Some(Ok(mem::take(&mut this.unfinished_line)))
                    });
                }
                Some(event) => match event {
                    Err(e) => return Poll::Ready(Some(Err(e))),
                    Ok(bytes) => bytes,
                },
            };

            let as_str = match str::from_utf8(&*buffer) {
                Ok(str) => str,
                Err(_) => {
                    return Poll::Ready(Some(Err(RawPowerSyncError::SyncServiceResponseParsing {
                        desc: "Expected text lines, but got utf-8 decoding error.",
                    }
                    .into())));
                }
            };

            this.unfinished_line.push_str(as_str);
        }
    }
}

#[cfg(test)]
mod test {
    use super::LineSplitter;
    use bytes::Bytes;
    use futures_lite::{StreamExt, future, stream};

    #[test]
    fn splits_lines() {
        let bytes = Bytes::copy_from_slice(b"hello\nworld");
        let mut lines = LineSplitter::from(stream::once(Ok(bytes)).boxed());

        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert_eq!(next.unwrap(), "hello");
        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert_eq!(next.unwrap(), "world");
        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert!(next.is_none());
    }
}
