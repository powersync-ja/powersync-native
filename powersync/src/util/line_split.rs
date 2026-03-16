use crate::error::{PowerSyncError, RawPowerSyncError};
use crate::http::ResponseStream;
use futures_lite::{Stream, StreamExt};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

pub struct LineSplitter {
    stream: Option<ResponseStream>,
    unfinished_line: Vec<u8>,
}

impl LineSplitter {
    fn emit_line(line: Vec<u8>) -> Poll<Option<Result<String, PowerSyncError>>> {
        Poll::Ready(Some(String::from_utf8(line).map_err(|_| {
            RawPowerSyncError::SyncServiceResponseParsing {
                desc: "Expected text lines, but got utf-8 decoding error.",
            }
            .into()
        })))
    }
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
        let this: &mut LineSplitter = &mut self;

        loop {
            if let Some(idx) = this.unfinished_line.iter().position(|b| *b == b'\n') {
                // Split into line including the \n, and the rest
                let remainder = this.unfinished_line.split_off(idx + 1);
                let mut completed_line = mem::replace(&mut this.unfinished_line, remainder);
                // Remove \n from the completed line.
                completed_line.pop();

                return Self::emit_line(completed_line);
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
                        return Self::emit_line(mem::take(&mut this.unfinished_line));
                    });
                }
                Some(event) => match event {
                    Err(e) => return Poll::Ready(Some(Err(e))),
                    Ok(bytes) => bytes,
                },
            };

            this.unfinished_line.extend(&buffer);
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

    #[test]
    fn utf8_split_across_chunks() {
        // "é" is two bytes: 0xC3 0xA9, split across chunk boundary. This verifies we don't try to
        // decode individual source chunks.
        let mut lines = LineSplitter::from(
            stream::iter(vec![
                Ok(Bytes::from_static(b"caf\xc3")),
                Ok(Bytes::from_static(b"\xa9\n")),
            ])
            .boxed(),
        );

        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert_eq!(next.unwrap(), "café");
        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn splits_lines_separate_events() {
        let mut lines = LineSplitter::from(
            stream::iter(vec![
                Ok(Bytes::from_static(b"hel")),
                Ok(Bytes::from_static(b"lo\nwor")),
                Ok(Bytes::from_static(b"ld")),
            ])
            .boxed(),
        );

        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert_eq!(next.unwrap(), "hello");
        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert_eq!(next.unwrap(), "world");
        let next = future::block_on(async { lines.try_next().await }).unwrap();
        assert!(next.is_none());
    }
}
