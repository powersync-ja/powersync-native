use crate::error::{RawPowerSyncError, Result};
use crate::http::ResponseStream;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt, ready};
use std::{
    cmp::min,
    pin::Pin,
    task::{Context, Poll},
};

/// A [Stream] implementation splitting an underlying [AsyncBufRead] instance into unparsed BSON
/// objects by extracting frame information from the length prefix.
pub struct BsonObjects {
    stream: ResponseStream,
    buf: Vec<u8>,
    remaining: RemainingBytes,
    current_event: Option<Bytes>,
}

impl BsonObjects {
    /// Creates a [BsonObjects] stream from a source reader [R].
    pub fn new(stream: ResponseStream) -> Self {
        Self {
            stream,
            buf: Vec::new(),
            remaining: RemainingBytes::default(),
            current_event: None,
        }
    }

    fn process_bytes(
        target: &mut Vec<u8>,
        remaining: &mut RemainingBytes,
        mut buf: &[u8],
    ) -> (Poll<Option<Result<Vec<u8>>>>, usize) {
        if buf.is_empty() {
            // End of stream. This is an error if we were in the middle of reading an object.
            return if remaining.is_at_object_boundary() {
                (Poll::Ready(None), 0)
            } else {
                (
                    Poll::Ready(Some(Err(RawPowerSyncError::SyncServiceResponseParsing {
                        desc: "stream ended in object",
                    }
                    .into()))),
                    0,
                )
            };
        }

        let mut consumed = 0usize;
        let result = loop {
            let (read, switch_state) = remaining.consume(buf.len());
            let (taken, remaining_buf) = buf.split_at(read);

            target.extend_from_slice(taken);
            buf = remaining_buf;
            consumed += read;

            break if !switch_state {
                // We need more bytes to complete the pending object or size header.
                Poll::Pending
            } else {
                match remaining.clone() {
                    RemainingBytes::ForSize(_) => {
                        // Done reading size => transition to reading object. Each BSON object
                        // starts with a little-endian 32-bit integer describing its size.
                        assert_eq!(target.len(), 4);

                        let size = i32::from_le_bytes(target[0..4].try_into().unwrap());
                        if size < 5 {
                            // At the very least we need the 4 byte length and a zero terminator.
                            Poll::Ready(Some(Err(RawPowerSyncError::SyncServiceResponseParsing {
                                desc: "Invalid length header for BSON",
                            }
                            .into())))
                        } else {
                            // Length is the total size of the frame, including the 4 byte length header
                            *remaining = RemainingBytes::ForObject(size as usize - 4);
                            continue;
                        }
                    }
                    RemainingBytes::ForObject(_) => {
                        // Done reading the object => emit.
                        *remaining = RemainingBytes::default();
                        let buf = std::mem::take(target);
                        Poll::Ready(Some(Ok(buf)))
                    }
                }
            };
        };

        (result, consumed)
    }
}

impl Stream for BsonObjects {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this: &mut BsonObjects = &mut self;

        loop {
            // First, try to process a buffer we've consumed before.
            while let Some(pending) = &mut this.current_event {
                let (result, consumed_bytes) =
                    Self::process_bytes(&mut this.buf, &mut this.remaining, pending);
                if pending.len() == consumed_bytes {
                    this.current_event = None;
                } else {
                    // current_event = current_event[consumed_bytes..]
                    drop(pending.split_to(consumed_bytes));
                }

                if result.is_ready() {
                    return result;
                }
            }

            let buf = match ready!(this.stream.poll_next(cx)) {
                Some(event) => match event {
                    Ok(buf) => buf,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                None => {
                    // End of stream, which we represent by reading an empty buffer.
                    Bytes::new()
                }
            };
            this.current_event = Some(buf);
        }
    }
}

#[derive(Clone)]
pub enum RemainingBytes {
    /// The stream is currently reading the size of the next BSON object.
    ///
    /// The attached value is the amount of bytes remaining in the length, never more than 4.
    ForSize(usize),
    /// The stream is currently reading the contents of a BSON object.
    ///
    /// The attached value is the amount of bytes remaining in the object, including the trailing
    /// zero byte.
    ForObject(usize),
}

impl RemainingBytes {
    pub fn consume(&mut self, max: usize) -> (usize, bool) {
        let remaining = match self {
            RemainingBytes::ForSize(size) => size,
            RemainingBytes::ForObject(size) => size,
        };
        let readable = min(*remaining, max);
        *remaining -= readable;

        (readable, *remaining == 0)
    }

    pub fn is_at_object_boundary(&self) -> bool {
        matches!(self, RemainingBytes::ForSize(4))
    }
}

impl Default for RemainingBytes {
    fn default() -> Self {
        Self::ForSize(4)
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use futures_lite::{StreamExt, future, stream};

    use crate::util::bson_split::BsonObjects;

    fn bson_objects(source: &[u8]) -> BsonObjects {
        let bytes = Bytes::copy_from_slice(source);
        BsonObjects::new(stream::once(Ok(bytes)).boxed())
    }

    #[test]
    fn empty_source() {
        let source: [u8; 0] = [];
        let mut source = bson_objects(source.as_slice());

        let next = future::block_on(async { source.next().await });
        assert!(next.is_none());
    }

    #[test]
    fn split_bson_objects() {
        let source: [u8; _] = [
            5, 0, 0, 0, 1, //
            6, 0, 0, 0, 0, 0,
        ];
        let mut bson = bson_objects(source.as_slice());

        let Some(Ok(bytes)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected BSON object");
        };
        assert_eq!(&bytes, &source[0..5]);

        let Some(Ok(bytes)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected BSON object");
        };
        assert_eq!(&bytes, &source[5..11]);

        let next = future::block_on(async { bson.next().await });
        assert!(next.is_none());
    }

    #[test]
    fn invalid_bson_size() {
        let source: [u8; _] = [3, 0, 0, 0];
        let mut bson = bson_objects(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }

    #[test]
    fn invalid_end_in_length() {
        let source: [u8; _] = [5, 0];
        let mut bson = bson_objects(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }

    #[test]
    fn invalid_end_in_object() {
        let source: [u8; _] = [6, 0, 0, 0, 0];
        let mut bson = bson_objects(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }
}
