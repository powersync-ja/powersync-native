use std::{
    cmp::min,
    io::{Error, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use futures_lite::{AsyncBufRead, Stream, ready};
use pin_project_lite::pin_project;

pin_project! {
    /// A [Stream] implementation splitting an underlying [AsyncBufRead] instance into unparsed BSON
    /// objects by extracting frame information from the length prefix.

    pub struct BsonObjects<R> {
        #[pin]
        reader:R,
        buf: Vec<u8>,
        remaining: RemainingBytes,
    }
}

impl<R: AsyncBufRead> BsonObjects<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            remaining: RemainingBytes::default(),
        }
    }

    fn process_bytes(
        target: &mut Vec<u8>,
        remaining: &mut RemainingBytes,
        mut buf: &[u8],
    ) -> (Poll<Option<std::io::Result<Vec<u8>>>>, usize) {
        if buf.len() == 0 {
            // End of stream. This is an error if we were in the middle of reading an object.
            return if remaining.is_idle() {
                (Poll::Ready(None), 0)
            } else {
                (
                    Poll::Ready(Some(Err(Error::new(
                        ErrorKind::UnexpectedEof,
                        "stream ended in object",
                    )))),
                    0,
                )
            };
        }

        let mut consumed = 0usize;
        let result = loop {
            let (read, switch_state) = remaining.consume(buf.len());
            let (taken, remaining_buf) = buf.split_at(read);

            target.extend_from_slice(&taken);
            buf = remaining_buf;
            consumed += read;

            break if !switch_state {
                // We need more bytes to complete the pending object or size header.
                Poll::Pending
            } else {
                match remaining.clone() {
                    RemainingBytes::ForSize(_) => {
                        // Done reading size => transition to reading object. Each BSON object
                        // starts with an little-endian 32-bit integer describing its size.
                        assert_eq!(target.len(), 4);

                        let size = i32::from_le_bytes(target[0..4].try_into().unwrap());
                        if size < 5 {
                            // At the very least we need the 4 byte length and a zero terminator.
                            Poll::Ready(Some(Err(Error::new(
                                ErrorKind::InvalidData,
                                "Invalid length header for BSON",
                            ))))
                        } else {
                            // Length is the total size of the frame, including the 4 byte length header
                            *remaining = RemainingBytes::ForObject(size as usize - 4);
                            continue;
                        }
                    }
                    RemainingBytes::ForObject(_) => {
                        // Done reading the object => emit.
                        *remaining = RemainingBytes::default();
                        let buf = std::mem::replace(target, Vec::new());
                        Poll::Ready(Some(Ok(buf)))
                    }
                }
            };
        };

        (result, consumed)
    }
}

impl<R: AsyncBufRead> Stream for BsonObjects<R> {
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            let buf = match ready!(this.reader.as_mut().poll_fill_buf(cx)) {
                Ok(buf) => buf,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };

            let (result, consumed_bytes) = Self::process_bytes(this.buf, this.remaining, buf);
            this.reader.as_mut().consume(consumed_bytes);

            if result.is_pending() {
                // We need more bytes to consume the object, so poll the reader again.
                continue;
            } else {
                break result;
            }
        }
    }
}

#[derive(Clone)]
pub enum RemainingBytes {
    ForSize(usize),
    ForObject(usize),
}

impl RemainingBytes {
    pub fn consume(&mut self, max: usize) -> (usize, bool) {
        let remaining = match self {
            RemainingBytes::ForSize(size) => size,
            RemainingBytes::ForObject(size) => size,
        };
        let readable = min(*remaining, max);
        *remaining = *remaining - readable;

        (readable, *remaining == 0)
    }

    pub fn is_idle(&self) -> bool {
        match self {
            RemainingBytes::ForSize(4) => true,
            _ => false,
        }
    }
}

impl Default for RemainingBytes {
    fn default() -> Self {
        Self::ForSize(4)
    }
}

#[cfg(test)]
mod test {
    use futures_lite::{StreamExt, future};

    use crate::util::bson_split::BsonObjects;

    #[test]
    fn empty_source() {
        let source: [u8; 0] = [];
        let mut source = BsonObjects::new(source.as_slice());

        let next = future::block_on(async { source.next().await });
        assert!(matches!(next, None));
    }

    #[test]
    fn split_bson_objects() {
        let source: [u8; _] = [
            5, 0, 0, 0, 1, //
            6, 0, 0, 0, 0, 0,
        ];
        let mut bson = BsonObjects::new(source.as_slice());

        let Some(Ok(bytes)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected BSON object");
        };
        assert_eq!(&bytes, &source[0..5]);

        let Some(Ok(bytes)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected BSON object");
        };
        assert_eq!(&bytes, &source[5..11]);

        let next = future::block_on(async { bson.next().await });
        assert!(matches!(next, None));
    }

    #[test]
    fn invalid_bson_size() {
        let source: [u8; _] = [3, 0, 0, 0];
        let mut bson = BsonObjects::new(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }

    #[test]
    fn invalid_end_in_length() {
        let source: [u8; _] = [5, 0];
        let mut bson = BsonObjects::new(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }

    #[test]
    fn invalid_end_in_object() {
        let source: [u8; _] = [6, 0, 0, 0, 0];
        let mut bson = BsonObjects::new(source.as_slice());

        let Some(Err(_)) = future::block_on(async { bson.next().await }) else {
            panic!("Expected error");
        };
    }
}
