use async_oneshot::oneshot;

pub mod connector;
pub mod download;
mod instruction;
pub mod options;
pub mod progress;
pub mod status;
pub mod stream_priority;
pub mod streams;

pub struct AsyncRequest<T> {
    pub command: T,
    pub response: async_oneshot::Sender<()>,
}

impl<T> AsyncRequest<T> {
    pub fn new(command: T) -> (Self, async_oneshot::Receiver<()>) {
        let (tx, rx) = oneshot();
        (
            Self {
                command,
                response: tx,
            },
            rx,
        )
    }
}
