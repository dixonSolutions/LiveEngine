//! The live transport behind one small trait.
//!
//! `live::serve_connection` speaks to a connected peer only through [`LiveTransport`], so a
//! transport other than the built-in WebSocket can be added without touching room, ticket,
//! channel or presence logic. A transport hands over text frames and the control frames
//! the room logic reacts to (ping, pong, close), using the same `Message` type the rooms
//! queue. Authentication, pairing and every size or rate limit stay in `live`.
use axum::extract::ws::{Message, WebSocket};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::{future::Future, pin::Pin};

/// A transport failure. It carries no detail; the connection is simply over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportError;

/// Frames arriving from the peer.
pub type Incoming = Pin<Box<dyn Stream<Item = Result<Message, TransportError>> + Send>>;
/// Frames going to the peer.
pub type Outgoing = Pin<Box<dyn Sink<Message, Error = TransportError> + Send>>;

/// The pending result of reading one frame.
pub type Received<'a> =
    Pin<Box<dyn Future<Output = Option<Result<Message, TransportError>>> + Send + 'a>>;

pub trait LiveTransport: Send + 'static {
    /// The next frame, used once for the opening `auth` frame before the connection is split.
    fn recv(&mut self) -> Received<'_>;
    /// Split into independent halves once the peer is authenticated.
    fn split(self) -> (Outgoing, Incoming)
    where
        Self: Sized;
}

impl LiveTransport for WebSocket {
    fn recv(&mut self) -> Received<'_> {
        Box::pin(async move {
            WebSocket::recv(self)
                .await
                .map(|result| result.map_err(|_| TransportError))
        })
    }
    fn split(self) -> (Outgoing, Incoming) {
        let (sink, stream) = StreamExt::split(self);
        (
            Box::pin(sink.sink_map_err(|_| TransportError)),
            Box::pin(stream.map(|result| result.map_err(|_| TransportError))),
        )
    }
}
