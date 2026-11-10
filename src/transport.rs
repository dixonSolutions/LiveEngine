//! The live transport behind one small trait.
//!
//! The live connection logic speaks to a connected peer only through [`LiveTransport`], so a
//! transport other than the built-in WebSocket can carry live rooms without touching room,
//! ticket, channel or presence logic. A transport hands over text frames and the control frames
//! the room logic reacts to (ping, pong, close), using the same `Message` type the rooms queue.
//!
//! To serve a peer over your own transport, accept the connection yourself and pass it to
//! [`HubService::serve_live_transport`](crate::hub::HubService::serve_live_transport). The peer
//! then authenticates with an `auth` frame carrying a ticket, exactly as over `/v1/live`.
//! Authentication, pairing and every size, rate and connection limit stay in the hub and are the
//! same as for the WebSocket path. The transport's own framing must deliver whole text frames;
//! frames larger than `LiveLimits::max_frame_bytes` are refused.
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

/// A bidirectional, message-framed connection to one live peer.
pub trait LiveTransport: Send + 'static {
    /// A short name for the transport, shown as `transport` in the connection listing.
    fn name(&self) -> &'static str {
        "custom"
    }
    /// The next frame, used once for the opening `auth` frame before the connection is split.
    fn recv(&mut self) -> Received<'_>;
    /// Split into independent halves once the peer is authenticated.
    fn split(self) -> (Outgoing, Incoming)
    where
        Self: Sized;
}

impl LiveTransport for WebSocket {
    fn name(&self) -> &'static str {
        "websocket"
    }
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
