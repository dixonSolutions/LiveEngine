//! A Rust client for live rooms, over the hub relay WebSocket (`/v1/live`).
//!
//! [`LiveClient`] is the Rust counterpart of the JavaScript `LiveClient`: it opens the socket a
//! connection descriptor names, authenticates with the descriptor's single-use ticket, and then
//! sends and receives frames on the room's channels. Everything the hub tells it arrives as a
//! [`LiveEvent`] on the receiver [`LiveClient::connect`] returns: the welcome and peer list,
//! presence changes, messages, round-trip times, hub errors and the final close.
//!
//! Get a descriptor from [`HubClient::pair`](crate::hub::HubClient::pair) (`{ticket, url, userId,
//! room, ...}`) or [`HubClient::ticket`](crate::hub::HubClient::ticket) (`{ticket, url}`) and pass
//! it to [`LiveDescriptor::from_value`]. Frames use the fixed envelope `{v, type, channel, to?,
//! seq, payload}`; the hub stamps `from`. Payloads, channel names and frame types are yours.
//!
//! This client always uses the hub relay. WebRTC data channels, which the JavaScript client can
//! open between browsers, are not implemented here: a Rust peer exchanges frames with browser
//! peers through the hub, and the hub's channel schemas and rate limits apply to every frame.
//!
//! A ticket is single use, so reconnecting needs a fresh one. Give
//! [`LiveClientOptions::with_renew`] a function that asks your server for one (the same `pair` or
//! `ticket` call) and the client reconnects by itself after a dropped socket, within the hub's
//! resume window when it is quick enough that the other peers see no `leave`. Without `renew`
//! a dropped socket ends with [`LiveEvent::Closed`], and the caller pairs again.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeSet,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{
    self, client::IntoClientRequest, protocol::WebSocketConfig, Message,
};

/// The envelope version this client sends.
pub const ENVELOPE_VERSION: u64 = 1;

/// Frame types the hub owns. A frame of one of these types, or one starting with `link.` or
/// `signal.`, cannot be sent with [`LiveClient::send_frame`].
pub const RESERVED_TYPES: &[&str] = &["presence", "welcome", "error", "auth"];

/// Hub error codes after which reconnecting cannot succeed, so the client does not try.
const FATAL_CODES: &[&str] = &[
    "kicked",
    "banned",
    "account-deleted",
    "credentials-changed",
    "auth",
    "duplicate",
];

/// Why a live connection failed or ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveError {
    /// The hub sent `{"type":"error","code":...}` and closed the socket. Codes include `auth`
    /// (bad, spent or expired ticket), `schema`, `channel`, `rate`, `frame`, `duplicate`,
    /// `kicked`, `banned`, `account-deleted` and `credentials-changed`.
    Hub { code: String },
    /// A frame was larger than `max_frame_bytes`: refused before sending, or received and the
    /// connection closed.
    FrameTooLarge { size: usize, limit: usize },
    /// The frame type belongs to the hub.
    ReservedType(String),
    /// The descriptor has no usable `ticket` or `url`.
    InvalidDescriptor(String),
    /// The hub could not be reached, or closed the socket before welcoming the client.
    Connect(String),
    /// No welcome arrived within `connect_timeout`.
    Timeout,
    /// The `renew` function failed to produce a fresh descriptor.
    Renew(String),
    /// The hub sent something that is not a live frame.
    Protocol(String),
    /// The connection is closed.
    Closed,
}

impl LiveError {
    /// The hub's error code, for [`LiveError::Hub`].
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Hub { code } => Some(code),
            _ => None,
        }
    }
    /// Whether reconnecting could not help: the hub refused this user, not this socket.
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::Hub { code } => FATAL_CODES.contains(&code.as_str()),
            Self::InvalidDescriptor(_) | Self::Closed => true,
            _ => false,
        }
    }
}

impl fmt::Display for LiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hub { code } => write!(f, "hub error: {code}"),
            Self::FrameTooLarge { size, limit } => {
                write!(f, "frame of {size} bytes exceeds {limit} bytes")
            }
            Self::ReservedType(kind) => write!(f, "frame type \"{kind}\" is reserved"),
            Self::InvalidDescriptor(why) => write!(f, "invalid connection descriptor: {why}"),
            Self::Connect(why) => write!(f, "could not connect: {why}"),
            Self::Timeout => f.write_str("timed out waiting for the hub"),
            Self::Renew(why) => write!(f, "could not renew the ticket: {why}"),
            Self::Protocol(why) => write!(f, "protocol error: {why}"),
            Self::Closed => f.write_str("connection closed"),
        }
    }
}

impl std::error::Error for LiveError {}

/// Where to connect and the single-use ticket to authenticate with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveDescriptor {
    /// The live socket, `ws(s)://.../v1/live`. `http(s)` is accepted and converted.
    pub url: String,
    pub ticket: String,
    /// This client's peer id, when the descriptor came from `pair`.
    pub user_id: Option<String>,
    /// The room, when the descriptor came from `pair`.
    pub room: Option<String>,
}

impl LiveDescriptor {
    pub fn new(url: impl Into<String>, ticket: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ticket: ticket.into(),
            user_id: None,
            room: None,
        }
    }

    /// Read a descriptor from a `pair` response or a `ticket` response.
    pub fn from_value(value: &Value) -> Result<Self, LiveError> {
        let field = |name: &str| value.get(name).and_then(Value::as_str).map(str::to_owned);
        let ticket = field("ticket")
            .filter(|t| !t.is_empty())
            .ok_or_else(|| LiveError::InvalidDescriptor("ticket is required".into()))?;
        let url =
            field("url").ok_or_else(|| LiveError::InvalidDescriptor("url is required".into()))?;
        Ok(Self {
            url,
            ticket,
            user_id: field("userId"),
            room: field("room"),
        })
    }

    fn socket_url(&self) -> Result<String, LiveError> {
        let mut url = url::Url::parse(&self.url)
            .map_err(|_| LiveError::InvalidDescriptor("url is not a URL".into()))?;
        let scheme = match url.scheme() {
            "http" | "ws" => "ws",
            "https" | "wss" => "wss",
            _ => {
                return Err(LiveError::InvalidDescriptor(
                    "url must be a ws(s) or http(s) URL".into(),
                ))
            }
        };
        url.set_scheme(scheme)
            .map_err(|_| LiveError::InvalidDescriptor("url is not a URL".into()))?;
        Ok(url.into())
    }
}

impl TryFrom<&Value> for LiveDescriptor {
    type Error = LiveError;
    fn try_from(value: &Value) -> Result<Self, LiveError> {
        Self::from_value(value)
    }
}

/// The pending result of a [`LiveRenew`] call.
pub type RenewFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;
/// Fetches a fresh `pair` or `ticket` response for a reconnect. Build one with
/// [`LiveClientOptions::with_renew`].
pub type LiveRenew = Arc<dyn Fn() -> RenewFuture + Send + Sync>;

/// Options for [`LiveClient::connect`]. [`Default`] gives the values named on each field.
#[derive(Clone)]
pub struct LiveClientOptions {
    /// Largest frame sent or received, in bytes (65536, the hub's default
    /// `LIVEENGINE_LIVE_MAX_FRAME_BYTES`).
    pub max_frame_bytes: usize,
    /// How long to wait for the hub's welcome (10 seconds).
    pub connect_timeout: Duration,
    /// How often to measure the round-trip time to the hub, or `None` never to (5 seconds).
    /// The hub must announce `pong` in its welcome; older hubs are never probed.
    pub rtt_interval: Option<Duration>,
    /// Reconnect attempts after a dropped socket, when `renew` is set (5).
    pub retries: u32,
    /// First reconnect delay, doubled per attempt with jitter (250 ms).
    pub retry_base: Duration,
    /// Longest reconnect delay (8 seconds).
    pub retry_max: Duration,
    /// Events held for the caller before the client stops reading the socket (256).
    pub event_buffer: usize,
    /// Where a reconnect gets its fresh ticket (`None`: a dropped socket ends the connection).
    pub renew: Option<LiveRenew>,
}

impl Default for LiveClientOptions {
    fn default() -> Self {
        Self {
            max_frame_bytes: 65536,
            connect_timeout: Duration::from_secs(10),
            rtt_interval: Some(Duration::from_secs(5)),
            retries: 5,
            retry_base: Duration::from_millis(250),
            retry_max: Duration::from_secs(8),
            event_buffer: 256,
            renew: None,
        }
    }
}

impl fmt::Debug for LiveClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveClientOptions")
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("rtt_interval", &self.rtt_interval)
            .field("retries", &self.retries)
            .field("retry_base", &self.retry_base)
            .field("retry_max", &self.retry_max)
            .field("event_buffer", &self.event_buffer)
            .field("renew", &self.renew.is_some())
            .finish()
    }
}

impl LiveClientOptions {
    /// Reconnect after a dropped socket with a fresh descriptor from `renew`, which returns a
    /// `pair` or `ticket` response, for example from [`HubClient::ticket`](crate::hub::HubClient::ticket).
    pub fn with_renew<F, Fut>(mut self, renew: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
    {
        self.renew = Some(Arc::new(move || Box::pin(renew()) as RenewFuture));
        self
    }
}

/// An application frame from another peer.
#[derive(Clone, Debug, PartialEq)]
pub struct LiveFrame {
    /// The sender, as stamped by the hub.
    pub from: String,
    /// Set when the frame was sent to this client alone.
    pub to: Option<String>,
    pub frame_type: String,
    pub channel: String,
    pub seq: u64,
    pub payload: Value,
}

/// Something that happened on the connection.
#[derive(Clone, Debug, PartialEq)]
pub enum LiveEvent {
    /// The hub accepted the ticket. `peers` are the other members of the room. Sent once after
    /// connecting and again after every reconnect, with `reconnected` set; the presence changes
    /// the reconnect missed arrive as `Join` and `Leave` events just before it.
    Welcome {
        peers: Vec<String>,
        /// This client's verified identity, for hubs that set `auth`.
        identity: Option<Value>,
        /// Verified identities of the peers, keyed by peer id.
        peer_identities: Map<String, Value>,
        ice_servers: Vec<Value>,
        reconnected: bool,
    },
    /// A peer joined the room.
    Join {
        peer: String,
        identity: Option<Value>,
    },
    /// A peer left the room.
    Leave { peer: String },
    /// A peer known only through a hub link became unreachable.
    Unreachable { peer: String },
    /// A frame from another peer.
    Message(LiveFrame),
    /// A new round-trip time to the hub.
    Rtt(Duration),
    /// The hub refused something. The hub closes the socket after it; `Reconnecting` or
    /// `Closed` follows.
    Error(LiveError),
    /// The socket dropped and the client is reconnecting with a renewed ticket.
    Reconnecting {
        attempt: u32,
        delay: Duration,
        error: LiveError,
    },
    /// The connection is over and no reconnect follows. `None` after [`LiveClient::close`].
    Closed { error: Option<LiveError> },
}

/// One frame to send. Build it with [`LiveSend::new`] and the setters.
#[derive(Clone, Debug, PartialEq)]
pub struct LiveSend {
    pub channel: String,
    pub frame_type: String,
    pub to: Option<String>,
    pub payload: Value,
}

impl LiveSend {
    /// A broadcast `message` frame on `channel`.
    pub fn new(channel: impl Into<String>, payload: Value) -> Self {
        Self {
            channel: channel.into(),
            frame_type: "message".into(),
            to: None,
            payload,
        }
    }
    /// Send to one peer instead of the whole room.
    pub fn to(mut self, peer: impl Into<String>) -> Self {
        self.to = Some(peer.into());
        self
    }
    /// Use your own frame type instead of `message`.
    pub fn frame_type(mut self, frame_type: impl Into<String>) -> Self {
        self.frame_type = frame_type.into();
        self
    }
}

fn reserved(frame_type: &str) -> bool {
    RESERVED_TYPES.contains(&frame_type)
        || frame_type.starts_with("link.")
        || frame_type.starts_with("signal.")
}

enum Command {
    Send(String, oneshot::Sender<Result<(), LiveError>>),
    Close(oneshot::Sender<()>),
}

#[derive(Default)]
struct Shared {
    peers: Mutex<BTreeSet<String>>,
    rtt: Mutex<Option<Duration>>,
    identity: Mutex<Option<Value>>,
    closed: AtomicBool,
}

/// A connection to one live room. Cheap to clone; every clone sends on the same connection.
/// The connection closes when [`LiveClient::close`] is called or every clone is dropped.
#[derive(Clone)]
pub struct LiveClient {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
    seq: Arc<AtomicU64>,
    user_id: Option<String>,
    room: Option<String>,
    max_frame_bytes: usize,
}

impl fmt::Debug for LiveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveClient")
            .field("user_id", &self.user_id)
            .field("room", &self.room)
            .field("closed", &self.is_closed())
            .finish()
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// What a welcome said.
struct Welcome {
    peers: BTreeSet<String>,
    identity: Option<Value>,
    peer_identities: Map<String, Value>,
    ice_servers: Vec<Value>,
    pong: bool,
}

impl LiveClient {
    /// Connect to the room `descriptor` names and wait for the hub's welcome. Returns the client
    /// and the receiver of its events, which starts with [`LiveEvent::Welcome`].
    ///
    /// The ticket is spent by this call, so a refused connection is not retried: the error is
    /// returned (a refused ticket is `LiveError::Hub { code: "auth" }`).
    pub async fn connect(
        descriptor: LiveDescriptor,
        options: LiveClientOptions,
    ) -> Result<(Self, mpsc::Receiver<LiveEvent>), LiveError> {
        let (socket, welcome) = open(&descriptor, &options).await?;
        let shared = Arc::new(Shared::default());
        *shared.peers.lock().unwrap() = welcome.peers.clone();
        *shared.identity.lock().unwrap() = welcome.identity.clone();
        let (events_tx, events) = mpsc::channel(options.event_buffer.max(1));
        let (commands, commands_rx) = mpsc::channel(64);
        let client = Self {
            commands,
            shared: shared.clone(),
            seq: Arc::new(AtomicU64::new(0)),
            user_id: descriptor.user_id.clone(),
            room: descriptor.room.clone(),
            max_frame_bytes: options.max_frame_bytes,
        };
        let pong = welcome.pong;
        let _ = events_tx.send(welcome_event(welcome, false)).await;
        tokio::spawn(
            Driver {
                shared,
                events: events_tx,
                commands: commands_rx,
                options,
                descriptor,
                held: Vec::new(),
            }
            .run(socket, pong),
        );
        Ok((client, events))
    }

    /// This client's peer id, when the descriptor carried `userId`.
    pub fn id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }
    /// The room, when the descriptor carried `room`.
    pub fn room(&self) -> Option<&str> {
        self.room.as_deref()
    }
    /// The other peers in the room now.
    pub fn peers(&self) -> Vec<String> {
        self.shared.peers.lock().unwrap().iter().cloned().collect()
    }
    /// The verified identity the hub gave this client, for hubs that set `auth`.
    pub fn identity(&self) -> Option<Value> {
        self.shared.identity.lock().unwrap().clone()
    }
    /// The last round-trip time measured to the hub, or `None` before the first answer or when
    /// the hub does not answer probes.
    pub fn rtt(&self) -> Option<Duration> {
        *self.shared.rtt.lock().unwrap()
    }
    /// Whether the connection is over.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst) || self.commands.is_closed()
    }

    /// Send `payload` to everyone else in the room on `channel`.
    pub async fn send(&self, channel: &str, payload: Value) -> Result<(), LiveError> {
        self.send_frame(LiveSend::new(channel, payload)).await
    }
    /// Send `payload` to one peer on `channel`. A peer that is not in the room never gets it.
    pub async fn send_to(
        &self,
        peer: &str,
        channel: &str,
        payload: Value,
    ) -> Result<(), LiveError> {
        self.send_frame(LiveSend::new(channel, payload).to(peer))
            .await
    }
    /// Send one frame. Resolves once the frame is written to the socket, or, while the client
    /// reconnects, once it is written to the new one. Refused before sending when the frame
    /// type is reserved or the frame exceeds `max_frame_bytes`.
    pub async fn send_frame(&self, frame: LiveSend) -> Result<(), LiveError> {
        if frame.channel.is_empty() || frame.frame_type.is_empty() {
            return Err(LiveError::Protocol(
                "channel and type must be non-empty".into(),
            ));
        }
        if reserved(&frame.frame_type) {
            return Err(LiveError::ReservedType(frame.frame_type));
        }
        let mut value = json!({
            "v": ENVELOPE_VERSION,
            "type": frame.frame_type,
            "channel": frame.channel,
            "seq": self.seq.fetch_add(1, Ordering::Relaxed),
            "payload": frame.payload,
        });
        if let Some(to) = frame.to {
            value["to"] = Value::String(to);
        }
        let text = value.to_string();
        if text.len() > self.max_frame_bytes {
            return Err(LiveError::FrameTooLarge {
                size: text.len(),
                limit: self.max_frame_bytes,
            });
        }
        let (done, result) = oneshot::channel();
        self.commands
            .send(Command::Send(text, done))
            .await
            .map_err(|_| LiveError::Closed)?;
        result.await.unwrap_or(Err(LiveError::Closed))
    }

    /// Close the connection cleanly. No reconnect follows, and the event receiver ends after
    /// [`LiveEvent::Closed`].
    pub async fn close(&self) {
        let (done, closed) = oneshot::channel();
        if self.commands.send(Command::Close(done)).await.is_ok() {
            let _ = closed.await;
        }
    }
}

fn welcome_event(welcome: Welcome, reconnected: bool) -> LiveEvent {
    LiveEvent::Welcome {
        peers: welcome.peers.into_iter().collect(),
        identity: welcome.identity,
        peer_identities: welcome.peer_identities,
        ice_servers: welcome.ice_servers,
        reconnected,
    }
}

fn hub_error(value: &Value) -> Option<LiveError> {
    (value.get("type").and_then(Value::as_str) == Some("error") && value.get("from").is_none())
        .then(|| LiveError::Hub {
            code: value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        })
}

fn read_error(error: tungstenite::Error, limit: usize) -> LiveError {
    match error {
        tungstenite::Error::Capacity(tungstenite::error::CapacityError::MessageTooLong {
            size,
            ..
        }) => LiveError::FrameTooLarge { size, limit },
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            LiveError::Closed
        }
        other => LiveError::Connect(other.to_string()),
    }
}

/// Open a socket, authenticate and wait for the welcome.
async fn open(
    descriptor: &LiveDescriptor,
    options: &LiveClientOptions,
) -> Result<(Socket, Welcome), LiveError> {
    let url = descriptor.socket_url()?;
    let request = url
        .as_str()
        .into_client_request()
        .map_err(|_| LiveError::InvalidDescriptor("url is not a WebSocket URL".into()))?;
    let limit = options.max_frame_bytes;
    let config = WebSocketConfig::default()
        .max_message_size(Some(limit))
        .max_frame_size(Some(limit));
    let deadline = Instant::now() + options.connect_timeout;
    let (mut socket, _) = tokio::time::timeout_at(
        deadline,
        tokio_tungstenite::connect_async_with_config(request, Some(config), true),
    )
    .await
    .map_err(|_| LiveError::Timeout)?
    .map_err(|e| LiveError::Connect(e.to_string()))?;
    let auth = json!({"type":"auth","ticket":descriptor.ticket}).to_string();
    socket
        .send(Message::Text(auth.into()))
        .await
        .map_err(|e| LiveError::Connect(e.to_string()))?;
    loop {
        let message = tokio::time::timeout_at(deadline, socket.next())
            .await
            .map_err(|_| LiveError::Timeout)?;
        let text = match message {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Close(_))) | None => {
                return Err(LiveError::Connect(
                    "the hub closed the connection before welcoming it".into(),
                ))
            }
            Some(Ok(_)) => continue,
            Some(Err(error)) => return Err(read_error(error, limit)),
        };
        let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
            continue;
        };
        if let Some(error) = hub_error(&value) {
            let _ = socket.close(None).await;
            return Err(error);
        }
        if value.get("type").and_then(Value::as_str) != Some("welcome")
            || value.get("from").is_some()
        {
            continue;
        }
        let own = descriptor.user_id.as_deref();
        let peers = value
            .get("peers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|peer| Some(*peer) != own)
            .map(str::to_owned)
            .collect();
        let welcome = Welcome {
            peers,
            identity: value.get("identity").cloned(),
            peer_identities: value
                .get("peerIdentities")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            ice_servers: value
                .get("iceServers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            pong: value.get("pong") == Some(&Value::Bool(true)),
        };
        return Ok((socket, welcome));
    }
}

/// Owns the socket: writes queued frames, reads and sorts incoming ones, probes the round trip
/// and reconnects.
struct Driver {
    shared: Arc<Shared>,
    events: mpsc::Sender<LiveEvent>,
    commands: mpsc::Receiver<Command>,
    options: LiveClientOptions,
    descriptor: LiveDescriptor,
    /// Frames sent while reconnecting, written to the new socket first.
    held: Vec<(String, oneshot::Sender<Result<(), LiveError>>)>,
}

/// How one socket ended.
enum Ended {
    /// The caller closed the client, or dropped every handle.
    Closed,
    /// The socket dropped, with the hub's error frame if it sent one.
    Dropped(LiveError),
}

impl Driver {
    async fn emit(&self, event: LiveEvent) {
        let _ = self.events.send(event).await;
    }

    async fn run(mut self, mut socket: Socket, mut pong: bool) {
        loop {
            let error = match self.serve(&mut socket, pong).await {
                Ended::Closed => {
                    let _ = socket.close(None).await;
                    self.finish(None).await;
                    return;
                }
                Ended::Dropped(error) => error,
            };
            let _ = socket.close(None).await;
            match self.reconnect(error).await {
                Ok((next, welcome)) => {
                    socket = next;
                    pong = welcome.pong;
                    self.rewelcome(welcome).await;
                }
                Err(error) => {
                    self.finish(error).await;
                    return;
                }
            }
        }
    }

    async fn finish(&mut self, error: Option<LiveError>) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.peers.lock().unwrap().clear();
        self.commands.close();
        // Frames still queued were never sent.
        for (_, done) in self.held.drain(..) {
            let _ = done.send(Err(LiveError::Closed));
        }
        while let Ok(command) = self.commands.try_recv() {
            match command {
                Command::Send(_, done) => {
                    let _ = done.send(Err(LiveError::Closed));
                }
                Command::Close(done) => {
                    let _ = done.send(());
                }
            }
        }
        self.emit(LiveEvent::Closed { error }).await;
    }

    /// Serve one authenticated socket until it ends.
    async fn serve(&mut self, socket: &mut Socket, pong: bool) -> Ended {
        let limit = self.options.max_frame_bytes;
        let interval = self.options.rtt_interval.filter(|_| pong);
        // The first probe goes out at once, so a round-trip time is known early.
        let mut probe = tokio::time::interval(interval.unwrap_or(Duration::from_secs(3600)));
        probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut next_probe = 0u64;
        let mut outstanding: Option<(u64, Instant)> = None;
        let mut hub_error_seen: Option<LiveError> = None;
        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(Command::Send(text, done)) => {
                        let sent = socket.send(Message::Text(text.into())).await;
                        let failed = sent.is_err();
                        let _ = done.send(sent.map_err(|_| LiveError::Closed));
                        if failed {
                            return Ended::Dropped(hub_error_seen.unwrap_or(LiveError::Closed));
                        }
                    }
                    Some(Command::Close(done)) => {
                        let _ = socket.close(None).await;
                        let _ = done.send(());
                        return Ended::Closed;
                    }
                    None => return Ended::Closed,
                },
                _ = probe.tick(), if interval.is_some() => {
                    // One probe at a time; a lost answer is replaced by the next probe.
                    next_probe += 1;
                    let ping = json!({"type":"ping","id":next_probe}).to_string();
                    if socket.send(Message::Text(ping.into())).await.is_err() {
                        return Ended::Dropped(hub_error_seen.unwrap_or(LiveError::Closed));
                    }
                    outstanding = Some((next_probe, Instant::now()));
                }
                message = socket.next() => {
                    let text = match message {
                        Some(Ok(Message::Text(text))) => text,
                        // Pings are answered by the WebSocket layer as they are read.
                        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                        Some(Ok(Message::Binary(_))) => continue,
                        Some(Ok(Message::Close(_))) | None => {
                            return Ended::Dropped(hub_error_seen.unwrap_or(LiveError::Closed));
                        }
                        Some(Err(error)) => {
                            let error = read_error(error, limit);
                            if matches!(error, LiveError::FrameTooLarge { .. }) {
                                self.emit(LiveEvent::Error(error.clone())).await;
                            }
                            return Ended::Dropped(hub_error_seen.unwrap_or(error));
                        }
                    };
                    // The WebSocket layer enforces this; checked again for any frame it let by.
                    if text.len() > limit {
                        let error = LiveError::FrameTooLarge { size: text.len(), limit };
                        self.emit(LiveEvent::Error(error.clone())).await;
                        return Ended::Dropped(error);
                    }
                    let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
                        continue;
                    };
                    if let Some(error) = hub_error(&value) {
                        self.emit(LiveEvent::Error(error.clone())).await;
                        hub_error_seen = Some(error);
                        continue;
                    }
                    if value.get("type").and_then(Value::as_str) == Some("pong") && value.get("from").is_none() {
                        if let (Some((id, sent)), Some(answered)) = (outstanding, value.get("id").and_then(Value::as_u64)) {
                            if id == answered {
                                outstanding = None;
                                let rtt = sent.elapsed();
                                *self.shared.rtt.lock().unwrap() = Some(rtt);
                                self.emit(LiveEvent::Rtt(rtt)).await;
                            }
                        }
                        continue;
                    }
                    self.frame(value).await;
                }
            }
        }
    }

    /// Sort one incoming frame into an event.
    async fn frame(&mut self, value: Value) {
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        let from = value.get("from").and_then(Value::as_str);
        if kind == "presence" && from.is_none() {
            let Some(peer) = value.get("peer").and_then(Value::as_str).map(str::to_owned) else {
                return;
            };
            if self.descriptor.user_id.as_deref() == Some(peer.as_str()) {
                return;
            }
            let event = match value.get("event").and_then(Value::as_str) {
                Some("join") => {
                    self.shared.peers.lock().unwrap().insert(peer.clone());
                    LiveEvent::Join {
                        peer,
                        identity: value.get("identity").cloned(),
                    }
                }
                Some("leave") => {
                    self.shared.peers.lock().unwrap().remove(&peer);
                    LiveEvent::Leave { peer }
                }
                Some("unreachable") => {
                    self.shared.peers.lock().unwrap().remove(&peer);
                    LiveEvent::Unreachable { peer }
                }
                _ => return,
            };
            return self.emit(event).await;
        }
        // Signalling is for WebRTC, which this client does not open.
        if reserved(kind) {
            return;
        }
        let (Some(from), Some(channel), Some(payload)) = (
            from,
            value.get("channel").and_then(Value::as_str),
            value.get("payload"),
        ) else {
            return;
        };
        let frame = LiveFrame {
            from: from.to_owned(),
            to: value.get("to").and_then(Value::as_str).map(str::to_owned),
            frame_type: kind.to_owned(),
            channel: channel.to_owned(),
            seq: value.get("seq").and_then(Value::as_u64).unwrap_or(0),
            payload: payload.clone(),
        };
        self.emit(LiveEvent::Message(frame)).await;
    }

    /// Get a fresh ticket and connect again, with backoff. `Err` carries the reason to close.
    async fn reconnect(
        &mut self,
        error: LiveError,
    ) -> Result<(Socket, Welcome), Option<LiveError>> {
        let Some(renew) = self.options.renew.clone() else {
            return Err(Some(error));
        };
        if error.is_fatal() {
            return Err(Some(error));
        }
        let mut last = error;
        for attempt in 0..self.options.retries {
            let base = self
                .options
                .retry_base
                .saturating_mul(1u32 << attempt.min(16))
                .min(self.options.retry_max);
            let delay = base.mul_f64(0.5 + rand::random::<f64>() / 2.0);
            self.emit(LiveEvent::Reconnecting {
                attempt: attempt + 1,
                delay,
                error: last.clone(),
            })
            .await;
            // A close during the wait ends the reconnect.
            let sleep = tokio::time::sleep(delay);
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => break,
                    command = self.commands.recv() => match command {
                        Some(Command::Close(done)) => {
                            let _ = done.send(());
                            return Err(None);
                        }
                        None => return Err(None),
                        Some(Command::Send(text, done)) => {
                            // Held until the new socket is open, then sent first.
                            self.held.push((text, done));
                        }
                    }
                }
            }
            let fresh = match renew().await {
                Ok(value) => match LiveDescriptor::from_value(&value) {
                    Ok(mut fresh) => {
                        fresh.user_id = fresh.user_id.or(self.descriptor.user_id.clone());
                        fresh.room = fresh.room.or(self.descriptor.room.clone());
                        fresh
                    }
                    Err(error) => return Err(Some(error)),
                },
                Err(why) => {
                    last = LiveError::Renew(why);
                    continue;
                }
            };
            self.descriptor = fresh;
            match open(&self.descriptor, &self.options).await {
                Ok((mut socket, welcome)) => {
                    for (text, done) in self.held.drain(..) {
                        let sent = socket.send(Message::Text(text.into())).await;
                        let _ = done.send(sent.map_err(|_| LiveError::Closed));
                    }
                    return Ok((socket, welcome));
                }
                Err(error) if error.is_fatal() => return Err(Some(error)),
                Err(error) => last = error,
            }
        }
        Err(Some(last))
    }

    /// Apply a welcome after a reconnect: report the presence changes it implies, then the welcome.
    async fn rewelcome(&mut self, welcome: Welcome) {
        let before = std::mem::replace(
            &mut *self.shared.peers.lock().unwrap(),
            welcome.peers.clone(),
        );
        *self.shared.identity.lock().unwrap() = welcome.identity.clone();
        for peer in before.difference(&welcome.peers) {
            self.emit(LiveEvent::Leave { peer: peer.clone() }).await;
        }
        for peer in welcome.peers.difference(&before) {
            self.emit(LiveEvent::Join {
                peer: peer.clone(),
                identity: welcome.peer_identities.get(peer).cloned(),
            })
            .await;
        }
        self.emit(welcome_event(welcome, true)).await;
    }
}
