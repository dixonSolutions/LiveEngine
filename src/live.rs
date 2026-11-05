//! Short-lived, single-use credentials for live room connections.
use crate::hub::{
    compile_schema, error, expire_hubs, forward_route, header_depth, valid_trace_id, HubRecord,
    Inner, LiveChannel, LiveDelivery, RouteRequest,
};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine as _,
};
use rand::{rngs::OsRng, RngCore};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelPolicy {
    channels: HashMap<String, LiveChannel>,
    schemas: HashMap<String, Value>,
}
impl ChannelPolicy {
    pub(crate) async fn from_channels(
        state: &Inner,
        channels: &HashMap<String, LiveChannel>,
    ) -> Option<Self> {
        let schemas = state.schemas.read().await;
        let mut selected = HashMap::new();
        for channel in channels.values() {
            if let Some(id) = &channel.schema_ref {
                selected.insert(id.clone(), schemas.get(id)?.clone());
            }
        }
        Some(Self {
            channels: channels.clone(),
            schemas: selected,
        })
    }
    pub(crate) fn valid(&self, max: usize) -> bool {
        self.channels.len() <= max
            && self.schemas.len() <= max
            && self.channels.iter().all(|(name, channel)| {
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                    && channel.max_rate != Some(0)
                    && channel
                        .schema_ref
                        .as_ref()
                        .is_none_or(|id| self.schemas.contains_key(id))
            })
            && self
                .schemas
                .values()
                .all(|schema| compile_schema(schema).is_ok())
    }
}

const MAX_USED: usize = 100_000;

pub(crate) fn live_allowed_origins_from_env() -> Vec<String> {
    std::env::var("LIVEENGINE_LIVE_ALLOWED_ORIGINS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(crate) fn ticket_ttl_from_env() -> Result<Duration, String> {
    match std::env::var("LIVEENGINE_TICKET_TTL_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| "LIVEENGINE_TICKET_TTL_SECONDS must be a positive integer".into()),
        Err(std::env::VarError::NotPresent) => Ok(Duration::from_secs(60)),
        Err(_) => Err("LIVEENGINE_TICKET_TTL_SECONDS must be a positive integer".into()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TicketClaims {
    pub hub_path: Vec<String>,
    pub room: String,
    pub user: String,
    pub role: String,
    pub expiry: u64,
    pub jti: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_channels: Option<ChannelPolicy>,
}

/// A service-local signer and replay guard. Its key must remain private.
pub struct TicketSigner {
    key: [u8; 32],
    used: Mutex<HashMap<String, u64>>,
}
impl TicketSigner {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            key,
            used: Mutex::new(HashMap::new()),
        }
    }
    pub fn random_key() -> [u8; 32] {
        let mut key = [0; 32];
        OsRng.fill_bytes(&mut key);
        key
    }
    pub(crate) fn key(&self) -> [u8; 32] {
        self.key
    }
    pub fn issue(
        &self,
        hub_path: Vec<String>,
        room: String,
        user: String,
        role: String,
        ttl: u64,
    ) -> Result<(String, u64), &'static str> {
        self.issue_with_channels(hub_path, room, user, role, ttl, None)
    }
    fn issue_with_channels(
        &self,
        hub_path: Vec<String>,
        room: String,
        user: String,
        role: String,
        ttl: u64,
        inherited_channels: Option<ChannelPolicy>,
    ) -> Result<(String, u64), &'static str> {
        if !matches!(role.as_str(), "peer" | "service" | "watch") || ttl == 0 {
            return Err("invalid ticket parameters");
        }
        let expiry = now()?.checked_add(ttl).ok_or("invalid ticket lifetime")?;
        let mut random = [0u8; 16];
        OsRng.fill_bytes(&mut random);
        let claims = TicketClaims {
            hub_path,
            room,
            user,
            role,
            expiry,
            jti: URL_SAFE_NO_PAD.encode(random),
            inherited_channels,
        };
        let encoded = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).map_err(|_| "cannot encode ticket")?);
        let mac = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.key),
            encoded.as_bytes(),
        );
        let ticket = format!("{}.{}", encoded, URL_SAFE_NO_PAD.encode(mac.as_ref()));
        if ticket.len() > 16384 {
            return Err("ticket exceeds size limit");
        }
        Ok((ticket, expiry))
    }
    /// Verify signature, expiry and binding, consuming the ticket on success.
    pub fn verify(
        &self,
        ticket: &str,
        hub_path: &[String],
        room: &str,
    ) -> Result<TicketClaims, &'static str> {
        let claims = self.verify_inner(ticket, Some((hub_path, room)))?;
        if claims.role == "watch" {
            return Err("invalid ticket");
        }
        Ok(claims)
    }
    pub(crate) fn verify_inner(
        &self,
        ticket: &str,
        binding: Option<(&[String], &str)>,
    ) -> Result<TicketClaims, &'static str> {
        if ticket.len() > 16384 {
            return Err("invalid ticket");
        }
        let (payload, signature) = ticket.split_once('.').ok_or("invalid ticket")?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| "invalid ticket")?;
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.key),
            payload.as_bytes(),
            &signature,
        )
        .map_err(|_| "invalid ticket")?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| "invalid ticket")?;
        let claims: TicketClaims = serde_json::from_slice(&bytes).map_err(|_| "invalid ticket")?;
        let now = now()?;
        if claims.expiry <= now
            || binding.is_some_and(|(path, room)| claims.hub_path != path || claims.room != room)
            || !matches!(claims.role.as_str(), "peer" | "service" | "watch")
            || claims.jti.is_empty()
        {
            return Err("invalid ticket");
        }
        let mut used = self.used.lock().map_err(|_| "ticket guard unavailable")?;
        used.retain(|_, expiry| *expiry > now);
        if used.contains_key(&claims.jti) || used.len() >= MAX_USED {
            return Err("ticket already used or capacity reached");
        }
        used.insert(claims.jti.clone(), claims.expiry);
        Ok(claims)
    }
}
fn now() -> Result<u64, &'static str> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "invalid clock")?
        .as_secs())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TicketRequest {
    user_id: String,
    server_id: String,
    #[serde(default = "peer_role")]
    role: String,
}
fn peer_role() -> String {
    "peer".into()
}
pub(crate) async fn ticket_hub(
    State(state): State<Arc<Inner>>,
    Path(id): Path<String>,
    credential: Option<axum::Extension<crate::links::CredentialAuth>>,
    headers: HeaderMap,
    payload: Result<Json<TicketRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let request = payload.ok().map(|Json(v)| v);
    if credential.is_some() && request.as_ref().is_some_and(|r| r.role != "service") {
        return error(
            StatusCode::FORBIDDEN,
            "link credentials may only request service tickets",
        );
    }
    issue_ticket(state, id, headers, request, None).await
}
pub(crate) async fn issue_ticket(
    state: Arc<Inner>,
    id: String,
    headers: HeaderMap,
    request: Option<TicketRequest>,
    mut inherited_channels: Option<ChannelPolicy>,
) -> Response {
    let Some(request) = request else {
        return error(StatusCode::BAD_REQUEST, "invalid ticket request");
    };
    if request.user_id.is_empty()
        || request.server_id.is_empty()
        || !matches!(request.role.as_str(), "peer" | "service")
    {
        return error(StatusCode::BAD_REQUEST, "invalid ticket request");
    }
    let record = {
        let mut hubs = state.hubs.write().await;
        expire_hubs(&mut hubs);
        hubs.get(&id).cloned()
    };
    let Some(record) = record else {
        return error(StatusCode::NOT_FOUND, "hub not found");
    };
    if let Some(channels) = &record.info.channels {
        if record.handler.is_some() {
            inherited_channels = None;
        } else {
            inherited_channels = ChannelPolicy::from_channels(&state, channels).await;
            if inherited_channels.is_none() {
                return error(StatusCode::BAD_REQUEST, "invalid channel policy");
            }
        }
    }
    if let Some(handler) = record.handler {
        let permit = match state.inflight_calls.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "hub call capacity is busy"),
        };
        let user = request.user_id.clone();
        let room = request.server_id.clone();
        let paired = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut handler = handler.try_lock().map_err(|_| ())?;
            let user_value = handler
                .call("getUser", vec![Value::String(user.clone())])
                .map_err(|_| ())?;
            let server_value = handler
                .call("getServer", vec![Value::String(room.clone())])
                .map_err(|_| ())?;
            Ok::<bool, ()>(
                user_value["status"] == "paired"
                    && user_value["serverId"] == room
                    && server_value["members"]
                        .as_array()
                        .is_some_and(|members| members.iter().any(|m| m.as_str() == Some(&user))),
            )
        })
        .await;
        match paired {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => return error(StatusCode::FORBIDDEN, "user is not paired in room"),
            _ => return error(StatusCode::SERVICE_UNAVAILABLE, "membership unavailable"),
        }
        let Some(url) = state.config.public_url.as_ref() else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "public URL is not configured",
            );
        };
        let mut path = state.hub_path.clone();
        path.push(id);
        match state.ticket_signer.issue_with_channels(
            path,
            request.server_id,
            request.user_id,
            request.role,
            state.config.ticket_ttl.as_secs(),
            inherited_channels,
        ) {
            Ok((ticket, expires_at)) => {
                Json(serde_json::json!({"ticket":ticket,"url":url,"expiresAt":expires_at}))
                    .into_response()
            }
            Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "could not issue ticket"),
        }
    } else if let Some(address) = record.info.address.as_deref() {
        let depth = header_depth(&headers).unwrap_or(usize::MAX);
        if depth >= state.config.max_depth {
            return error(StatusCode::LOOP_DETECTED, "forwarding depth exceeded");
        }
        let mut body = serde_json::to_value(request).unwrap();
        body["id"] = Value::String(record.info.remote_hub_id);
        forward_route(
            &state,
            address,
            record.remote_token.as_deref().unwrap_or(""),
            &headers,
            depth + 1,
            &RouteRequest {
                path: vec![],
                op: "ticket".into(),
                body,
                inherited_schemas: None,
                inherited_channels,
            },
        )
        .await
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "hub handler is unavailable",
        )
    }
}

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

type RoomKey = (Vec<String>, String);
#[derive(Clone)]
struct PeerQueue {
    sender: mpsc::Sender<Message>,
    disconnect: tokio::sync::watch::Sender<bool>,
    /// Service sockets are never announced as bridged peers.
    service: bool,
}
impl PeerQueue {
    fn send(&self, message: Message, lossy: bool) {
        if self.sender.try_send(message).is_err() && !lossy {
            let _ = self.disconnect.send(true);
        }
    }
}

#[derive(Default)]
pub(crate) struct LiveRooms {
    rooms: AsyncMutex<HashMap<RoomKey, HashMap<String, PeerQueue>>>,
    pending: AsyncMutex<HashMap<(RoomKey, String), u64>>,
    connections: std::sync::atomic::AtomicUsize,
    validators: AsyncMutex<HashMap<String, (Value, Arc<jsonschema::Validator>)>>,
    bridges: AsyncMutex<HashMap<RoomKey, Bridge>>,
    recent: Mutex<Recent>,
    next_attach: std::sync::atomic::AtomicU64,
    next_frame: std::sync::atomic::AtomicU64,
    /// Linked frames dropped as loops or duplicates.
    dropped: std::sync::atomic::AtomicU64,
    /// Linked frames or presence refused by wire, channel or schema rules.
    rejected: std::sync::atomic::AtomicU64,
}
type Validators = HashMap<String, Arc<jsonschema::Validator>>;

/// A link attached to a room. Frames offered to its queue are carried to the linked hub.
struct Attached {
    id: u64,
    queue: PeerQueue,
    /// The local user of an inbound link socket; `None` for a link this hub dials.
    user: Option<String>,
}
/// The links of one room and the peers known only through them (peer to link name).
#[derive(Default)]
struct Bridge {
    links: HashMap<String, Attached>,
    remote: HashMap<String, String>,
}
impl Bridge {
    fn is_link_user(&self, peer: &str) -> bool {
        self.links.values().any(|a| a.user.as_deref() == Some(peer))
    }
    fn offer(&self, except: Option<&str>, message: &Message, lossy: bool) {
        for (name, attached) in &self.links {
            if Some(name.as_str()) != except {
                attached.queue.send(message.clone(), lossy);
            }
        }
    }
}
#[derive(Default)]
struct Recent {
    order: VecDeque<(std::time::Instant, u64)>,
    set: HashSet<u64>,
}
const MAX_WIRE: usize = 65536;
const MAX_TRACE: usize = 32;
const MAX_REMOTE_PEERS: usize = 1024;
const RECENT_FRAMES: usize = 4096;
const RECENT_WINDOW: Duration = Duration::from_secs(10);
impl LiveRooms {
    /// Whether a linked frame id was already handled; a ring can deliver one frame along two paths.
    fn seen(&self, id: &str) -> bool {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        id.hash(&mut hasher);
        let key = hasher.finish();
        let mut guard = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        let recent = &mut *guard;
        let now = std::time::Instant::now();
        while recent
            .order
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > RECENT_WINDOW)
            || recent.order.len() >= RECENT_FRAMES
        {
            let Some((_, old)) = recent.order.pop_front() else {
                break;
            };
            recent.set.remove(&old);
        }
        if !recent.set.insert(key) {
            return true;
        }
        recent.order.push_back((now, key));
        false
    }
    /// A unique id for a frame entering the links here: instance id plus a per-process counter.
    fn frame_id(&self, instance: &str) -> String {
        let n = self
            .next_frame
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{instance}-{n}")
    }
    fn count(counter: &std::sync::atomic::AtomicU64) {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.dropped.load(Relaxed), self.rejected.load(Relaxed))
    }
}
fn text(value: Value) -> Message {
    Message::Text(value.to_string().into())
}
fn local_presence(event: &str, peer: &str) -> Message {
    text(serde_json::json!({"type":"presence","event":event,"peer":peer}))
}
fn wire_presence(trace: &[String], event: &str, peer: Option<&str>) -> Message {
    let mut value = serde_json::json!({"type":"link.presence","trace":trace,"event":event});
    if let Some(peer) = peer {
        value["peer"] = Value::String(peer.to_owned());
    }
    text(value)
}
fn extended(trace: &[String], instance: &str) -> Vec<String> {
    let mut trace = trace.to_vec();
    trace.push(instance.to_owned());
    trace
}
/// Carry a frame over the links of a room: to the link that knows `target`, or to every link.
fn forward(
    live: &LiveRooms,
    bridge: &Bridge,
    id: &str,
    trace: &[String],
    except: Option<&str>,
    target: Option<&str>,
    frame: &Value,
    lossy: bool,
) {
    let message =
        text(serde_json::json!({"type":"link.frame","id":id,"trace":trace,"frame":frame}));
    if matches!(&message, Message::Text(t) if t.len() > MAX_WIRE) {
        LiveRooms::count(&live.rejected);
        return;
    }
    match target {
        Some(target) => {
            if let Some(via) = bridge.remote.get(target) {
                if Some(via.as_str()) != except {
                    if let Some(attached) = bridge.links.get(via) {
                        attached.queue.send(message, lossy);
                    }
                }
            }
        }
        None => bridge.offer(except, &message, lossy),
    }
}
fn notify_local(room: Option<&HashMap<String, PeerQueue>>, bridge: &Bridge, message: &Message) {
    for (peer, queue) in room.into_iter().flatten() {
        if !bridge.is_link_user(peer) {
            queue.send(message.clone(), false);
        }
    }
}
fn parse_trace(wire: &Value) -> Option<Vec<String>> {
    let items = wire.get("trace")?.as_array()?;
    if items.len() > MAX_TRACE {
        return None;
    }
    items
        .iter()
        .map(|id| {
            id.as_str()
                .filter(|id| valid_trace_id(id))
                .map(str::to_owned)
        })
        .collect()
}

/// A link this hub dials, attached to a local room.
pub(crate) struct LinkAttach {
    pub(crate) generation: u64,
    /// Current presence, to send first; the first frame also announces the link.
    pub(crate) snapshot: Vec<Message>,
    pub(crate) rx: mpsc::Receiver<Message>,
    pub(crate) disconnected: tokio::sync::watch::Receiver<bool>,
}
pub(crate) fn room_key(state: &Inner, hub_id: &str, room: &str) -> RoomKey {
    let mut path = state.hub_path.clone();
    path.push(hub_id.to_owned());
    (path, room.to_owned())
}
pub(crate) async fn attach_link(
    state: &Arc<Inner>,
    key: &RoomKey,
    link_id: &str,
) -> Option<LinkAttach> {
    resolve(state, &key.0).await?;
    let (sender, rx) = mpsc::channel::<Message>(limit("LIVEENGINE_LIVE_SEND_QUEUE", 64));
    let (disconnect, disconnected) = tokio::sync::watch::channel(false);
    let queue = PeerQueue {
        sender,
        disconnect,
        service: true,
    };
    let (generation, snapshot) =
        attach(state, key, format!("out:{link_id}"), None, Some(queue)).await?;
    Some(LinkAttach {
        generation,
        snapshot,
        rx,
        disconnected,
    })
}
/// Register a link in a room and return its id with the presence to announce over it.
async fn attach(
    state: &Arc<Inner>,
    key: &RoomKey,
    name: String,
    user: Option<&str>,
    fresh: Option<PeerQueue>,
) -> Option<(u64, Vec<Message>)> {
    let live = &state.live_rooms;
    let rooms = live.rooms.lock().await;
    let queue = match fresh {
        Some(queue) => queue,
        None => rooms.get(key)?.get(user?)?.clone(),
    };
    let mut bridges = live.bridges.lock().await;
    let bridge = bridges.entry(key.clone()).or_default();
    let generation = live
        .next_attach
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    bridge.links.insert(
        name.clone(),
        Attached {
            id: generation,
            queue,
            user: user.map(str::to_owned),
        },
    );
    let trace = [state.instance_id.clone()];
    // A link this hub dials announces itself first; an inbound link already has.
    let mut messages = if user.is_none() {
        vec![wire_presence(&trace, "join", None)]
    } else {
        vec![]
    };
    messages.extend(snapshot(state, rooms.get(key), bridge, &name));
    Some((generation, messages))
}
/// The peers to announce to link `name`: local peers and those known through other links.
fn snapshot(
    state: &Inner,
    room: Option<&HashMap<String, PeerQueue>>,
    bridge: &Bridge,
    name: &str,
) -> Vec<Message> {
    let trace = [state.instance_id.clone()];
    let local = room
        .into_iter()
        .flatten()
        .filter(|(peer, queue)| !queue.service && !bridge.is_link_user(peer))
        .map(|(peer, _)| peer);
    let remote = bridge
        .remote
        .iter()
        .filter(|(_, via)| *via != name)
        .map(|(peer, _)| peer);
    local
        .chain(remote)
        .map(|peer| wire_presence(&trace, "join", Some(peer)))
        .collect()
}
/// Queue messages with backpressure, off the lock; a snapshot can exceed the queue.
fn spawn_send(sender: mpsc::Sender<Message>, messages: Vec<Message>) {
    tokio::spawn(async move {
        for message in messages {
            if sender.send(message).await.is_err() {
                break;
            }
        }
    });
}
/// Remove a link; peers known only through it become unreachable.
pub(crate) async fn detach_link(state: &Arc<Inner>, key: &RoomKey, name: &str, generation: u64) {
    let live = &state.live_rooms;
    let rooms = live.rooms.lock().await;
    let mut bridges = live.bridges.lock().await;
    let Some(bridge) = bridges.get_mut(key) else {
        return;
    };
    if bridge.links.get(name).is_none_or(|a| a.id != generation) {
        return;
    }
    bridge.links.remove(name);
    let lost: Vec<String> = bridge
        .remote
        .iter()
        .filter(|(_, via)| *via == name)
        .map(|(peer, _)| peer.clone())
        .collect();
    for peer in &lost {
        bridge.remote.remove(peer);
    }
    let trace = [state.instance_id.clone()];
    for peer in &lost {
        notify_local(rooms.get(key), bridge, &local_presence("unreachable", peer));
        bridge.offer(
            None,
            &wire_presence(&trace, "unreachable", Some(peer)),
            false,
        );
    }
    if !lost.is_empty() {
        // Ask the remaining links to announce again: a peer may be reachable another way.
        bridge.offer(None, &wire_presence(&trace, "join", None), false);
    }
    if bridge.links.is_empty() && bridge.remote.is_empty() {
        bridges.remove(key);
    }
}
/// The channel rules of the local hub, for frames arriving over a link this hub dials.
async fn link_policy(
    state: &Arc<Inner>,
    key: &RoomKey,
) -> Option<(Option<HashMap<String, LiveChannel>>, Validators)> {
    let (_, channels) = resolve(state, &key.0).await?;
    let validators = build_validators(state, &channels, true, None).await?;
    Some((channels, validators))
}
/// Handle a `link.frame` or `link.presence` received from the link `via`.
/// Returns false for any other type. `policy` carries the receiving socket's channel rules;
/// without it the local hub's own rules apply. Over the link budget, lossy frames are dropped.
pub(crate) async fn ingest(
    state: &Arc<Inner>,
    key: &RoomKey,
    via: &str,
    wire: &Value,
    policy: Option<(&Option<HashMap<String, LiveChannel>>, &Validators)>,
    over_budget: bool,
) -> bool {
    match wire.get("type").and_then(Value::as_str) {
        Some("link.frame") => {
            match policy {
                Some((channels, validators)) => {
                    ingest_frame(state, key, via, wire, channels, validators, over_budget).await
                }
                None => {
                    if let Some((channels, validators)) = link_policy(state, key).await {
                        ingest_frame(state, key, via, wire, &channels, &validators, over_budget)
                            .await;
                    } else {
                        LiveRooms::count(&state.live_rooms.rejected);
                    }
                }
            }
            true
        }
        Some("link.presence") => {
            ingest_presence(state, key, via, wire).await;
            true
        }
        _ => false,
    }
}
async fn ingest_frame(
    state: &Arc<Inner>,
    key: &RoomKey,
    via: &str,
    wire: &Value,
    channels: &Option<HashMap<String, LiveChannel>>,
    validators: &Validators,
    over_budget: bool,
) {
    let live = &state.live_rooms;
    let Some(trace) = parse_trace(wire) else {
        return LiveRooms::count(&live.rejected);
    };
    if trace.contains(&state.instance_id) {
        return LiveRooms::count(&live.dropped);
    }
    let Some(frame) = wire.get("frame").and_then(Value::as_object) else {
        return LiveRooms::count(&live.rejected);
    };
    let (Some(from), Some(frame_type), Some(channel)) = (
        frame.get("from").and_then(Value::as_str),
        frame.get("type").and_then(Value::as_str),
        frame.get("channel").and_then(Value::as_str),
    ) else {
        return LiveRooms::count(&live.rejected);
    };
    let target = frame.get("to").and_then(Value::as_str);
    let signal = matches!(frame_type, "signal.offer" | "signal.answer" | "signal.ice");
    if !frame.get("v").is_some_and(Value::is_number)
        || !frame.get("seq").is_some_and(Value::is_u64)
        || !frame.contains_key("payload")
        || frame.get("to").is_some_and(|to| !to.is_string())
        || frame_type.starts_with("link.")
        || (frame_type.starts_with("signal.") && !signal)
        || (signal && target.is_none_or(|to| to == from))
    {
        return LiveRooms::count(&live.rejected);
    }
    let mut lossy = false;
    if !signal {
        if let Some(channels) = channels {
            let Some(definition) = channels.get(channel) else {
                return LiveRooms::count(&live.rejected);
            };
            lossy = matches!(definition.delivery, LiveDelivery::Lossy);
        }
        if validators
            .get(channel)
            .is_some_and(|v| !v.is_valid(&wire["frame"]["payload"]))
        {
            return LiveRooms::count(&live.rejected);
        }
    }
    let Some(id) = wire
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_graphic()))
    else {
        return LiveRooms::count(&live.rejected);
    };
    if lossy && over_budget {
        return LiveRooms::count(&live.dropped);
    }
    let serialized = wire["frame"].to_string();
    if live.seen(id) {
        return LiveRooms::count(&live.dropped);
    }
    let trace = extended(&trace, &state.instance_id);
    let message = Message::Text(serialized.into());
    let rooms = live.rooms.lock().await;
    let bridges = live.bridges.lock().await;
    let Some(bridge) = bridges.get(key).filter(|b| b.links.contains_key(via)) else {
        return;
    };
    let room = rooms.get(key);
    match target {
        Some(to) => {
            if let Some(queue) = room
                .and_then(|r| r.get(to))
                .filter(|_| !bridge.is_link_user(to))
            {
                queue.send(message, lossy);
            } else {
                forward(
                    live,
                    bridge,
                    id,
                    &trace,
                    Some(via),
                    Some(to),
                    &wire["frame"],
                    lossy,
                );
            }
        }
        None => {
            for (peer, queue) in room.into_iter().flatten() {
                if peer != from && !bridge.is_link_user(peer) {
                    queue.send(message.clone(), lossy);
                }
            }
            forward(
                live,
                bridge,
                id,
                &trace,
                Some(via),
                None,
                &wire["frame"],
                lossy,
            );
        }
    }
}
async fn ingest_presence(state: &Arc<Inner>, key: &RoomKey, via: &str, wire: &Value) {
    let live = &state.live_rooms;
    let Some(trace) = parse_trace(wire) else {
        return LiveRooms::count(&live.rejected);
    };
    if trace.contains(&state.instance_id) {
        return LiveRooms::count(&live.dropped);
    }
    let event = wire.get("event").and_then(Value::as_str);
    let peer = match wire.get("peer") {
        // A join without a peer announces the link and asks for the current presence.
        None => None,
        Some(Value::String(peer)) if !peer.is_empty() && peer.len() <= 256 => Some(peer.as_str()),
        _ => return LiveRooms::count(&live.rejected),
    };
    let Some(event @ ("join" | "leave" | "unreachable")) = event else {
        return LiveRooms::count(&live.rejected);
    };
    let rooms = live.rooms.lock().await;
    let mut bridges = live.bridges.lock().await;
    let Some(bridge) = bridges.get_mut(key).filter(|b| b.links.contains_key(via)) else {
        return;
    };
    let Some(peer) = peer else {
        if event == "join" {
            let messages = snapshot(state, rooms.get(key), bridge, via);
            spawn_send(bridge.links[via].queue.sender.clone(), messages);
        }
        return;
    };
    if event == "join" {
        if rooms.get(key).is_some_and(|r| r.contains_key(peer))
            || bridge.remote.contains_key(peer)
            || bridge.remote.len() >= MAX_REMOTE_PEERS
        {
            return;
        }
        bridge.remote.insert(peer.to_owned(), via.to_owned());
    } else if bridge.remote.get(peer).map(String::as_str) == Some(via) {
        bridge.remote.remove(peer);
    } else {
        return;
    }
    notify_local(rooms.get(key), bridge, &local_presence(event, peer));
    let trace = extended(&trace, &state.instance_id);
    bridge.offer(Some(via), &wire_presence(&trace, event, Some(peer)), false);
    if event == "unreachable" {
        // The peer may still be reachable through another link: ask them to announce again.
        let hello = wire_presence(&[state.instance_id.clone()], "join", None);
        bridge.offer(Some(via), &hello, false);
    }
}

pub(crate) async fn live_socket(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let origins = headers.get_all(axum::http::header::ORIGIN);
    let mut origins = origins.iter();
    if let Some(origin) = origins.next() {
        if origins.next().is_some() {
            return error(StatusCode::FORBIDDEN, "origin not allowed");
        }
        if !origin.to_str().ok().is_some_and(|o| {
            state
                .config
                .live_allowed_origins
                .iter()
                .any(|allowed| allowed == o)
        }) {
            return error(StatusCode::FORBIDDEN, "origin not allowed");
        }
    }
    let live = state.live_rooms.clone();
    if live
        .connections
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |n| (n < limit("LIVEENGINE_LIVE_MAX_CONNECTIONS", 4096)).then_some(n + 1),
        )
        .is_err()
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "live connection capacity reached",
        );
    }
    let permit = ConnectionPermit(live);
    let frame_limit = limit("LIVEENGINE_LIVE_MAX_FRAME_BYTES", 65536);
    ws.max_message_size(frame_limit)
        .max_frame_size(frame_limit)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            serve_socket(state, socket).await
        })
}

struct ConnectionPermit(Arc<LiveRooms>);
impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.0
            .connections
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Messages and bytes per second a link socket may carry.
pub(crate) fn link_budget() -> (usize, usize) {
    (
        limit("LIVEENGINE_LINK_MAX_MESSAGES_PER_SECOND", 2000),
        limit("LIVEENGINE_LINK_MAX_BYTES_PER_SECOND", 16_777_216),
    )
}

fn limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
        .min(16_777_216)
}

fn ice_servers(config: &crate::hub::HubConfig, user: &str) -> Option<Vec<Value>> {
    let mut servers = config.ice_servers.clone();
    if let Some(secret) = &config.turn_secret {
        // A random suffix avoids collisions when the same identity connects to multiple rooms.
        let expiry = now().ok()?.checked_add(3600)?;
        let username = format!("{}:{}:{}", expiry, user, rand::random::<u64>());
        let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret.as_bytes());
        let credential = STANDARD.encode(hmac::sign(&key, username.as_bytes()).as_ref());
        for server in &mut servers {
            let object = server.as_object_mut()?;
            let is_turn = |url: &str| url.starts_with("turn:") || url.starts_with("turns:");
            let has_turn = match object.get("urls")? {
                Value::String(url) => is_turn(url),
                Value::Array(urls) => urls.iter().any(|url| url.as_str().is_some_and(is_turn)),
                _ => false,
            };
            if has_turn {
                object.insert("username".into(), Value::String(username.clone()));
                object.insert("credential".into(), Value::String(credential.clone()));
                object.remove("credentialType");
            }
        }
    }
    Some(servers)
}

async fn resolve(
    state: &Arc<Inner>,
    path: &[String],
) -> Option<(HubRecord, Option<HashMap<String, LiveChannel>>)> {
    let mut current = state.clone();
    let mut channels = None;
    for (index, id) in path.iter().enumerate() {
        let record = current.hubs.read().await.get(id).cloned()?;
        if record.info.channels.is_some() {
            channels = record.info.channels.clone();
        }
        if index + 1 == path.len() {
            return Some((record, channels));
        }
        current = record.child?;
    }
    None
}

async fn paired(record: &HubRecord, user: &str, room: &str) -> bool {
    let Some(handler) = record.handler.clone() else {
        return false;
    };
    let user = user.to_owned();
    let room = room.to_owned();
    tokio::task::spawn_blocking(move || {
        let Ok(mut handler) = handler.lock() else {
            return false;
        };
        let Ok(u) = handler.call("getUser", vec![Value::String(user.clone())]) else {
            return false;
        };
        let Ok(r) = handler.call("getServer", vec![Value::String(room.clone())]) else {
            return false;
        };
        u["status"] == "paired"
            && u["serverId"] == room
            && r["members"]
                .as_array()
                .is_some_and(|m| m.iter().any(|v| v.as_str() == Some(&user)))
    })
    .await
    .unwrap_or(false)
}

async fn build_validators(
    state: &Arc<Inner>,
    channels: &Option<HashMap<String, LiveChannel>>,
    local_channels: bool,
    inherited_channels: Option<&ChannelPolicy>,
) -> Option<Validators> {
    let mut validators = HashMap::new();
    if let Some(channels) = channels {
        let schemas = state.schemas.read().await;
        let mut cache = state.live_rooms.validators.lock().await;
        cache.retain(|id, _| schemas.contains_key(id));
        for (name, channel) in channels {
            if let Some(id) = &channel.schema_ref {
                let schema = if local_channels {
                    schemas.get(id)
                } else {
                    inherited_channels.and_then(|p| p.schemas.get(id))
                }?;
                if !cache.get(id).is_some_and(|(saved, _)| saved == schema) {
                    let validator = compile_schema(schema).ok()?;
                    cache.insert(id.clone(), (schema.clone(), Arc::new(validator)));
                }
                validators.insert(name.clone(), cache[id].1.clone());
            }
        }
    }
    Some(validators)
}

async fn serve_socket(state: Arc<Inner>, mut socket: WebSocket) {
    let first = tokio::time::timeout(Duration::from_secs(5), socket.recv()).await;
    let Some(Ok(Message::Text(text))) = first.ok().flatten() else {
        return;
    };
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Auth {
        r#type: String,
        ticket: String,
    }
    let Ok(auth) = serde_json::from_str::<Auth>(&text) else {
        return;
    };
    if auth.r#type != "auth" {
        return;
    }
    let Ok(claims) = state.ticket_signer.verify_inner(&auth.ticket, None) else {
        return;
    };
    if claims.role == "watch" {
        return;
    }
    let inherited_channels = claims.inherited_channels;
    let service = claims.role == "service";
    let key = (claims.hub_path, claims.room);
    let user = claims.user;
    let Some((record, channels)) = resolve(&state, &key.0).await else {
        return;
    };
    if !paired(&record, &user, &key.1).await {
        return;
    }
    let local_channels = channels.is_some();
    let channels = channels.or_else(|| inherited_channels.as_ref().map(|p| p.channels.clone()));
    let Some(validators) = build_validators(
        &state,
        &channels,
        local_channels,
        inherited_channels.as_ref(),
    )
    .await
    else {
        return;
    };
    let (tx, mut rx) = mpsc::channel::<Message>(limit("LIVEENGINE_LIVE_SEND_QUEUE", 64));
    let (disconnect, mut disconnected) = tokio::sync::watch::channel(false);
    {
        let mut rooms = state.live_rooms.rooms.lock().await;
        if rooms.values().map(HashMap::len).sum::<usize>()
            >= limit("LIVEENGINE_LIVE_MAX_CONNECTIONS", 4096)
        {
            return;
        }
        let room = rooms.entry(key.clone()).or_default();
        if room.contains_key(&user)
            || room.len() >= limit("LIVEENGINE_LIVE_MAX_CONNECTIONS_PER_ROOM", 256)
        {
            return;
        }
        let mut pending = state.live_rooms.pending.lock().await;
        let bridges = state.live_rooms.bridges.lock().await;
        let resumed = pending.remove(&(key.clone(), user.clone())).is_some();
        let mut peers: Vec<_> = room.keys().cloned().collect();
        peers.extend(
            bridges
                .get(&key)
                .into_iter()
                .flat_map(|b| b.remote.keys().cloned()),
        );
        peers.extend(
            pending
                .keys()
                .filter(|(k, u)| k == &key && u != &user)
                .map(|(_, u)| u.clone()),
        );
        let Some(ice_servers) = ice_servers(&state.config, &user) else {
            return;
        };
        let welcome = serde_json::json!({"type":"welcome","peers":peers,"iceServers":ice_servers})
            .to_string();
        if tx.try_send(Message::Text(welcome.into())).is_err() {
            return;
        }
        if !resumed {
            let joined = Message::Text(
                serde_json::json!({"type":"presence","event":"join","peer":user})
                    .to_string()
                    .into(),
            );
            for queue in room.values() {
                queue.send(joined.clone(), false);
            }
            if !service {
                if let Some(bridge) = bridges.get(&key) {
                    let trace = [state.instance_id.clone()];
                    bridge.offer(None, &wire_presence(&trace, "join", Some(&user)), false);
                }
            }
        }
        room.insert(
            user.clone(),
            PeerQueue {
                sender: tx,
                disconnect,
                service,
            },
        );
    }
    let (mut sink, mut stream) = socket.split();
    let mut writer = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut last_activity = tokio::time::Instant::now();
    let mut last_ping = last_activity;
    let mut window = last_activity;
    let mut messages = 0usize;
    let mut bytes = 0usize;
    let mut channel_rates: HashMap<String, usize> = HashMap::new();
    let mut attached: Option<u64> = None;
    let link_name = format!("in:{user}");
    let trace = vec![state.instance_id.clone()];
    loop {
        let message = tokio::select! {
            result = stream.next() => match result { Some(Ok(message)) => message, _ => break },
            _ = disconnected.changed() => break,
            _ = &mut writer => break,
            _ = tick.tick() => {
                if last_activity.elapsed() >= Duration::from_secs(limit("LIVEENGINE_LIVE_IDLE_SECONDS", 60) as u64) { break }
                if !resolve(&state, &key.0).await.is_some_and(|(current, _)| match (&current.handler, &record.handler) { (Some(a), Some(b)) => Arc::ptr_eq(a, b), _ => false }) || !paired(&record, &user, &key.1).await { break }
                if last_ping.elapsed() >= Duration::from_secs(limit("LIVEENGINE_LIVE_PING_SECONDS", 20) as u64) {
                    let rooms = state.live_rooms.rooms.lock().await;
                    if let Some(queue) = rooms.get(&key).and_then(|r| r.get(&user)) { queue.send(Message::Ping(Default::default()), false); }
                    last_ping = tokio::time::Instant::now();
                }
                continue
            }
        };
        last_activity = tokio::time::Instant::now();
        if matches!(message, Message::Pong(_) | Message::Ping(_)) {
            continue;
        }
        let Message::Text(text) = message else { break };
        if window.elapsed() >= Duration::from_secs(1) {
            window = tokio::time::Instant::now();
            messages = 0;
            bytes = 0;
            channel_rates.clear();
        }
        messages += 1;
        bytes = bytes.saturating_add(text.len());
        // A link has its own, higher budget; over it, lossy frames are dropped, not the link.
        let linked = attached.is_some();
        let (max_messages, max_bytes) = if linked {
            link_budget()
        } else {
            (
                limit("LIVEENGINE_LIVE_MESSAGES_PER_SECOND", 100),
                limit("LIVEENGINE_LIVE_BYTES_PER_SECOND", 1_048_576),
            )
        };
        let over_budget = messages > max_messages;
        if bytes > max_bytes || (over_budget && !linked) {
            break;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            break;
        };
        let Some(object) = value.as_object_mut() else {
            break;
        };
        if object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| t.starts_with("link."))
        {
            // Only a service socket may carry linked frames; a peer sending one is refused.
            if !service {
                break;
            }
            if attached.is_none() {
                let Some((generation, snapshot)) =
                    attach(&state, &key, link_name.clone(), Some(&user), None).await
                else {
                    break;
                };
                attached = Some(generation);
                let sender = state
                    .live_rooms
                    .rooms
                    .lock()
                    .await
                    .get(&key)
                    .and_then(|room| room.get(&user))
                    .map(|queue| queue.sender.clone());
                if let Some(sender) = sender {
                    spawn_send(sender, snapshot);
                }
                // The first hello is answered by the snapshot just queued.
                if object.get("peer").is_none()
                    && object.get("type") == Some(&Value::from("link.presence"))
                {
                    continue;
                }
            }
            if !ingest(
                &state,
                &key,
                &link_name,
                &value,
                Some((&channels, &validators)),
                over_budget,
            )
            .await
            {
                break;
            }
            continue;
        }
        if object
            .get("from")
            .is_some_and(|from| from.as_str() != Some(&user))
            || !object.get("v").is_some_and(|v| v.is_number())
            || !object.get("type").is_some_and(|v| v.is_string())
            || !object.get("channel").is_some_and(|v| v.is_string())
            || !object.get("seq").is_some_and(|v| v.is_u64())
            || !object.contains_key("payload")
            || object.get("to").is_some_and(|v| !v.is_string())
        {
            break;
        }
        let channel = object["channel"].as_str().unwrap();
        let frame_type = object["type"].as_str().unwrap();
        let signal = matches!(frame_type, "signal.offer" | "signal.answer" | "signal.ice");
        if frame_type.starts_with("signal.") && !signal {
            break;
        }
        if signal {
            let Some(target) = object.get("to").and_then(Value::as_str).map(str::to_owned) else {
                break;
            };
            let target = target.as_str();
            if target == user || text.len() > limit("LIVEENGINE_SIGNAL_MAX_BYTES", 16384) {
                break;
            }
            let rooms = state.live_rooms.rooms.lock().await;
            let bridges = state.live_rooms.bridges.lock().await;
            object.insert("from".into(), Value::String(user.clone()));
            if let Some(queue) = rooms.get(&key).and_then(|room| room.get(target)) {
                queue.send(Message::Text(value.to_string().into()), false);
            } else if let Some(bridge) = bridges.get(&key).filter(|b| b.remote.contains_key(target))
            {
                forward(
                    &state.live_rooms,
                    bridge,
                    &state.live_rooms.frame_id(&state.instance_id),
                    &trace,
                    None,
                    Some(target),
                    &value,
                    false,
                );
            } else {
                break;
            }
            continue;
        }
        let mut lossy = false;
        if let Some(channels) = &channels {
            let Some(definition) = channels.get(channel) else {
                break;
            };
            lossy = matches!(definition.delivery, LiveDelivery::Lossy);
            let count = channel_rates.entry(channel.to_owned()).or_default();
            *count += 1;
            // maxRate is a per-peer, per-channel messages-per-second budget.
            if definition.max_rate.is_some_and(|max| *count > max) {
                break;
            }
        }
        if validators
            .get(channel)
            .is_some_and(|v| !v.is_valid(&object["payload"]))
        {
            break;
        }
        let target = object.get("to").and_then(Value::as_str).map(str::to_owned);
        object.insert("from".into(), Value::String(user.clone()));
        let frame = Message::Text(value.to_string().into());
        let rooms = state.live_rooms.rooms.lock().await;
        let bridges = state.live_rooms.bridges.lock().await;
        let bridge = bridges.get(&key);
        if let Some(room) = rooms.get(&key) {
            for (peer, queue) in room {
                if target.as_ref().map_or(peer != &user, |to| to == peer)
                    && !bridge.is_some_and(|b| b.is_link_user(peer))
                {
                    queue.send(frame.clone(), lossy);
                }
            }
        }
        if let Some(bridge) = bridge {
            let local = target
                .as_ref()
                .is_some_and(|to| rooms.get(&key).is_some_and(|room| room.contains_key(to)));
            if !local {
                forward(
                    &state.live_rooms,
                    bridge,
                    &state.live_rooms.frame_id(&state.instance_id),
                    &trace,
                    None,
                    target.as_deref(),
                    &value,
                    lossy,
                );
            }
        }
    }
    writer.abort();
    if let Some(generation) = attached {
        detach_link(&state, &key, &link_name, generation).await;
    }
    let generation = rand::random::<u64>();
    {
        let mut rooms = state.live_rooms.rooms.lock().await;
        if let Some(room) = rooms.get_mut(&key) {
            room.remove(&user);
        }
        state
            .live_rooms
            .pending
            .lock()
            .await
            .insert((key.clone(), user.clone()), generation);
    }
    let live = state.live_rooms.clone();
    let instance = state.instance_id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(
            limit("LIVEENGINE_LIVE_RESUME_MILLIS", 500) as u64,
        ))
        .await;
        let mut rooms = live.rooms.lock().await;
        let mut pending = live.pending.lock().await;
        let bridges = live.bridges.lock().await;
        if pending.get(&(key.clone(), user.clone())) != Some(&generation) {
            return;
        }
        pending.remove(&(key.clone(), user.clone()));
        if !service {
            if let Some(bridge) = bridges.get(&key) {
                bridge.offer(
                    None,
                    &wire_presence(&[instance], "leave", Some(&user)),
                    false,
                );
            }
        }
        if let Some(room) = rooms.get_mut(&key) {
            let left = Message::Text(
                serde_json::json!({"type":"presence","event":"leave","peer":user})
                    .to_string()
                    .into(),
            );
            for queue in room.values() {
                queue.send(left.clone(), false);
            }
            if room.is_empty() {
                rooms.remove(&key);
            }
        }
    });
}

#[cfg(test)]
mod queue_tests {
    use super::*;

    #[test]
    fn full_queue_drops_lossy_and_disconnects_reliable() {
        let (sender, mut receiver) = mpsc::channel(1);
        let (disconnect, disconnected) = tokio::sync::watch::channel(false);
        let queue = PeerQueue {
            sender,
            disconnect,
            service: false,
        };
        queue.send(Message::Text("first".into()), false);
        queue.send(Message::Text("lossy".into()), true);
        assert!(!*disconnected.borrow());
        assert_eq!(receiver.try_recv().unwrap(), Message::Text("first".into()));
        queue.send(Message::Text("first".into()), false);
        queue.send(Message::Text("reliable".into()), false);
        assert!(*disconnected.borrow());
    }
}
