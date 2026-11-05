//! Short-lived, single-use credentials for live room connections.
use crate::hub::{
    compile_schema, error, expire_hubs, forward_route, header_depth, HubRecord, Inner, LiveChannel,
    LiveDelivery, RouteRequest,
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
    collections::HashMap,
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
    let mut validators = HashMap::new();
    if let Some(channels) = &channels {
        let schemas = state.schemas.read().await;
        let mut cache = state.live_rooms.validators.lock().await;
        cache.retain(|id, _| schemas.contains_key(id));
        for (name, channel) in channels {
            if let Some(id) = &channel.schema_ref {
                let Some(schema) = (if local_channels {
                    schemas.get(id)
                } else {
                    inherited_channels.as_ref().and_then(|p| p.schemas.get(id))
                }) else {
                    return;
                };
                if !cache.get(id).is_some_and(|(saved, _)| saved == schema) {
                    let Ok(validator) = compile_schema(schema) else {
                        return;
                    };
                    cache.insert(id.clone(), (schema.clone(), Arc::new(validator)));
                }
                validators.insert(name.clone(), cache[id].1.clone());
            }
        }
    }
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
        let resumed = pending.remove(&(key.clone(), user.clone())).is_some();
        let mut peers: Vec<_> = room.keys().cloned().collect();
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
        }
        room.insert(
            user.clone(),
            PeerQueue {
                sender: tx,
                disconnect,
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
        if messages > limit("LIVEENGINE_LIVE_MESSAGES_PER_SECOND", 100)
            || bytes > limit("LIVEENGINE_LIVE_BYTES_PER_SECOND", 1_048_576)
        {
            break;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            break;
        };
        let Some(object) = value.as_object_mut() else {
            break;
        };
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
            let Some(target) = object.get("to").and_then(Value::as_str) else {
                break;
            };
            if target == user || text.len() > limit("LIVEENGINE_SIGNAL_MAX_BYTES", 16384) {
                break;
            }
            let rooms = state.live_rooms.rooms.lock().await;
            let Some(queue) = rooms.get(&key).and_then(|room| room.get(target)) else {
                break;
            };
            object.insert("from".into(), Value::String(user.clone()));
            queue.send(Message::Text(value.to_string().into()), false);
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
        if let Some(room) = rooms.get(&key) {
            for (peer, queue) in room {
                if target.as_ref().map_or(peer != &user, |to| to == peer) {
                    queue.send(frame.clone(), lossy);
                }
            }
        }
    }
    writer.abort();
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
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(
            limit("LIVEENGINE_LIVE_RESUME_MILLIS", 500) as u64,
        ))
        .await;
        let mut rooms = live.rooms.lock().await;
        let mut pending = live.pending.lock().await;
        if pending.get(&(key.clone(), user.clone())) != Some(&generation) {
            return;
        }
        pending.remove(&(key.clone(), user.clone()));
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
        let queue = PeerQueue { sender, disconnect };
        queue.send(Message::Text("first".into()), false);
        queue.send(Message::Text("lossy".into()), true);
        assert!(!*disconnected.borrow());
        assert_eq!(receiver.try_recv().unwrap(), Message::Text("first".into()));
        queue.send(Message::Text("first".into()), false);
        queue.send(Message::Text("reliable".into()), false);
        assert!(*disconnected.borrow());
    }
}
