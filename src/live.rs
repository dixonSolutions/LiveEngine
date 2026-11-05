//! Short-lived, single-use credentials for live room connections.
use crate::hub::{error, expire_hubs, forward_route, header_depth, Inner, RouteRequest};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
        if !matches!(role.as_str(), "peer" | "service") || ttl == 0 {
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
        };
        let encoded = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).map_err(|_| "cannot encode ticket")?);
        let mac = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.key),
            encoded.as_bytes(),
        );
        Ok((
            format!("{}.{}", encoded, URL_SAFE_NO_PAD.encode(mac.as_ref())),
            expiry,
        ))
    }
    /// Verify signature, expiry and binding, consuming the ticket on success.
    pub fn verify(
        &self,
        ticket: &str,
        hub_path: &[String],
        room: &str,
    ) -> Result<TicketClaims, &'static str> {
        self.verify_inner(ticket, Some((hub_path, room)))
    }
    fn verify_inner(
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
            || !matches!(claims.role.as_str(), "peer" | "service")
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
    headers: HeaderMap,
    payload: Result<Json<TicketRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    issue_ticket(state, id, headers, payload.ok().map(|Json(v)| v)).await
}
pub(crate) async fn issue_ticket(
    state: Arc<Inner>,
    id: String,
    headers: HeaderMap,
    request: Option<TicketRequest>,
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
        match state.ticket_signer.issue(
            path,
            request.server_id,
            request.user_id,
            request.role,
            state.config.ticket_ttl.as_secs(),
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
    fn send(&self, message: Message) {
        if self.sender.try_send(message).is_err() {
            let _ = self.disconnect.send(true);
        }
    }
}

#[derive(Default)]
pub(crate) struct LiveRooms {
    rooms: AsyncMutex<HashMap<RoomKey, HashMap<String, PeerQueue>>>,
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
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| serve_socket(state, socket))
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
    let key = (claims.hub_path, claims.room);
    let user = claims.user;
    let (tx, mut rx) = mpsc::channel::<Message>(64);
    let (disconnect, mut disconnected) = tokio::sync::watch::channel(false);
    {
        let mut rooms = state.live_rooms.rooms.lock().await;
        let room = rooms.entry(key.clone()).or_default();
        if room.contains_key(&user) {
            return;
        }
        let peers: Vec<_> = room.keys().cloned().collect();
        let welcome = serde_json::json!({"type":"welcome","peers":peers}).to_string();
        if tx.try_send(Message::Text(welcome.into())).is_err() {
            return;
        }
        let joined = Message::Text(
            serde_json::json!({"type":"presence","event":"join","peer":user})
                .to_string()
                .into(),
        );
        for queue in room.values() {
            queue.send(joined.clone());
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
    loop {
        let message = tokio::select! {
            result = stream.next() => match result { Some(Ok(message)) => message, _ => break },
            _ = disconnected.changed() => break,
            _ = &mut writer => break,
        };
        let Message::Text(text) = message else { break };
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
        let target = object.get("to").and_then(Value::as_str).map(str::to_owned);
        object.insert("from".into(), Value::String(user.clone()));
        let frame = Message::Text(value.to_string().into());
        let rooms = state.live_rooms.rooms.lock().await;
        if let Some(room) = rooms.get(&key) {
            for (peer, queue) in room {
                if target.as_ref().map_or(peer != &user, |to| to == peer) {
                    queue.send(frame.clone());
                }
            }
        }
    }
    {
        let mut rooms = state.live_rooms.rooms.lock().await;
        if let Some(room) = rooms.get_mut(&key) {
            room.remove(&user);
            let left = Message::Text(
                serde_json::json!({"type":"presence","event":"leave","peer":user})
                    .to_string()
                    .into(),
            );
            for queue in room.values() {
                queue.send(left.clone());
            }
            if room.is_empty() {
                rooms.remove(&key);
            }
        }
    }
    writer.abort();
}
