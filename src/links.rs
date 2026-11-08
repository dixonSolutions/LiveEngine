//! Persistent service-to-service live connections.
use crate::hub::{endpoint, error, validated_remote_url, Inner};
use crate::live;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    protocol::{Message, WebSocketConfig},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkDefinition {
    pub id: String,
    pub address: String,
    pub remote_token: String,
    pub hub_id: String,
    pub user_id: String,
    pub server_id: String,
    /// With `local_room`, bridges that room of a local hub to the remote room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_hub_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_room: Option<String>,
}
struct Entry {
    definition: LinkDefinition,
    status: AtomicU8,
    running: AtomicBool,
}
impl Entry {
    fn new(definition: LinkDefinition) -> Self {
        Self {
            definition,
            status: AtomicU8::new(0),
            running: AtomicBool::new(false),
        }
    }
}
#[derive(Default)]
pub(crate) struct Links {
    entries: RwLock<HashMap<String, Arc<Entry>>>,
    started: AtomicBool,
}
impl Links {
    pub(crate) async fn definitions(&self) -> Vec<LinkDefinition> {
        self.entries
            .read()
            .await
            .values()
            .map(|e| e.definition.clone())
            .collect()
    }
    pub(crate) async fn replace(&self, definitions: Vec<LinkDefinition>) {
        *self.entries.write().await = definitions
            .into_iter()
            .map(|d| (d.id.clone(), Arc::new(Entry::new(d))))
            .collect();
    }
}
const MAX_CREDENTIALS: usize = 16;
/// Marks a request authenticated by a hub link credential rather than the admin token.
#[derive(Clone)]
pub(crate) struct CredentialAuth;
pub(crate) fn digest(token: &str) -> String {
    ring::digest::digest(&ring::digest::SHA256, token.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
/// Accept a link credential only for `POST /v1/hubs/{id}/tickets` on its own hub.
pub(crate) async fn credential_auth(
    state: &Inner,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
) -> Option<CredentialAuth> {
    if method != Method::POST {
        return None;
    }
    let mut parts = path.strip_prefix("/v1/hubs/")?.split('/');
    let (id, last) = (parts.next()?, parts.next()?);
    if last != "tickets" || parts.next().is_some() {
        return None;
    }
    let token = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    let supplied = digest(token);
    let hubs = state.hubs.read().await;
    let record = hubs.get(id)?;
    let mut matched = 0u8;
    for stored in &record.link_credentials {
        matched |= u8::from(bool::from(stored.as_bytes().ct_eq(supplied.as_bytes())));
    }
    (matched == 1).then_some(CredentialAuth)
}
pub(crate) async fn issue_credential(
    State(state): State<Arc<Inner>>,
    Path(id): Path<String>,
) -> Response {
    let mut hubs = state.hubs.write().await;
    let Some(record) = hubs.get_mut(&id) else {
        return error(StatusCode::NOT_FOUND, "hub not found");
    };
    if record.link_credentials.len() >= MAX_CREDENTIALS {
        return error(StatusCode::CONFLICT, "link credential limit reached");
    }
    let token = format!(
        "{:032x}{:032x}",
        rand::random::<u128>(),
        rand::random::<u128>()
    );
    record.link_credentials.push(digest(&token));
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"token":token})),
    )
        .into_response()
}
pub(crate) async fn revoke_credentials(
    State(state): State<Arc<Inner>>,
    Path(id): Path<String>,
) -> Response {
    match state.hubs.write().await.get_mut(&id) {
        Some(record) => {
            record.link_credentials.clear();
            StatusCode::NO_CONTENT.into_response()
        }
        None => error(StatusCode::NOT_FOUND, "hub not found"),
    }
}
fn valid(state: &Inner, d: &LinkDefinition) -> bool {
    let id_ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 128
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    };
    id_ok(&d.id)
        && id_ok(&d.hub_id)
        && !d.user_id.is_empty()
        && d.user_id.len() <= 256
        && !d.server_id.is_empty()
        && d.server_id.len() <= 256
        && match (&d.local_hub_id, &d.local_room) {
            (None, None) => true,
            (Some(hub), Some(room)) => id_ok(hub) && !room.is_empty() && room.len() <= 256,
            _ => false,
        }
        && d.remote_token.len() >= 24
        && d.remote_token.len() <= 4096
        && validated_remote_url(state, &d.address).is_ok()
}
pub(crate) fn restore(state: &Inner, definitions: &[LinkDefinition]) -> Result<(), String> {
    if definitions.len() > state.config.max_hubs {
        return Err("state file exceeds link limits".into());
    }
    let mut entries = state
        .links
        .entries
        .try_write()
        .map_err(|_| "state initialization failed")?;
    for d in definitions {
        if !valid(state, d)
            || entries
                .insert(d.id.clone(), Arc::new(Entry::new(d.clone())))
                .is_some()
        {
            return Err("invalid state file link".into());
        }
    }
    Ok(())
}
fn info(e: &Entry) -> serde_json::Value {
    let mut value = serde_json::json!({"id":e.definition.id,"hubId":e.definition.hub_id,"userId":e.definition.user_id,"serverId":e.definition.server_id,"status":match e.status.load(Ordering::Relaxed) { 1 => "connected", 2 => "unreachable", _ => "connecting" }});
    if let (Some(hub), Some(room)) = (&e.definition.local_hub_id, &e.definition.local_room) {
        value["localHubId"] = hub.clone().into();
        value["localRoom"] = room.clone().into();
    }
    value
}
pub(crate) async fn list(State(state): State<Arc<Inner>>) -> Json<serde_json::Value> {
    let mut links: Vec<_> = state
        .links
        .entries
        .read()
        .await
        .values()
        .map(|e| info(e))
        .collect();
    links.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    let (dropped, rejected) = state.live_rooms.counters();
    Json(serde_json::json!({"links":links,"frames":{"dropped":dropped,"rejected":rejected}}))
}
pub(crate) async fn get_one(State(state): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    match state.links.entries.read().await.get(&id) {
        Some(e) => Json(info(e)).into_response(),
        None => error(StatusCode::NOT_FOUND, "link not found"),
    }
}
pub(crate) async fn create(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<LinkDefinition>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(d)) = payload else {
        return error(StatusCode::BAD_REQUEST, "invalid link definition");
    };
    if !valid(&state, &d) {
        return error(StatusCode::BAD_REQUEST, "invalid link definition");
    }
    let mut entries = state.links.entries.write().await;
    if entries.contains_key(&d.id) {
        return error(StatusCode::CONFLICT, "link already exists");
    }
    if entries.len() >= state.config.max_hubs {
        return error(StatusCode::SERVICE_UNAVAILABLE, "link capacity reached");
    }
    let e = Arc::new(Entry::new(d));
    let result = info(&e);
    entries.insert(e.definition.id.clone(), e);
    (StatusCode::CREATED, Json(result)).into_response()
}
#[derive(Deserialize)]
pub(crate) struct DeleteQuery {
    id: String,
}
pub(crate) async fn delete_query(
    State(state): State<Arc<Inner>>,
    Query(q): Query<DeleteQuery>,
) -> Response {
    remove(&state, &q.id).await
}
pub(crate) async fn delete_one(
    State(state): State<Arc<Inner>>,
    Path(id): Path<String>,
) -> Response {
    remove(&state, &id).await
}
async fn remove(state: &Inner, id: &str) -> Response {
    if state.links.entries.write().await.remove(id).is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "link not found")
    }
}

pub(crate) fn start(state: &Arc<Inner>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    if state.links.started.swap(true, Ordering::AcqRel) {
        return;
    }
    let weak = Arc::downgrade(state);
    handle.spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let _guard = if state.config.state_file.is_some() {
                Some(state.mutation_lock.lock().await)
            } else {
                None
            };
            let entries: Vec<_> = state.links.entries.read().await.values().cloned().collect();
            for entry in entries {
                if !entry.running.swap(true, Ordering::AcqRel) {
                    let weak = Arc::downgrade(&state);
                    tokio::spawn(async move {
                        worker(weak, entry).await;
                    });
                }
            }
            drop(_guard);
            drop(state);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
}
/// Convert a queued room message for the outbound socket.
fn wire(message: axum::extract::ws::Message) -> Option<Message> {
    match message {
        axum::extract::ws::Message::Text(text) => Some(Message::Text(text.as_str().into())),
        _ => None,
    }
}
async fn present(state: &Inner, entry: &Arc<Entry>) -> bool {
    state
        .links
        .entries
        .read()
        .await
        .get(&entry.definition.id)
        .is_some_and(|e| Arc::ptr_eq(e, entry))
}
async fn worker(weak: std::sync::Weak<Inner>, entry: Arc<Entry>) {
    let mut backoff = Duration::from_millis(250);
    loop {
        let Some(state) = weak.upgrade() else { break };
        if !present(&state, &entry).await {
            break;
        }
        let connected = connect(state, &entry).await;
        entry.status.store(2, Ordering::Relaxed);
        if connected {
            backoff = Duration::from_millis(250);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
    entry.running.store(false, Ordering::Release);
}
async fn connect(state: Arc<Inner>, entry: &Arc<Entry>) -> bool {
    let d = &entry.definition;
    let Ok(base) = validated_remote_url(&state, &d.address) else {
        return false;
    };
    let Ok(ticket_url) = endpoint(&base, &format!("v1/hubs/{}/tickets", d.hub_id)) else {
        return false;
    };
    let Ok(client) = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(state.config.request_timeout)
        .build()
    else {
        return false;
    };
    let Ok(response) = client
        .post(ticket_url)
        .bearer_auth(&d.remote_token)
        .json(&serde_json::json!({"userId":d.user_id,"serverId":d.server_id,"role":"service"}))
        .send()
        .await
    else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { return false };
        if body.len().saturating_add(chunk.len()) > state.config.max_response_bytes {
            return false;
        }
        body.extend_from_slice(&chunk);
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return false;
    };
    let Some(ticket) = value["ticket"].as_str() else {
        return false;
    };
    let Ok(mut url) = endpoint(&base, "v1/live") else {
        return false;
    };
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    if url.set_scheme(scheme).is_err() {
        return false;
    }
    let Ok(request) = url.as_str().into_client_request() else {
        return false;
    };
    let config = WebSocketConfig::default()
        .max_message_size(Some(65536))
        .max_frame_size(Some(65536));
    let Ok(Ok((mut socket, _))) = tokio::time::timeout(
        state.config.request_timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(config), false),
    )
    .await
    else {
        return false;
    };
    let auth = serde_json::json!({"type":"auth","ticket":ticket}).to_string();
    if socket.send(Message::Text(auth.into())).await.is_err() {
        return false;
    }
    let Ok(Some(Ok(Message::Text(welcome)))) =
        tokio::time::timeout(state.config.request_timeout, socket.next()).await
    else {
        return false;
    };
    if serde_json::from_str::<serde_json::Value>(&welcome)
        .ok()
        .and_then(|v| v["type"].as_str().map(str::to_owned))
        .as_deref()
        != Some("welcome")
    {
        return false;
    }
    // A link with a local hub and room also carries that room's frames and presence.
    let bridge = match (&d.local_hub_id, &d.local_room) {
        (Some(hub), Some(room)) => Some(live::room_key(&state, hub, room)),
        _ => None,
    };
    let mut attach = match &bridge {
        Some(key) => match live::attach_link(&state, key, &d.id).await {
            Some(attach) => Some(attach),
            None => return false,
        },
        None => None,
    };
    entry.status.store(1, Ordering::Relaxed);
    let weak = Arc::downgrade(&state);
    drop(state);
    let name = format!("out:{}", d.id);
    let generation = attach.as_ref().map_or(0, |a| a.generation);
    let snapshot = attach.as_mut().map(|a| std::mem::take(&mut a.snapshot));
    let (mut rx, mut disconnected) = match attach {
        Some(a) => (Some(a.rx), Some(a.disconnected)),
        None => (None, None),
    };
    let mut healthy = true;
    for message in snapshot.into_iter().flatten() {
        if let Some(message) = wire(message) {
            if socket.send(message).await.is_err() {
                healthy = false;
                break;
            }
        }
    }
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut window = tokio::time::Instant::now();
    let mut bytes = 0usize;
    let mut count = 0usize;
    let mut last_frame = tokio::time::Instant::now();
    if healthy {
        loop {
            tokio::select! {
                _ = tick.tick() => { if last_frame.elapsed() > Duration::from_secs(60) { break; } let Some(state) = weak.upgrade() else { break }; if !present(&state, entry).await { let _ = socket.close(None).await; break; } },
                queued = async { match rx.as_mut() { Some(rx) => rx.recv().await, None => std::future::pending().await } } => { match queued { Some(message) => { if let Some(message) = wire(message) { if socket.send(message).await.is_err() { break; } } }, None => break } },
                _ = async { match disconnected.as_mut() { Some(d) => { let _ = d.changed().await; }, None => std::future::pending().await } } => break,
                msg = socket.next() => { last_frame = tokio::time::Instant::now(); match msg { Some(Ok(Message::Text(s))) => {
                    if window.elapsed() >= Duration::from_secs(1) { window = tokio::time::Instant::now(); bytes = 0; count = 0; }
                    bytes += s.len(); count += 1;
                    let (max_count, max_bytes) = if bridge.is_some() { live::link_budget() } else { (100, 1_048_576) };
                    if bytes > max_bytes || (count > max_count && bridge.is_none()) { break; }
                    let over_budget = count > max_count;
                    if let (Some(key), Ok(value)) = (&bridge, serde_json::from_str::<serde_json::Value>(&s)) {
                        // Ordinary room frames are not carried; only the two link frame types are.
                        if !matches!(value["type"].as_str(), Some("link.frame" | "link.presence" | "link.control")) { continue; }
                        if let Some(state) = weak.upgrade() { live::ingest(&state, key, &name, &value, None, over_budget).await; }
                    }
                }, Some(Ok(Message::Ping(data))) => { if socket.send(Message::Pong(data)).await.is_err() { break; } }, Some(Ok(Message::Pong(_))) => {}, _ => break } }
            }
        }
    }
    if let (Some(key), Some(state)) = (&bridge, weak.upgrade()) {
        live::detach_link(&state, key, &name, generation).await;
    }
    true
}
