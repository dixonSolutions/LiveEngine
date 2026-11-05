//! Persistent service-to-service live connections.
use crate::hub::{endpoint, error, validated_remote_url, Inner};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
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
    serde_json::json!({"id":e.definition.id,"hubId":e.definition.hub_id,"userId":e.definition.user_id,"serverId":e.definition.server_id,"status":match e.status.load(Ordering::Relaxed) { 1 => "connected", 2 => "unreachable", _ => "connecting" }})
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
    Json(serde_json::json!({"links":links}))
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
    entry.status.store(1, Ordering::Relaxed);
    let weak = Arc::downgrade(&state);
    drop(state);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut window = tokio::time::Instant::now();
    let mut bytes = 0usize;
    let mut count = 0usize;
    let mut last_frame = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = tick.tick() => { if last_frame.elapsed() > Duration::from_secs(60) { break; } let Some(state) = weak.upgrade() else { break }; if !present(&state, entry).await { let _ = socket.close(None).await; break; } },
            msg = socket.next() => { last_frame = tokio::time::Instant::now(); match msg { Some(Ok(Message::Text(s))) => { if window.elapsed() >= Duration::from_secs(1) { window = tokio::time::Instant::now(); bytes = 0; count = 0; } bytes += s.len(); count += 1; if bytes > 1_048_576 || count > 100 { break; } }, Some(Ok(Message::Ping(data))) => { if socket.send(Message::Pong(data)).await.is_err() { break; } }, Some(Ok(Message::Pong(_))) => {}, _ => break } }
        }
    }
    true
}
