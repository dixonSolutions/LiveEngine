//! Bounded, read-only subscriptions to the local hub tree.
use crate::hub::{
    endpoint, error, expire_hubs, outgoing_trace, trace, validated_remote_url, HubMode, Inner,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as UpstreamMessage};

#[derive(Default)]
pub(crate) struct Upstreams(Mutex<HashMap<String, Weak<Upstream>>>);

struct Upstream {
    state: tokio::sync::watch::Receiver<Option<Vec<Value>>>,
    depth: tokio::sync::watch::Sender<usize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Upstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Upstreams {
    fn subscribe(
        &self,
        state: &Inner,
        address: &str,
        token: &str,
        depth: usize,
        trace: &str,
    ) -> Option<(String, Arc<Upstream>)> {
        let base = validated_remote_url(state, address).ok()?;
        let mut url = endpoint(&base, "v1/live/registry").ok()?;
        url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
            .ok()?;
        let key = format!("{}\n{}\n{}", url, token, trace);
        let mut entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, v| v.strong_count() > 0);
        if let Some(existing) = entries.get(&key).and_then(Weak::upgrade) {
            existing.depth.send_if_modified(|d| {
                if depth > *d {
                    *d = depth;
                    true
                } else {
                    false
                }
            });
            return Some((key, existing));
        }
        let (tx, rx) = tokio::sync::watch::channel(None);
        let (depth_tx, mut depth_rx) = tokio::sync::watch::channel(depth);
        let token = token.to_owned();
        let trace = trace.to_owned();
        let max_bytes = state.config.max_response_bytes;
        let timeout = state.config.request_timeout;
        let task = tokio::spawn(async move {
            let mut delay = Duration::from_millis(100);
            loop {
                let depth = *depth_rx.borrow_and_update();
                let mut request = match url.as_str().into_client_request() {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let Ok(header) = trace.parse() else { return };
                request.headers_mut().insert("x-liveengine-trace", header);
                let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                    .max_message_size(Some(max_bytes))
                    .max_frame_size(Some(max_bytes));
                if let Ok(Ok((mut ws, _))) = tokio::time::timeout(
                    timeout,
                    tokio_tungstenite::connect_async_with_config(request, Some(config), false),
                )
                .await
                {
                    let auth =
                        json!({"type":"auth","token":token,"path":[],"depth":depth}).to_string();
                    if ws.send(UpstreamMessage::Text(auth.into())).await.is_ok() {
                        let mut sequence = None;
                        let mut resnapshot_pending = false;
                        let mut ping = tokio::time::interval(Duration::from_secs(15));
                        ping.tick().await;
                        let mut last_message = tokio::time::Instant::now();
                        loop {
                            let message = tokio::select! {
                                result = ws.next() => match result { Some(Ok(message)) => message, _ => break },
                                _ = ping.tick() => {
                                    if last_message.elapsed() > Duration::from_secs(45) || ws.send(UpstreamMessage::Ping(Vec::new().into())).await.is_err() { break; }
                                    continue;
                                }
                                _ = depth_rx.changed() => break,
                            };
                            last_message = tokio::time::Instant::now();
                            match message {
                                UpstreamMessage::Text(text) => {
                                    let Ok(v) = serde_json::from_str::<Value>(&text) else {
                                        break;
                                    };
                                    let Some(seq) = v["seq"].as_u64() else { break };
                                    if v["type"] == "snapshot" {
                                        let Some(nodes) = v["hubs"].as_array() else {
                                            break;
                                        };
                                        let mut budget = 4096;
                                        let Some(nodes) = clean_nodes(nodes, depth, &mut budget)
                                        else {
                                            break;
                                        };
                                        tx.send_replace(Some(nodes));
                                        sequence = Some(seq);
                                        resnapshot_pending = false;
                                        delay = Duration::from_millis(100);
                                    } else {
                                        if sequence.is_none_or(|last| seq != last + 1) {
                                            sequence = None;
                                        } else {
                                            sequence = Some(seq);
                                        }
                                        if !resnapshot_pending
                                            && ws
                                                .send(UpstreamMessage::Text(
                                                    json!({"type":"resnapshot"}).to_string().into(),
                                                ))
                                                .await
                                                .is_err()
                                        {
                                            break;
                                        }
                                        resnapshot_pending = true;
                                    }
                                }
                                UpstreamMessage::Ping(data) => {
                                    if ws.send(UpstreamMessage::Pong(data)).await.is_err() {
                                        break;
                                    }
                                }
                                UpstreamMessage::Pong(_) => {}
                                _ => break,
                            }
                        }
                    }
                }
                tx.send_replace(None);
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        });
        let upstream = Arc::new(Upstream {
            state: rx,
            depth: depth_tx,
            task,
        });
        entries.insert(key.clone(), Arc::downgrade(&upstream));
        Some((key, upstream))
    }
}

// Only the public live-node fields may cross a trust boundary.
fn clean_nodes(nodes: &[Value], depth: usize, budget: &mut usize) -> Option<Vec<Value>> {
    if depth == 0 || nodes.len() > *budget {
        return None;
    }
    let mut clean = Vec::with_capacity(nodes.len());
    for node in nodes {
        *budget = budget.checked_sub(1)?;
        let id = node["id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 128)?;
        let mode = node["mode"]
            .as_str()
            .filter(|m| matches!(*m, "internal" | "external" | "central"))?;
        let status = node["status"]
            .as_str()
            .filter(|s| matches!(*s, "ok" | "stale" | "unreachable"))?;
        let live = depth > 1 && node["childrenLive"] == true && mode == "central";
        let mut item = json!({"id":id,"mode":mode,"status":status,"roomCount":node["roomCount"].as_u64()?,"memberCount":node["memberCount"].as_u64()?,"childrenLive":live});
        if live {
            item["hubs"] = json!(clean_nodes(node["hubs"].as_array()?, depth - 1, budget)?);
        }
        clean.push(item);
    }
    Some(clean)
}

/// Environment setting for the default depth of local live tree subscriptions.
pub(crate) const LIVE_TREE_DEPTH_ENV: &str = "LIVEENGINE_LIVE_TREE_DEPTH";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchRequest {
    #[serde(default)]
    path: Vec<String>,
    depth: usize,
}

pub(crate) async fn watch_ticket(
    State(state): State<Arc<Inner>>,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    match body {
        Ok(Json(body)) => issue_watch(state, body).await,
        Err(_) => error(StatusCode::BAD_REQUEST, "invalid watch request"),
    }
}

pub(crate) async fn issue_watch(state: Arc<Inner>, body: Value) -> Response {
    let Ok(request) = serde_json::from_value::<WatchRequest>(body) else {
        return error(StatusCode::BAD_REQUEST, "invalid watch request");
    };
    let mut path = state.hub_path.clone();
    path.extend(request.path);
    if request.depth == 0
        || path.len() >= state.config.max_depth
        || resolve(state.clone(), &path[state.hub_path.len()..])
            .await
            .is_none()
    {
        return error(StatusCode::BAD_REQUEST, "invalid watch scope");
    }
    match state.ticket_signer.issue(
        path.clone(),
        "registry-watch".into(),
        request
            .depth
            .min(state.config.max_depth - path.len())
            .to_string(),
        "watch".into(),
        state.config.ticket_ttl.as_secs(),
    ) {
        Ok((ticket, expiry)) => Json(json!({"ticket":ticket,"expiresAt":expiry})).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not issue watch ticket",
        ),
    }
}

pub(crate) async fn registry_socket(
    State(state): State<Arc<Inner>>,
    peer: Option<axum::Extension<axum::extract::ConnectInfo<std::net::SocketAddr>>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if let Err(status) = trace(&state, &headers) {
        return error(status, "invalid or repeated forwarding trace");
    }
    let forward_trace = outgoing_trace(&state, &headers);
    let mut origins = headers.get_all(axum::http::header::ORIGIN).iter();
    if let Some(origin) = origins.next() {
        if origins.next().is_some()
            || !origin
                .to_str()
                .ok()
                .is_some_and(|o| state.config.live_allowed_origins.iter().any(|a| a == o))
        {
            return error(StatusCode::FORBIDDEN, "origin not allowed");
        }
    }
    let Some(preauth) = crate::live::pre_auth_slot(&state, peer.map(|p| p.0 .0)) else {
        return crate::live::pre_auth_refusal();
    };
    let slots = state.registry_subscribers.clone();
    let Ok(permit) = slots.try_acquire_owned() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "subscriber capacity reached",
        );
    };
    ws.max_message_size(state.config.max_request_bytes)
        .max_frame_size(state.config.max_request_bytes)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            serve(state, socket, forward_trace, preauth).await;
        })
        .into_response()
}

async fn receive(socket: &mut WebSocket) -> Option<Value> {
    let Some(Ok(Message::Text(text))) = tokio::time::timeout(Duration::from_secs(5), socket.recv())
        .await
        .ok()
        .flatten()
    else {
        return None;
    };
    serde_json::from_str(&text).ok()
}
async fn send(socket: &mut WebSocket, value: Value) -> bool {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(value.to_string().into())),
    )
    .await
    .is_ok_and(|v| v.is_ok())
}
/// `preauth` is released once the subscriber has authenticated.
async fn serve(
    root: Arc<Inner>,
    mut socket: WebSocket,
    forward_trace: String,
    preauth: crate::live::PreAuthPermit,
) {
    let Some(auth) = receive(&mut socket).await else {
        return;
    };
    if auth["type"] != "auth" {
        return;
    }
    let token = auth
        .get("token")
        .or_else(|| auth.get("bearer"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .strip_prefix("Bearer ")
        .unwrap_or_else(|| {
            auth.get("token")
                .or_else(|| auth.get("bearer"))
                .and_then(Value::as_str)
                .unwrap_or("")
        });
    let admin: bool = bool::from(token.as_bytes().ct_eq(root.config.token.as_bytes()))
        || crate::accounts::is_admin(&root, token).await;
    let claims = if admin {
        None
    } else {
        let Some(ticket) = auth.get("ticket").and_then(Value::as_str) else {
            return;
        };
        let Ok(claims) = root.ticket_signer.verify_inner(ticket, None) else {
            return;
        };
        if claims.room != "registry-watch" || claims.role != "watch" {
            return;
        }
        Some(claims)
    };
    drop(preauth);
    let request_value = if auth.get("path").is_some() {
        auth
    } else {
        match receive(&mut socket).await {
            Some(v) => v,
            None => return,
        }
    };
    let Some(path) = request_value
        .get("path")
        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
    else {
        return;
    };
    let Some(depth) = request_value
        .get("depth")
        .and_then(Value::as_u64)
        .and_then(|d| usize::try_from(d).ok())
        .filter(|d| *d > 0)
    else {
        return;
    };
    if path.len() >= root.config.max_depth {
        return;
    }
    let mut allowed_depth = root.config.max_depth - path.len();
    if let Some(c) = claims {
        let Ok(scope_depth) = c.user.parse::<usize>() else {
            return;
        };
        if !path.starts_with(&c.hub_path) || path.len() - c.hub_path.len() >= scope_depth {
            return;
        }
        allowed_depth = allowed_depth.min(scope_depth - (path.len() - c.hub_path.len()));
    }
    let Some((state, limit)) = resolve(root.clone(), &path).await else {
        return;
    };
    let effective = depth.min(limit).min(allowed_depth);
    let mut seq = 0u64;
    let mut subscriptions = HashMap::new();
    let mut previous = scoped_snapshot(&state, effective, &forward_trace, &mut subscriptions).await;
    if !send(&mut socket, json!({"type":"snapshot","seq":seq,"path":path,"depth":effective,"effectiveDepth":effective,"hubs":previous})).await { return; }
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let current = scoped_snapshot(&state, effective, &forward_trace, &mut subscriptions).await;
                let mut old = BTreeMap::new(); let mut new = BTreeMap::new();
                flatten(&previous, &mut Vec::new(), &mut old); flatten(&current, &mut Vec::new(), &mut new);
                for (p, node) in &old { if !new.contains_key(p) { seq += 1; if !send(&mut socket,json!({"type":"removed","seq":seq,"path":p})).await { return; } } else { let _ = node; } }
                for (p, node) in &new { let kind = match old.get(p) { None => Some("added"), Some(before) if before["status"] != node["status"] => Some("status"), Some(before) if *before != *node => Some("updated"), _ => None }; if let Some(kind) = kind { seq += 1; if !send(&mut socket,json!({"type":kind,"seq":seq,"path":p,"hub":node})).await { return; } } }
                previous = current;
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(text))) if serde_json::from_str::<Value>(&text).ok().is_some_and(|v| v["type"] == "resnapshot") => { previous = scoped_snapshot(&state, effective, &forward_trace, &mut subscriptions).await; if !send(&mut socket,json!({"type":"snapshot","seq":seq,"path":path,"depth":effective,"effectiveDepth":effective,"hubs":previous})).await { return; } },
                Some(Ok(Message::Ping(data))) => { if socket.send(Message::Pong(data)).await.is_err() { return; } },
                Some(Ok(Message::Pong(_))) => {}, _ => return,
            }
        }
    }
}
enum Scope {
    Local(Arc<Inner>),
    Remote {
        owner: Arc<Inner>,
        address: String,
        token: String,
        path: Vec<String>,
    },
}

async fn resolve(root: Arc<Inner>, path: &[String]) -> Option<(Scope, usize)> {
    let mut current = root;
    let mut limit = current.config.live_tree_depth.min(current.config.max_depth);
    for (index, id) in path.iter().enumerate() {
        let record = current.hubs.read().await.get(id).cloned()?;
        if record.info.mode != HubMode::Central {
            return None;
        }
        if let Some(depth) = record.info.live_depth {
            limit = depth;
        }
        if let Some(child) = record.child {
            current = child;
        } else {
            return Some((
                Scope::Remote {
                    owner: current,
                    address: record.info.address?,
                    token: record.remote_token?,
                    path: path[index + 1..].to_vec(),
                },
                limit,
            ));
        }
    }
    Some((Scope::Local(current), limit))
}
async fn scoped_snapshot(
    scope: &Scope,
    depth: usize,
    trace: &str,
    subscriptions: &mut HashMap<String, Arc<Upstream>>,
) -> Vec<Value> {
    match scope {
        Scope::Local(state) => snapshot(state.clone(), depth, trace, subscriptions).await,
        Scope::Remote {
            owner,
            address,
            token,
            path,
        } => {
            let mut used = HashMap::new();
            let mut result = Vec::new();
            if let Some((key, upstream)) =
                owner
                    .registry_upstreams
                    .subscribe(owner, address, token, depth + path.len(), trace)
            {
                if let Some(mut nodes) = upstream.state.borrow().clone() {
                    let mut found = true;
                    for segment in path {
                        if let Some(node) = nodes
                            .iter()
                            .find(|n| n["id"] == *segment && n["mode"] == "central")
                        {
                            nodes = node["hubs"].as_array().cloned().unwrap_or_default();
                        } else {
                            found = false;
                            break;
                        }
                    }
                    if found {
                        result = clean_nodes(&nodes, depth, &mut 4096).unwrap_or_default();
                    }
                }
                used.insert(key, upstream);
            }
            *subscriptions = used;
            result
        }
    }
}
async fn snapshot(
    state: Arc<Inner>,
    depth: usize,
    trace: &str,
    subscriptions: &mut HashMap<String, Arc<Upstream>>,
) -> Vec<Value> {
    let mut used = HashMap::new();
    let result = snapshot_inner(state, depth, trace, &mut used).await;
    *subscriptions = used;
    result
}
fn snapshot_inner<'a>(
    state: Arc<Inner>,
    depth: usize,
    trace: &'a str,
    used: &'a mut HashMap<String, Arc<Upstream>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Value>> + Send + 'a>> {
    Box::pin(async move {
        if depth == 0 {
            return vec![];
        }
        let mut records: Vec<_> = {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            hubs.values().cloned().collect()
        };
        records.sort_by(|a, b| a.info.id.cmp(&b.info.id));
        let mut nodes = Vec::new();
        for r in records {
            let (rooms, members) = if let Some(handler) = r.handler.clone() {
                state
                    .run_on_hub(&handler, |h| {
                        let Ok(s) = h.call("stats", vec![]) else {
                            return (0, 0);
                        };
                        (
                            s["servers"].as_u64().unwrap_or(0),
                            s["paired"].as_u64().unwrap_or(0),
                        )
                    })
                    .await
                    .unwrap_or((0, 0))
            } else {
                (0, 0)
            };
            let central = r.info.mode == HubMode::Central;
            let child_depth = r
                .info
                .live_depth
                .unwrap_or(depth.saturating_sub(1))
                .min(depth.saturating_sub(1));
            let mut status = if r.info.stale { "stale" } else { "ok" };
            let mut children = None;
            if central && child_depth > 0 {
                if let Some(child) = r.child {
                    children = Some(snapshot_inner(child, child_depth, trace, used).await);
                } else if let (Some(address), Some(token)) =
                    (r.info.address.as_deref(), r.remote_token.as_deref())
                {
                    if let Some((key, upstream)) = state.registry_upstreams.subscribe(
                        &state,
                        address,
                        token,
                        child_depth,
                        trace,
                    ) {
                        children = upstream
                            .state
                            .borrow()
                            .as_ref()
                            .and_then(|nodes| clean_nodes(nodes, child_depth, &mut 4096));
                        used.insert(key, upstream);
                    }
                    if children.is_none() {
                        status = "unreachable";
                    }
                }
            }
            let live = children.is_some();
            let mut node = json!({"id":r.info.id,"mode":r.info.mode,"status":status,"roomCount":rooms,"memberCount":members,"childrenLive":live});
            if let Some(children) = children {
                node["hubs"] = json!(children);
            }
            nodes.push(node);
        }
        nodes
    })
}
fn flatten(nodes: &[Value], path: &mut Vec<String>, out: &mut BTreeMap<Vec<String>, Value>) {
    for node in nodes {
        let Some(id) = node["id"].as_str() else {
            continue;
        };
        path.push(id.to_owned());
        let mut value = node.clone();
        if let Some(o) = value.as_object_mut() {
            o.remove("hubs");
        }
        out.insert(path.clone(), value);
        if let Some(children) = node["hubs"].as_array() {
            flatten(children, path, out);
        }
        path.pop();
    }
}
