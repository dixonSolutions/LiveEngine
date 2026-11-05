//! Bounded, read-only subscriptions to the local hub tree.
use crate::hub::{error, expire_hubs, HubMode, Inner};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

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
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
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
    static SUBSCRIBERS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let slots = SUBSCRIBERS
        .get_or_init(|| Arc::new(Semaphore::new(256)))
        .clone();
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
            serve(state, socket).await;
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
async fn serve(root: Arc<Inner>, mut socket: WebSocket) {
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
    let admin = token.as_bytes().ct_eq(root.config.token.as_bytes()).into();
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
    let mut previous = snapshot(state.clone(), effective).await;
    if !send(&mut socket, json!({"type":"snapshot","seq":seq,"path":path,"depth":effective,"effectiveDepth":effective,"hubs":previous})).await { return; }
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let current = snapshot(state.clone(), effective).await;
                let mut old = BTreeMap::new(); let mut new = BTreeMap::new();
                flatten(&previous, &mut Vec::new(), &mut old); flatten(&current, &mut Vec::new(), &mut new);
                for (p, node) in &old { if !new.contains_key(p) { seq += 1; if !send(&mut socket,json!({"type":"removed","seq":seq,"path":p})).await { return; } } else { let _ = node; } }
                for (p, node) in &new { let kind = match old.get(p) { None => Some("added"), Some(before) if before["status"] != node["status"] => Some("status"), Some(before) if *before != *node => Some("updated"), _ => None }; if let Some(kind) = kind { seq += 1; if !send(&mut socket,json!({"type":kind,"seq":seq,"path":p,"hub":node})).await { return; } } }
                previous = current;
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(text))) if serde_json::from_str::<Value>(&text).ok().is_some_and(|v| v["type"] == "resnapshot") => { previous = snapshot(state.clone(), effective).await; if !send(&mut socket,json!({"type":"snapshot","seq":seq,"path":path,"depth":effective,"effectiveDepth":effective,"hubs":previous})).await { return; } },
                Some(Ok(Message::Ping(data))) => { if socket.send(Message::Pong(data)).await.is_err() { return; } },
                Some(Ok(Message::Pong(_))) => {}, _ => return,
            }
        }
    }
}
async fn resolve(root: Arc<Inner>, path: &[String]) -> Option<(Arc<Inner>, usize)> {
    let mut current = root;
    let mut limit = current.config.live_tree_depth.min(current.config.max_depth);
    for id in path {
        let record = current.hubs.read().await.get(id).cloned()?;
        if record.info.mode != HubMode::Central {
            return None;
        }
        if let Some(depth) = record.info.live_depth {
            limit = depth;
        }
        current = record.child?;
    }
    Some((current, limit))
}
fn snapshot(
    state: Arc<Inner>,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Value>> + Send>> {
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
                tokio::task::spawn_blocking(move || {
                    let Ok(mut h) = handler.lock() else {
                        return (0, 0);
                    };
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
            let live = central && child_depth > 0 && r.child.is_some();
            let mut node = json!({"id":r.info.id,"mode":r.info.mode,"status":if r.info.stale {"stale"} else if central && r.child.is_none() {"unreachable"} else {"ok"},"roomCount":rooms,"memberCount":members,"childrenLive":live});
            if live {
                node["hubs"] = json!(snapshot(r.child.unwrap(), child_depth).await);
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
