//! Discovery of hosted hubs and their peers.
use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    hub::{
        error, expire_hubs, forward_route, header_depth, route_hub, HubMode, Inner, RouteRequest,
    },
    rules,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoverRequest {
    #[serde(default)]
    path: Vec<String>,
    depth: Option<usize>,
    kind: String,
    requirements: Option<Value>,
}

pub(crate) async fn discover_endpoint(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(mut body)) = payload else {
        return error(StatusCode::BAD_REQUEST, "invalid discovery request");
    };
    let path = match body.get_mut("path") {
        Some(v) => match serde_json::from_value(std::mem::take(v)) {
            Ok(p) => p,
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid path"),
        },
        None => Vec::new(),
    };
    if let Some(obj) = body.as_object_mut() {
        obj.remove("path");
    }
    route_hub(
        State(state),
        headers,
        Ok(Json(RouteRequest {
            path,
            op: "discover".into(),
            body,
            inherited_schemas: None,
        })),
    )
    .await
}

pub(crate) async fn discover(state: Arc<Inner>, headers: HeaderMap, body: Value) -> Response {
    let Ok(request) = serde_json::from_value::<DiscoverRequest>(body) else {
        return error(StatusCode::BAD_REQUEST, "invalid discovery request");
    };
    if !request.path.is_empty() || !matches!(request.kind.as_str(), "hub" | "peer" | "any") {
        return error(StatusCode::BAD_REQUEST, "invalid discovery request");
    }
    let depth = request.depth.unwrap_or(1);
    let base = header_depth(&headers).unwrap_or(usize::MAX);
    if depth > state.config.max_depth || base.saturating_add(depth) > state.config.max_depth {
        return error(StatusCode::BAD_REQUEST, "discovery depth exceeded");
    }
    let Ok(requirements) = rules::parse_requirements(request.requirements.as_ref()) else {
        return error(StatusCode::BAD_REQUEST, "invalid requirements");
    };
    let mut budget = state.config.max_hubs.min(1024);
    let mut candidates = Vec::new();
    search(
        state.clone(),
        headers,
        depth,
        base,
        &request.kind,
        &requirements,
        &mut budget,
        &mut candidates,
        Vec::new(),
    )
    .await;
    for candidate in &mut candidates {
        if let Some(object) = candidate.as_object_mut() {
            object.remove("scopes");
        }
    }
    let result = json!({"candidates": candidates});
    if serde_json::to_vec(&result).map_or(true, |v| v.len() > state.config.max_response_bytes) {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "discovery response exceeds size limit",
        );
    }
    Json(result).into_response()
}

#[allow(clippy::too_many_arguments)]
fn search<'a>(
    state: Arc<Inner>,
    headers: HeaderMap,
    depth: usize,
    base: usize,
    kind: &'a str,
    requirements: &'a Value,
    budget: &'a mut usize,
    out: &'a mut Vec<Value>,
    prefix: Vec<String>,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        if depth == 0 || *budget == 0 {
            return;
        }
        let mut records: Vec<_> = {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            hubs.values().cloned().collect()
        };
        records.sort_by(|a, b| a.info.id.cmp(&b.info.id));
        for record in records {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            let mut path = prefix.clone();
            path.push(record.info.id.clone());
            if kind != "peer"
                && rules::evaluate(requirements, &rules::scopes(Some(&record.info.scopes)))
            {
                out.push(json!({"kind":"hub", "id":record.info.id, "path":path, "scopes":record.info.scopes}));
            }
            if kind != "hub" {
                let users = if let Some(handler) = record.handler.clone() {
                    if let Ok(permit) = state.inflight_calls.clone().try_acquire_owned() {
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            handler.try_lock().ok()?.call("listUsers", vec![]).ok()
                        })
                        .await
                        .ok()
                        .flatten()
                    } else {
                        None
                    }
                } else if record.info.mode == HubMode::External && base < state.config.max_depth {
                    if let Some(address) = record.info.address.as_deref() {
                        let request = RouteRequest {
                            path: vec![],
                            op: "call".into(),
                            body: json!({"id":record.info.remote_hub_id,"method":"listUsers","args":[]}),
                            inherited_schemas: None,
                        };
                        let response = forward_route(
                            &state,
                            address,
                            record.remote_token.as_deref().unwrap_or(""),
                            &headers,
                            base + 1,
                            &request,
                        )
                        .await;
                        if response.status().is_success() {
                            axum::body::to_bytes(
                                response.into_body(),
                                state.config.max_response_bytes,
                            )
                            .await
                            .ok()
                            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                            .and_then(|value| value.get("result").cloned())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(users) = users.as_ref().and_then(Value::as_array) {
                    for user in users.iter().take(*budget) {
                        if let (Some(id), Some(scopes)) =
                            (user.get("id").and_then(Value::as_str), user.get("scopes"))
                        {
                            if rules::evaluate(requirements, scopes) {
                                out.push(
                                    json!({"kind":"peer", "id":id, "path":path, "scopes":scopes}),
                                );
                            }
                        }
                        *budget -= 1;
                    }
                }
            }
            if record.info.mode == HubMode::Central && depth > 1 && *budget > 0 {
                if let Some(child) = record.child {
                    search(
                        child,
                        headers.clone(),
                        depth - 1,
                        base + 1,
                        kind,
                        requirements,
                        budget,
                        out,
                        path,
                    )
                    .await;
                } else if let Some(address) = record.info.address.as_deref() {
                    let req = RouteRequest {
                        path: vec![],
                        op: "discover".into(),
                        body: json!({"depth":depth-1,"kind":kind,"requirements":requirements}),
                        inherited_schemas: None,
                    };
                    let response = forward_route(
                        &state,
                        address,
                        record.remote_token.as_deref().unwrap_or(""),
                        &headers,
                        base + 1,
                        &req,
                    )
                    .await;
                    if response.status().is_success() {
                        if let Ok(bytes) = axum::body::to_bytes(
                            response.into_body(),
                            state.config.max_response_bytes,
                        )
                        .await
                        {
                            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                                if let Some(candidates) =
                                    value.get("candidates").and_then(Value::as_array)
                                {
                                    for candidate in candidates.iter().take(*budget) {
                                        let mut candidate = candidate.clone();
                                        if let Some(obj) = candidate.as_object_mut() {
                                            let child_path = obj
                                                .get("path")
                                                .and_then(Value::as_array)
                                                .cloned()
                                                .unwrap_or_default();
                                            let mut full: Vec<Value> =
                                                path.iter().cloned().map(Value::String).collect();
                                            full.extend(child_path);
                                            // Only copy the documented public candidate fields from a remote.
                                            let clean = json!({"kind":obj.get("kind"),"id":obj.get("id"),"scopes":obj.get("scopes"),"path":full});
                                            out.push(clean);
                                            *budget -= 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Pairing uses the hosted engine's user/server matching and live ticket issuance.
pub(crate) async fn pair_endpoint(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(mut body)) = payload else {
        return error(StatusCode::BAD_REQUEST, "invalid pairing request");
    };
    let path = match body.get_mut("path") {
        Some(v) => match serde_json::from_value(std::mem::take(v)) {
            Ok(p) => p,
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid path"),
        },
        None => Vec::new(),
    };
    if let Some(obj) = body.as_object_mut() {
        obj.remove("path");
    }
    Box::pin(route_hub(
        State(state),
        headers,
        Ok(Json(RouteRequest {
            path,
            op: "pair".into(),
            body,
            inherited_schemas: None,
        })),
    ))
    .await
}

async fn routed(
    state: Arc<Inner>,
    headers: HeaderMap,
    path: Vec<String>,
    op: &str,
    body: Value,
    inherited: [Option<Value>; 3],
) -> Result<Value, Response> {
    let response = Box::pin(route_hub(
        State(state.clone()),
        headers,
        Ok(Json(RouteRequest {
            path,
            op: op.into(),
            body,
            inherited_schemas: Some(inherited),
        })),
    ))
    .await;
    if !response.status().is_success() {
        return Err(response);
    }
    let bytes = axum::body::to_bytes(response.into_body(), state.config.max_response_bytes)
        .await
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "invalid pairing response"))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| error(StatusCode::BAD_GATEWAY, "invalid pairing response"))
}

pub(crate) async fn pair(
    state: Arc<Inner>,
    headers: HeaderMap,
    body: Value,
    inherited: [Option<Value>; 3],
) -> Response {
    let Some(subject) = body.get("subject").and_then(Value::as_object) else {
        return error(StatusCode::BAD_REQUEST, "invalid subject");
    };
    let Some(target) = body.get("target").and_then(Value::as_object) else {
        return error(StatusCode::BAD_REQUEST, "invalid target");
    };
    let (Some(kind), Some(id), Some(target_kind)) = (
        subject.get("kind").and_then(Value::as_str),
        subject.get("id").and_then(Value::as_str),
        target.get("kind").and_then(Value::as_str),
    ) else {
        return error(StatusCode::BAD_REQUEST, "invalid pairing request");
    };
    if !matches!(kind, "peer" | "hub")
        || !matches!(target_kind, "peer" | "hub" | "any")
        || id.is_empty()
        || !subject.get("scopes").is_some_and(Value::is_object)
    {
        return error(StatusCode::BAD_REQUEST, "invalid pairing request");
    }
    let depth = state
        .config
        .max_depth
        .saturating_sub(header_depth(&headers).unwrap_or(usize::MAX));
    let found = discover(
        state.clone(),
        headers.clone(),
        json!({"kind":target_kind,"depth":depth,"requirements":target.get("requirements")}),
    )
    .await;
    if !found.status().is_success() {
        return found;
    }
    let Ok(bytes) = axum::body::to_bytes(found.into_body(), state.config.max_response_bytes).await
    else {
        return error(StatusCode::BAD_GATEWAY, "invalid discovery response");
    };
    let Ok(found) = serde_json::from_slice::<Value>(&bytes) else {
        return error(StatusCode::BAD_GATEWAY, "invalid discovery response");
    };
    let mut selected = None;
    if let Some(candidates) = found["candidates"].as_array() {
        for candidate in candidates {
            if candidate["kind"] == kind && candidate["id"] == id {
                continue;
            }
            if candidate["kind"] == "hub" {
                let Some(mut candidate_path) = candidate["path"].as_array().map(|p| {
                    p.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                }) else {
                    continue;
                };
                let Some(candidate_id) = candidate_path.pop() else {
                    continue;
                };
                let Ok(info) = routed(
                    state.clone(),
                    headers.clone(),
                    candidate_path,
                    "get",
                    json!({"id":candidate_id}),
                    inherited.clone(),
                )
                .await
                else {
                    continue;
                };
                if info["mode"] == "central" {
                    continue;
                }
            }
            if candidate["kind"] == "peer" {
                let Some(mut candidate_path) = candidate["path"].as_array().map(|p| {
                    p.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                }) else {
                    continue;
                };
                let Some(candidate_hub) = candidate_path.pop() else {
                    continue;
                };
                let Some(candidate_id) = candidate["id"].as_str() else {
                    continue;
                };
                let Ok(user) = routed(
                    state.clone(),
                    headers.clone(),
                    candidate_path.clone(),
                    "call",
                    call_for(&candidate_hub, "getUser", vec![json!(candidate_id)]),
                    inherited.clone(),
                )
                .await
                else {
                    continue;
                };
                if user["result"]["status"] == "idle"
                    || !rules::evaluate(&user["result"]["requirements"], &subject["scopes"])
                {
                    continue;
                }
                if let Some(room) = user["result"]["serverId"].as_str() {
                    let Ok(server) = routed(
                        state.clone(),
                        headers.clone(),
                        candidate_path,
                        "call",
                        call_for(&candidate_hub, "getServer", vec![json!(room)]),
                        inherited.clone(),
                    )
                    .await
                    else {
                        continue;
                    };
                    if server["result"]["status"] != "open" {
                        continue;
                    }
                }
            }
            selected = Some(candidate);
            break;
        }
    }
    let Some(candidate) = selected.cloned() else {
        return error(StatusCode::NOT_FOUND, "no matching candidate");
    };
    let Some(mut path) = candidate["path"].as_array().map(|p| {
        p.iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>()
    }) else {
        return error(StatusCode::BAD_GATEWAY, "invalid candidate");
    };
    let Some(hub_id) = path.pop() else {
        return error(StatusCode::BAD_GATEWAY, "invalid candidate path");
    };
    let peer = candidate["kind"] == "peer";
    let target_id = candidate["id"].as_str().unwrap_or("");
    let target_scopes = if peer {
        match routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call_for(&hub_id, "getUser", vec![json!(target_id)]),
            inherited.clone(),
        )
        .await
        {
            Ok(v) => v["result"]["scopes"].clone(),
            Err(e) => return e,
        }
    } else {
        match routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "get",
            json!({"id":hub_id}),
            inherited.clone(),
        )
        .await
        {
            Ok(v) => v["scopes"].clone(),
            Err(e) => return e,
        }
    };
    let room = if peer {
        format!("pair:{target_id}")
    } else {
        format!("pair:hub:{hub_id}")
    };
    let call = |method: &str, args: Vec<Value>| json!({"id":hub_id,"method":method,"args":args});
    let existing = if peer {
        match routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call("getUser", vec![json!(target_id)]),
            inherited.clone(),
        )
        .await
        {
            Ok(v) => {
                if !rules::evaluate(&v["result"]["requirements"], &subject["scopes"]) {
                    return error(StatusCode::CONFLICT, "candidate requirements do not match");
                }
                v["result"]["serverId"].as_str().map(str::to_owned)
            }
            Err(e) => return e,
        }
    } else {
        None
    };
    let room = existing.clone().unwrap_or(room);
    if peer {
        let result = routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call(
                "pairPeers",
                vec![json!({"room":room,"capacity":64,"users":[
                    {"id":target_id,"scopes":target_scopes},
                    {"id":id,"scopes":subject["scopes"]}
                ]})],
            ),
            inherited.clone(),
        )
        .await;
        if let Err(e) = result {
            return e;
        }
    } else {
        if existing.is_none() {
            let server = json!({"id":room,"scopes":target_scopes,"capacity":64});
            if let Err(e) = routed(
                state.clone(),
                headers.clone(),
                path.clone(),
                "call",
                call("upsertServer", vec![server]),
                inherited.clone(),
            )
            .await
            {
                return e;
            }
        }
        let user =
            json!({"id":id,"scopes":subject["scopes"],"requirements":target.get("requirements")});
        if let Err(e) = routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call("upsertUser", vec![user]),
            inherited.clone(),
        )
        .await
        {
            return e;
        }
        let current = match routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call("getUser", vec![json!(id)]),
            inherited.clone(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => return e,
        };
        if current["result"]["serverId"] != room {
            let joined = match routed(
                state.clone(),
                headers.clone(),
                path.clone(),
                "call",
                call("join", vec![json!(id), json!(room)]),
                inherited.clone(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => return e,
            };
            if joined["result"]["ok"] != true {
                return error(StatusCode::CONFLICT, "subject could not join");
            }
        }
    }
    let ticket = match routed(state, headers, path, "ticket", json!({"id":hub_id,"userId":id,"serverId":room,"role":if kind == "hub" {"service"} else {"peer"}}), inherited).await { Ok(v) => v, Err(e) => return e };
    let link = match (kind, peer) {
        ("peer", true) => "peer-peer",
        ("hub", false) => "hub-hub",
        _ => "peer-hub",
    };
    Json(json!({"link":link,"room":room,"ticket":ticket["ticket"],"url":ticket["url"],"transports":["webrtc","websocket"]})).into_response()
}

fn call_for(id: &str, method: &str, args: Vec<Value>) -> Value {
    json!({"id":id,"method":method,"args":args})
}
