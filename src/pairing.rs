//! Discovery of hosted hubs and their peers.
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

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
use tokio::sync::oneshot;

/// What a waiting peer is told when another peer pairs with it.
pub(crate) struct Matched {
    path: Vec<String>,
    hub_id: String,
    room: String,
}

struct Waiting {
    token: u64,
    kind: Value,
    id: Value,
    requirements: Value,
    wake: oneshot::Sender<Matched>,
}

/// A waiting peer is known by its id and its scopes, so the same id waiting in two hubs of one
/// service (users with different scopes) stays two peers.
type SeatKey = (String, String);

fn seat_key(id: &str, scopes: &Value) -> SeatKey {
    (id.to_owned(), scopes.to_string())
}

/// Peers whose `pair` request is waiting for a partner. Guarded by one mutex that is
/// never held across an await, so a claim removes the waiter and the claimer together.
#[derive(Default)]
pub(crate) struct PendingPairs {
    state: Mutex<(u64, HashMap<SeatKey, Waiting>)>,
}

pub(crate) enum Claim {
    /// The caller was itself claimed meanwhile and will be woken.
    Lost,
    /// The candidate waits but does not accept the caller.
    Skip,
    /// The candidate is not waiting in a request.
    Direct,
    /// The candidate was removed from the queue; wake it once the pairing holds.
    Taken(oneshot::Sender<Matched>),
}

pub(crate) enum Enter {
    Duplicate,
    Full,
}

/// A queue entry that leaves the queue when dropped, so a disconnected request frees its seat.
pub(crate) struct Seat {
    state: Arc<Inner>,
    key: SeatKey,
    token: u64,
}

impl Seat {
    fn holds(&self) -> bool {
        self.state
            .pending
            .lock()
            .1
            .get(&self.key)
            .is_some_and(|w| w.token == self.token)
    }
}

impl Drop for Seat {
    fn drop(&mut self) {
        self.state.pending.remove(&self.key, self.token);
    }
}

impl PendingPairs {
    fn lock(&self) -> std::sync::MutexGuard<'_, (u64, HashMap<SeatKey, Waiting>)> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remove(&self, key: &SeatKey, token: u64) -> bool {
        let mut guard = self.lock();
        if guard.1.get(key).is_some_and(|w| w.token == token) {
            guard.1.remove(key);
            return true;
        }
        false
    }

    fn enter(
        &self,
        state: &Arc<Inner>,
        id: &str,
        scopes: &Value,
        target: &serde_json::Map<String, Value>,
        requirements: Value,
    ) -> Result<(Seat, oneshot::Receiver<Matched>), Enter> {
        let key = seat_key(id, scopes);
        let mut guard = self.lock();
        if guard.1.contains_key(&key) {
            return Err(Enter::Duplicate);
        }
        if guard.1.len() >= state.config.max_pending_pairs {
            return Err(Enter::Full);
        }
        guard.0 += 1;
        let token = guard.0;
        let (wake, receiver) = oneshot::channel();
        guard.1.insert(
            key.clone(),
            Waiting {
                token,
                kind: target.get("kind").cloned().unwrap_or(Value::Null),
                id: target.get("id").cloned().unwrap_or(Value::Null),
                requirements,
                wake,
            },
        );
        Ok((
            Seat {
                state: state.clone(),
                key,
                token,
            },
            receiver,
        ))
    }

    /// Take `candidate` out of the queue for a caller with these `scopes`. When the caller
    /// waits too (`seat`), both leave the queue in this one step, or neither does.
    fn claim(
        &self,
        seat: Option<&Seat>,
        subject_id: &str,
        scopes: &Value,
        candidate: &SeatKey,
    ) -> Claim {
        let mut guard = self.lock();
        if let Some(seat) = seat {
            if !guard
                .1
                .get(&seat.key)
                .is_some_and(|w| w.token == seat.token)
            {
                return Claim::Lost;
            }
        }
        let Some(waiting) = guard.1.get(candidate) else {
            return Claim::Direct;
        };
        let accepts = matches!(waiting.kind.as_str(), Some("peer" | "any"))
            && waiting
                .id
                .as_str()
                .is_none_or(|wanted| wanted == subject_id)
            && rules::evaluate(&waiting.requirements, scopes);
        if !accepts {
            return Claim::Skip;
        }
        let taken = guard.1.remove(candidate).unwrap();
        if let Some(seat) = seat {
            guard.1.remove(&seat.key);
        }
        Claim::Taken(taken.wake)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().1.len()
    }
}

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
            inherited_channels: None,
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
                    state
                        .run_on_hub(&handler, |h| h.call("listUsers", vec![]).ok())
                        .await
                        .ok()
                        .flatten()
                } else if record.info.mode == HubMode::External && base < state.config.max_depth {
                    if let Some(address) = record.info.address.as_deref() {
                        let request = RouteRequest {
                            path: vec![],
                            op: "call".into(),
                            body: json!({"id":record.info.remote_hub_id,"method":"listUsers","args":[]}),
                            inherited_schemas: None,
                            inherited_channels: None,
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
                        inherited_channels: None,
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
            inherited_channels: None,
        })),
    ))
    .await
}

/// How often a waiting peer that lost a race for a partner looks again before it waits.
const RACE_RETRIES: u32 = 8;

async fn routed(
    state: Arc<Inner>,
    headers: HeaderMap,
    path: Vec<String>,
    op: &str,
    body: Value,
    inherited: [Option<Value>; 3],
) -> Result<Value, Box<Response>> {
    routed_with_channels(state, headers, path, op, body, inherited, None).await
}

async fn routed_with_channels(
    state: Arc<Inner>,
    headers: HeaderMap,
    path: Vec<String>,
    op: &str,
    body: Value,
    inherited: [Option<Value>; 3],
    inherited_channels: Option<crate::live::ChannelPolicy>,
) -> Result<Value, Box<Response>> {
    let request = RouteRequest {
        path,
        op: op.into(),
        body,
        inherited_schemas: Some(inherited),
        inherited_channels,
    };
    let response = Box::pin(route_hub(State(state.clone()), headers, Ok(Json(request)))).await;
    if !response.status().is_success() {
        return Err(Box::new(response));
    }
    let bytes = axum::body::to_bytes(response.into_body(), state.config.max_response_bytes)
        .await
        .map_err(|_| Box::new(error(StatusCode::BAD_GATEWAY, "invalid pairing response")))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| Box::new(error(StatusCode::BAD_GATEWAY, "invalid pairing response")))
}

/// Pair, and with `waitMs` stay queued until a partner arrives. Waiting never polls: the peer
/// that pairs with a queued one wakes it. The wait ends at `waitMs` (at most the configured
/// maximum) with `408`, and a request dropped by its caller leaves the queue at once.
pub(crate) async fn pair(
    state: Arc<Inner>,
    headers: HeaderMap,
    body: Value,
    inherited: [Option<Value>; 3],
    inherited_channels: Option<crate::live::ChannelPolicy>,
) -> Response {
    let _capacity = match body.pointer("/target/capacity") {
        None => 2,
        Some(value) => match value
            .as_u64()
            .filter(|n| *n >= 2 && *n <= state.config.max_room_capacity as u64)
        {
            Some(n) => n,
            None => return error(StatusCode::BAD_REQUEST, "invalid target capacity"),
        },
    };
    let wait = match body.get("waitMs") {
        None | Some(Value::Null) => Duration::ZERO,
        Some(value) => match value.as_u64() {
            Some(_) => crate::hub::pair_wait(&state.config, &body),
            None => return error(StatusCode::BAD_REQUEST, "invalid waitMs"),
        },
    };
    if wait.is_zero() {
        for attempt in 0..=RACE_RETRIES {
            let response = pair_once(
                state.clone(),
                headers.clone(),
                &body,
                inherited.clone(),
                inherited_channels.clone(),
                None,
            )
            .await;
            if response.status() != StatusCode::CONFLICT || attempt == RACE_RETRIES {
                return response;
            }
        }
        unreachable!();
    }
    let (Some(id), Some(target)) = (
        body["subject"]
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && body["subject"]["kind"] == "peer"),
        body["target"]
            .as_object()
            .filter(|t| matches!(t.get("kind").and_then(Value::as_str), Some("peer" | "any"))),
    ) else {
        return error(
            StatusCode::BAD_REQUEST,
            "waitMs needs a peer subject and a peer or any target",
        );
    };
    let id = id.to_owned();
    let Ok(requirements) = rules::parse_requirements(target.get("requirements")) else {
        return error(StatusCode::BAD_REQUEST, "invalid requirements");
    };
    let credential = body["subject"]["credential"].clone();
    let deadline = Instant::now() + wait;
    loop {
        let (seat, mut wake) = match state.pending.enter(
            &state,
            &id,
            &body["subject"]["scopes"],
            target,
            requirements.clone(),
        ) {
            Ok(entered) => entered,
            Err(Enter::Duplicate) => {
                return error(StatusCode::CONFLICT, "peer is already waiting to pair")
            }
            Err(Enter::Full) => {
                return error(StatusCode::SERVICE_UNAVAILABLE, "pairing queue is full")
            }
        };
        let mut races = 0;
        let response = loop {
            let response = pair_once(
                state.clone(),
                headers.clone(),
                &body,
                inherited.clone(),
                inherited_channels.clone(),
                Some(&seat),
            )
            .await;
            // Losing a race for a partner is not "no partner": look again at once.
            if response.status() == StatusCode::CONFLICT && seat.holds() && races < RACE_RETRIES {
                races += 1;
                continue;
            }
            break response;
        };
        let retry = matches!(
            response.status(),
            StatusCode::NOT_FOUND | StatusCode::CONFLICT
        );
        if !seat.holds() || retry {
            // Either no partner exists yet, or another peer took this seat and will wake it.
            // A peer taken after a failed attempt of its own is still matched.
            let woken = match wake.try_recv() {
                Ok(matched) => Woken::Matched(matched),
                Err(oneshot::error::TryRecvError::Closed) => Woken::Dropped,
                Err(oneshot::error::TryRecvError::Empty) => {
                    wait_for(&mut wake, deadline, &seat, state.config.request_timeout).await
                }
            };
            match woken {
                Woken::Matched(matched) => {
                    return deliver(
                        state,
                        headers,
                        matched,
                        &id,
                        &credential,
                        inherited,
                        inherited_channels,
                    )
                    .await
                }
                // The claimer failed before the pairing held: queue again while time remains.
                Woken::Dropped if Instant::now() < deadline => continue,
                Woken::Dropped | Woken::TimedOut => {
                    return error(
                        StatusCode::REQUEST_TIMEOUT,
                        "no matching peer arrived in time",
                    )
                }
            }
        }
        return response;
    }
}

enum Woken {
    Matched(Matched),
    /// The peer that claimed this one failed before the pairing held.
    Dropped,
    TimedOut,
}

/// Sleep until woken or the deadline. A seat claimed as the deadline passes is still woken by
/// its claimer, which finishes within the request timeout (`grace`).
async fn wait_for(
    wake: &mut oneshot::Receiver<Matched>,
    deadline: Instant,
    seat: &Seat,
    grace: Duration,
) -> Woken {
    let settle = |result: Result<Matched, oneshot::error::RecvError>| match result {
        Ok(matched) => Woken::Matched(matched),
        Err(_) => Woken::Dropped,
    };
    match tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        &mut *wake,
    )
    .await
    {
        Ok(result) => settle(result),
        Err(_) if seat.holds() => Woken::TimedOut,
        Err(_) => match tokio::time::timeout(grace, wake).await {
            Ok(result) => settle(result),
            Err(_) => Woken::TimedOut,
        },
    }
}

/// A peer that is already paired in an open system room of its hub.
async fn seated_room(
    state: &Arc<Inner>,
    headers: &HeaderMap,
    candidate: &Value,
    inherited: &[Option<Value>; 3],
) -> Option<Matched> {
    let mut path: Vec<String> = candidate["path"]
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let hub_id = path.pop()?;
    let user = routed(
        state.clone(),
        headers.clone(),
        path.clone(),
        "call",
        call_for(&hub_id, "getUser", vec![candidate["id"].clone()]),
        inherited.clone(),
    )
    .await
    .ok()?;
    if user["result"]["status"] != "paired" {
        return None;
    }
    let room = user["result"]["serverId"].as_str()?.to_owned();
    let server = routed(
        state.clone(),
        headers.clone(),
        path.clone(),
        "call",
        call_for(&hub_id, "getServer", vec![json!(room)]),
        inherited.clone(),
    )
    .await
    .ok()?;
    (server["result"]["system"] == true
        && matches!(server["result"]["status"].as_str(), Some("open" | "full")))
    .then_some(Matched { path, hub_id, room })
}

/// The waiting peer's own half of a pairing another peer made: authorize and issue its ticket.
/// If that fails the peer leaves the room it was seated in.
async fn deliver(
    state: Arc<Inner>,
    headers: HeaderMap,
    matched: Matched,
    id: &str,
    credential: &Value,
    inherited: [Option<Value>; 3],
    inherited_channels: Option<crate::live::ChannelPolicy>,
) -> Response {
    let Matched { path, hub_id, room } = matched;
    let issued = async {
        let proof = authorize_proof(
            &state, &headers, &path, &hub_id, id, &room, "peer", credential, &inherited,
        )
        .await?;
        let response = ticket_descriptor(
            state.clone(),
            headers.clone(),
            path.clone(),
            &hub_id,
            id,
            &room,
            "peer",
            &proof,
            "peer-peer",
            inherited.clone(),
            inherited_channels,
        )
        .await;
        if response.status().is_success() {
            Ok(response)
        } else {
            Err(Box::new(response))
        }
    }
    .await;
    match issued {
        Ok(response) => response,
        Err(refused) => {
            let _ = routed(
                state,
                headers,
                path,
                "call",
                call_for(&hub_id, "leave", vec![json!(id)]),
                inherited,
            )
            .await;
            *refused
        }
    }
}

/// One attempt to pair. With a `seat` the caller also waits in the queue, so a peer already
/// waiting there is claimed atomically and woken once the pairing holds.
async fn pair_once(
    state: Arc<Inner>,
    headers: HeaderMap,
    body: &Value,
    inherited: [Option<Value>; 3],
    inherited_channels: Option<crate::live::ChannelPolicy>,
    seat: Option<&Seat>,
) -> Response {
    let capacity = body
        .pointer("/target/capacity")
        .and_then(Value::as_u64)
        .unwrap_or(2);
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
    // `target.id` narrows the match to one named hub or peer, so hubs that share scopes stay distinct.
    let wanted = match target.get("id") {
        None | Some(Value::Null) => None,
        Some(Value::String(wanted)) if !wanted.is_empty() => Some(wanted.as_str()),
        Some(_) => return error(StatusCode::BAD_REQUEST, "invalid target"),
    };
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
    // A waiting peer another peer paired before this request began is already seated.
    if seat.is_some() {
        let mine = found["candidates"]
            .as_array()
            .and_then(|all| all.iter().find(|c| c["kind"] == "peer" && c["id"] == id));
        if let Some(mine) = mine {
            if let Some(seated) = seated_room(&state, &headers, mine, &inherited).await {
                let credential = subject.get("credential").cloned().unwrap_or(Value::Null);
                return deliver(
                    state,
                    headers,
                    seated,
                    id,
                    &credential,
                    inherited,
                    inherited_channels,
                )
                .await;
            }
        }
    }
    let mut selected = None;
    let mut taken = None;
    let mut candidate_scopes = Value::Null;
    if let Some(candidates) = found["candidates"].as_array() {
        for candidate in candidates {
            if candidate["kind"] == kind && candidate["id"] == id {
                continue;
            }
            if wanted.is_some_and(|wanted| candidate["id"] != wanted) {
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
                candidate_scopes = user["result"]["scopes"].clone();
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
                    if server["result"]["system"] != true
                        || server["result"]["status"] != "open"
                        || server["result"]["free"].as_u64().unwrap_or(0) == 0
                    {
                        continue;
                    }
                }
            }
            if candidate["kind"] == "peer" {
                match state.pending.claim(
                    seat,
                    id,
                    &subject["scopes"],
                    &seat_key(candidate["id"].as_str().unwrap_or(""), &candidate_scopes),
                ) {
                    Claim::Lost => break,
                    Claim::Skip => continue,
                    Claim::Direct => {}
                    Claim::Taken(wake) => taken = Some(wake),
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
            Err(e) => return *e,
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
            Err(e) => return *e,
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
            Err(e) => return *e,
        }
    } else {
        None
    };
    let room = existing.clone().unwrap_or(room);
    // A hub that sets `auth` must accept the subject's credential before membership changes.
    let role = if kind == "hub" { "service" } else { "peer" };
    let credential = subject.get("credential").cloned().unwrap_or(Value::Null);
    let proof = match authorize_proof(
        &state,
        &headers,
        &path,
        &hub_id,
        id,
        &room,
        role,
        &credential,
        &inherited,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return *e,
    };
    if peer {
        let result = routed(
            state.clone(),
            headers.clone(),
            path.clone(),
            "call",
            call(
                "pairPeers",
                vec![json!({"room":room,"capacity":capacity,"users":[
                    {"id":target_id,"scopes":target_scopes},
                    {"id":id,"scopes":subject["scopes"]}
                ]})],
            ),
            inherited.clone(),
        )
        .await;
        if let Err(e) = result {
            // A waiting caller that lost a race for a partner who was paired meanwhile asks again.
            if e.status() == StatusCode::BAD_REQUEST {
                let text = axum::body::to_bytes(e.into_body(), 4096)
                    .await
                    .unwrap_or_default();
                if String::from_utf8_lossy(&text).contains("already paired")
                    || String::from_utf8_lossy(&text).contains("room at capacity")
                {
                    return error(StatusCode::CONFLICT, "peer already paired");
                }
                return error(StatusCode::BAD_REQUEST, "pairing refused");
            }
            return *e;
        }
        // The waiter is told as soon as the engine holds the pairing; if it already left, the
        // pairing is undone for it.
        if let Some(wake) = taken.take() {
            let matched = Matched {
                path: path.clone(),
                hub_id: hub_id.clone(),
                room: room.clone(),
            };
            if wake.send(matched).is_err() {
                let _ = routed(
                    state.clone(),
                    headers.clone(),
                    path.clone(),
                    "call",
                    call("leave", vec![json!(target_id)]),
                    inherited.clone(),
                )
                .await;
            }
        }
    } else {
        if existing.is_none() {
            let server = json!({"id":room,"scopes":target_scopes,"capacity":capacity});
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
                return *e;
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
            return *e;
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
            Err(e) => return *e,
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
                Err(e) => return *e,
            };
            if joined["result"]["ok"] != true {
                return error(StatusCode::CONFLICT, "subject could not join");
            }
        }
    }
    let link = match (kind, peer) {
        ("peer", true) => "peer-peer",
        ("hub", false) => "hub-hub",
        _ => "peer-hub",
    };
    ticket_descriptor(
        state,
        headers,
        path,
        &hub_id,
        id,
        &room,
        role,
        &proof,
        link,
        inherited,
        inherited_channels,
    )
    .await
}

/// The credential is checked once. A hub that verified it returns a receipt, which the
/// ticket step uses in place of the credential, so a single-use credential works.
#[allow(clippy::too_many_arguments)]
async fn authorize_proof(
    state: &Arc<Inner>,
    headers: &HeaderMap,
    path: &[String],
    hub_id: &str,
    id: &str,
    room: &str,
    role: &str,
    credential: &Value,
    inherited: &[Option<Value>; 3],
) -> Result<Value, Box<Response>> {
    let checked = routed(
        state.clone(),
        headers.clone(),
        path.to_vec(),
        "authorize",
        json!({"id":hub_id,"userId":id,"serverId":room,"role":role,"credential":credential}),
        inherited.clone(),
    )
    .await?;
    Ok(match checked.get("receipt").and_then(Value::as_str) {
        Some(receipt) => json!({"verified":receipt}),
        None => json!({"credential":credential}),
    })
}

#[allow(clippy::too_many_arguments)]
async fn ticket_descriptor(
    state: Arc<Inner>,
    headers: HeaderMap,
    path: Vec<String>,
    hub_id: &str,
    id: &str,
    room: &str,
    role: &str,
    proof: &Value,
    link: &str,
    inherited: [Option<Value>; 3],
    inherited_channels: Option<crate::live::ChannelPolicy>,
) -> Response {
    let mut body = json!({"id":hub_id,"userId":id,"serverId":room,"role":role});
    body.as_object_mut()
        .unwrap()
        .extend(proof.as_object().cloned().unwrap_or_default());
    let ticket = match routed_with_channels(
        state,
        headers,
        path,
        "ticket",
        body,
        inherited,
        inherited_channels,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return *e,
    };
    Json(json!({"link":link,"room":room,"userId":id,"ticket":ticket["ticket"],"url":ticket["url"],"transports":["webrtc","websocket"]})).into_response()
}

fn call_for(id: &str, method: &str, args: Vec<Value>) -> Value {
    json!({"id":id,"method":method,"args":args})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{HubConfig, HubService};

    fn state(max_pending_pairs: usize) -> Arc<Inner> {
        HubService::new(HubConfig {
            token: "pending-queue-unit-test-token-123".into(),
            max_pending_pairs,
            ..HubConfig::default()
        })
        .unwrap()
        .inner
    }

    fn target(value: Value) -> serde_json::Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn a_peer_waits_once_and_the_queue_is_bounded() {
        let state = state(2);
        let any = target(json!({"kind":"peer"}));
        let (a, _a) = state
            .pending
            .enter(&state, "a", &json!({}), &any, Value::Null)
            .ok()
            .unwrap();
        assert!(matches!(
            state
                .pending
                .enter(&state, "a", &json!({}), &any, Value::Null),
            Err(Enter::Duplicate)
        ));
        let (_b, _b_wake) = state
            .pending
            .enter(&state, "b", &json!({}), &any, Value::Null)
            .ok()
            .unwrap();
        assert!(matches!(
            state
                .pending
                .enter(&state, "c", &json!({}), &any, Value::Null),
            Err(Enter::Full)
        ));
        drop(a);
        assert_eq!(state.pending.len(), 1);
        assert!(state
            .pending
            .enter(&state, "c", &json!({}), &any, Value::Null)
            .is_ok());
    }

    #[test]
    fn a_dropped_seat_leaves_and_a_stale_seat_removes_nothing() {
        let state = state(4);
        let any = target(json!({"kind":"peer"}));
        let (seat, _wake) = state
            .pending
            .enter(&state, "a", &json!({}), &any, Value::Null)
            .ok()
            .unwrap();
        let stale = Seat {
            state: state.clone(),
            key: seat_key("a", &json!({})),
            token: seat.token + 100,
        };
        drop(stale);
        assert!(seat.holds());
        drop(seat);
        assert_eq!(state.pending.len(), 0);
    }

    #[test]
    fn a_claim_removes_both_peers_once() {
        let state = state(4);
        let any = target(json!({"kind":"peer"}));
        let (a, mut a_wake) = state
            .pending
            .enter(&state, "a", &json!({}), &any, Value::Null)
            .ok()
            .unwrap();
        let (b, _b_wake) = state
            .pending
            .enter(&state, "b", &json!({}), &any, Value::Null)
            .ok()
            .unwrap();
        let scopes = json!({});
        let first = state
            .pending
            .claim(Some(&a), "a", &scopes, &seat_key("b", &json!({})));
        assert!(matches!(first, Claim::Taken(_)));
        assert_eq!(state.pending.len(), 0);
        // `b` was claimed, so its own claim of `a` finds nobody and does not pair twice.
        assert!(matches!(
            state
                .pending
                .claim(Some(&b), "b", &scopes, &seat_key("a", &json!({}))),
            Claim::Lost
        ));
        assert!(a_wake.try_recv().is_err());
    }

    #[test]
    fn the_same_peer_id_waits_once_per_scope() {
        let state = state(4);
        let any = target(json!({"kind":"peer"}));
        let north = json!({"arena":"north"});
        let south = json!({"arena":"south"});
        let (_a, _a_wake) = state
            .pending
            .enter(&state, "twin", &north, &any, Value::Null)
            .ok()
            .unwrap();
        let (_b, _b_wake) = state
            .pending
            .enter(&state, "twin", &south, &any, Value::Null)
            .ok()
            .unwrap();
        assert!(matches!(
            state
                .pending
                .enter(&state, "twin", &north, &any, Value::Null),
            Err(Enter::Duplicate)
        ));
        // A claim reaches only the twin with the scopes it asked for.
        let taken = state
            .pending
            .claim(None, "x", &json!({}), &seat_key("twin", &south));
        assert!(matches!(taken, Claim::Taken(_)));
        assert_eq!(state.pending.len(), 1);
        assert!(matches!(
            state
                .pending
                .claim(None, "x", &json!({}), &seat_key("twin", &south)),
            Claim::Direct
        ));
    }

    #[test]
    fn a_claim_respects_what_the_waiter_asked_for() {
        let state = state(4);
        let picky = target(json!({"kind":"peer","id":"z"}));
        let rank = rules::parse_requirements(Some(&json!({"rank":3}))).unwrap();
        let (_seat, _wake) = state
            .pending
            .enter(&state, "w", &json!({}), &picky, rank)
            .ok()
            .unwrap();
        assert!(matches!(
            state
                .pending
                .claim(None, "y", &json!({"rank":3}), &seat_key("w", &json!({}))),
            Claim::Skip
        ));
        assert!(matches!(
            state
                .pending
                .claim(None, "z", &json!({"rank":1}), &seat_key("w", &json!({}))),
            Claim::Skip
        ));
        assert!(matches!(
            state.pending.claim(
                None,
                "z",
                &json!({"rank":3}),
                &seat_key("nobody", &json!({}))
            ),
            Claim::Direct
        ));
        assert!(matches!(
            state
                .pending
                .claim(None, "z", &json!({"rank":3}), &seat_key("w", &json!({}))),
            Claim::Taken(_)
        ));
        assert_eq!(state.pending.len(), 0);
    }
}
