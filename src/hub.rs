//! HTTP hosting and registry support for the `liveengine.hub/v1` protocol.
//!
//! A [`HubService`] can be mounted in an existing Axum application with
//! [`HubService::router`], or served directly by the `liveengine-hub` binary.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use reqwest::{redirect::Policy, Client};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use subtle::ConstantTimeEq;
use tokio::sync::{RwLock, Semaphore};
use url::Url;

use crate::engine::Engine;

pub const HUB_PROTOCOL: &str = "liveengine.hub/v1";

/// Application-defined handler for an in-process hub.
pub trait HubHandler: Send {
    fn call(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String>;
    fn preflight(
        &mut self,
        _method: &str,
        _args: &[Value],
        _config: &HubConfig,
    ) -> Result<(), String> {
        Ok(())
    }
}

impl HubHandler for Engine {
    fn call(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String> {
        self.dispatch(method, args)
    }
    fn preflight(
        &mut self,
        method: &str,
        args: &[Value],
        config: &HubConfig,
    ) -> Result<(), String> {
        preflight_internal_call(
            self,
            &CallRequest {
                method: method.to_string(),
                args: args.to_vec(),
            },
            config,
        )
    }
}

/// Runtime limits and security settings for a hub server.
#[derive(Clone)]
pub struct HubConfig {
    /// Administrative bearer token. Must contain at least 24 bytes.
    pub token: String,
    /// Exact remote host names or IP addresses permitted for registration.
    pub allowed_remote_hosts: Vec<String>,
    pub max_hubs: usize,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_args: usize,
    pub max_entities: usize,
    pub max_events: usize,
    pub max_generates_per_call: usize,
    pub max_inflight_calls: usize,
    pub request_timeout: Duration,
    pub rate_limit_requests: usize,
    pub rate_limit_window: Duration,
    pub rate_limit_peers: usize,
    pub max_ttl: Duration,
    /// Seed for engines created by internal hub registrations.
    pub seed: u64,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            allowed_remote_hosts: Vec::new(),
            max_hubs: 256,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_args: 256,
            max_entities: 100_000,
            max_events: 100_000,
            max_generates_per_call: 10_000,
            max_inflight_calls: 16,
            request_timeout: Duration::from_secs(5),
            rate_limit_requests: 120,
            rate_limit_window: Duration::from_secs(60),
            rate_limit_peers: 4096,
            max_ttl: Duration::from_secs(24 * 60 * 60),
            seed: 0,
        }
    }
}

impl HubConfig {
    /// Read the required token and optional comma-separated remote host list
    /// from the standard standalone environment variables.
    pub fn from_env() -> Result<Self, String> {
        let token = std::env::var("LIVEENGINE_TOKEN")
            .map_err(|_| "LIVEENGINE_TOKEN must be set".to_string())?;
        let allowed_remote_hosts = std::env::var("LIVEENGINE_ALLOWED_REMOTE_HOSTS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|host| !host.is_empty())
            .map(str::to_ascii_lowercase)
            .collect();
        let mut config = Self {
            token,
            allowed_remote_hosts,
            ..Self::default()
        };
        config.max_hubs = env_limit("LIVEENGINE_MAX_HUBS", config.max_hubs)?;
        config.max_request_bytes =
            env_limit("LIVEENGINE_MAX_BODY_BYTES", config.max_request_bytes)?;
        config.max_response_bytes =
            env_limit("LIVEENGINE_MAX_RESPONSE_BYTES", config.max_response_bytes)?;
        config.max_args = env_limit("LIVEENGINE_MAX_ARGS", config.max_args)?;
        config.max_entities = env_limit("LIVEENGINE_MAX_ENTITIES", config.max_entities)?;
        config.max_events = env_limit("LIVEENGINE_MAX_EVENTS", config.max_events)?;
        config.max_generates_per_call =
            env_limit("LIVEENGINE_MAX_GENERATE", config.max_generates_per_call)?;
        config.max_inflight_calls =
            env_limit("LIVEENGINE_MAX_INFLIGHT_CALLS", config.max_inflight_calls)?;
        config.rate_limit_requests =
            env_limit("LIVEENGINE_RATE_LIMIT_REQUESTS", config.rate_limit_requests)?;
        config.rate_limit_peers =
            env_limit("LIVEENGINE_RATE_LIMIT_PEERS", config.rate_limit_peers)?;
        if let Ok(seconds) = std::env::var("LIVEENGINE_RATE_LIMIT_WINDOW_SECONDS") {
            config.rate_limit_window = Duration::from_secs(seconds.parse().map_err(|_| {
                "LIVEENGINE_RATE_LIMIT_WINDOW_SECONDS must be an integer".to_string()
            })?);
        }
        if let Ok(seed) = std::env::var("LIVEENGINE_SEED") {
            config.seed = seed
                .parse()
                .map_err(|_| "LIVEENGINE_SEED must be an unsigned integer".to_string())?;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.token.len() < 24 {
            return Err("LIVEENGINE_TOKEN must be at least 24 bytes".into());
        }
        if self.max_hubs == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_args == 0
            || self.max_entities == 0
            || self.max_events == 0
            || self.max_generates_per_call == 0
            || self.max_inflight_calls == 0
            || self.rate_limit_requests == 0
            || self.rate_limit_peers == 0
            || self.rate_limit_window.is_zero()
            || self.request_timeout.is_zero()
        {
            return Err("hub limits and timeouts must be greater than zero".into());
        }
        Ok(())
    }
}

/// Uniform registration payload accepted by internal and external hubs.
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HubRegistration {
    pub id: String,
    pub mode: HubMode,
    #[serde(default)]
    pub address: Option<String>,
    /// Outbound credential for the remote service. Never exposed in HubInfo.
    #[serde(default, skip_serializing)]
    pub remote_token: Option<String>,
    #[serde(default)]
    pub remote_hub_id: Option<String>,
    #[serde(default = "empty_object")]
    pub scopes: Value,
    #[serde(default)]
    pub scope_schema: Option<Value>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HubMode {
    Internal,
    External,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HubInfo {
    pub id: String,
    pub mode: HubMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub remote_hub_id: String,
    pub scopes: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in_seconds: Option<u64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CallRequest {
    method: String,
    #[serde(default)]
    args: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportRegistryRequest {
    address: String,
    token: String,
    #[serde(default)]
    prefix: Option<String>,
}

#[derive(Clone)]
struct HubRecord {
    info: HubInfo,
    expires_at: Option<Instant>,
    handler: Option<Arc<Mutex<Box<dyn HubHandler>>>>,
    remote_token: Option<String>,
}

struct RateEntry {
    started: Instant,
    requests: usize,
}

enum HandlerCallError {
    Busy,
    Unavailable,
    Preflight(String),
    Call(String),
}

struct Inner {
    config: HubConfig,
    hubs: RwLock<HashMap<String, HubRecord>>,
    client: Client,
    rate: Mutex<HashMap<String, RateEntry>>,
    boot: Instant,
    inflight_calls: Arc<Semaphore>,
}

/// Cloneable state for a hosted hub registry.
#[derive(Clone)]
pub struct HubService {
    inner: Arc<Inner>,
}

impl HubService {
    pub fn new(config: HubConfig) -> Result<Self, String> {
        config.validate()?;
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| format!("could not create HTTP client: {e}"))?;
        let inflight_calls = Arc::new(Semaphore::new(config.max_inflight_calls));
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                hubs: RwLock::new(HashMap::new()),
                client,
                rate: Mutex::new(HashMap::new()),
                boot: Instant::now(),
                inflight_calls,
            }),
        })
    }

    /// Router for embedding in another Axum application. It owns routes rooted
    /// at `/health`, `/v1/contract`, and `/v1/hubs`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/v1/contract", get(contract))
            .route("/v1/contract/schema", get(contract_schema))
            .route("/v1/hubs", get(list_hubs).post(register_hub))
            .route("/v1/hubs/{id}", get(get_hub).delete(delete_hub))
            .route("/v1/hubs/{id}/call", post(call_hub))
            .route("/v1/registries/import", post(import_registry))
            .layer(axum::extract::DefaultBodyLimit::max(
                self.inner.config.max_request_bytes,
            ))
            .route_layer(middleware::from_fn_with_state(
                self.inner.clone(),
                secure_request,
            ))
            .with_state(self.inner.clone())
    }

    /// Add a custom internal hub handler to this registry. Custom handlers use
    /// the same request, response, argument, rate, and event result checks as
    /// built-in engines.
    pub async fn register_internal<H>(
        &self,
        registration: HubRegistration,
        handler: H,
    ) -> Result<HubInfo, String>
    where
        H: HubHandler + 'static,
    {
        if registration.mode != HubMode::Internal {
            return Err("register_internal requires mode=internal".into());
        }
        validate_registration(&self.inner, &registration)?;
        if registration.address.is_some()
            || registration.remote_hub_id.is_some()
            || registration.remote_token.is_some()
        {
            return Err("internal hubs cannot set remote address, id, or token".into());
        }
        let ttl = registration.ttl_seconds.map(Duration::from_secs);
        if ttl.is_some_and(|ttl| ttl.is_zero() || ttl > self.inner.config.max_ttl) {
            return Err("ttlSeconds is outside configured bounds".into());
        }
        let info = HubInfo {
            id: registration.id.clone(),
            mode: HubMode::Internal,
            address: None,
            remote_hub_id: registration.id.clone(),
            scopes: registration.scopes,
            scope_schema: registration.scope_schema,
            expires_in_seconds: ttl.map(|d| d.as_secs()),
        };
        let record = HubRecord {
            info: info.clone(),
            expires_at: ttl.map(|d| Instant::now() + d),
            handler: Some(Arc::new(Mutex::new(Box::new(handler)))),
            remote_token: None,
        };
        let mut hubs = self.inner.hubs.write().await;
        expire_hubs(&mut hubs);
        if hubs.contains_key(&registration.id) {
            return Err("hub id already registered".into());
        }
        if hubs.len() >= self.inner.config.max_hubs {
            return Err("hub registry is full".into());
        }
        hubs.insert(registration.id, record);
        Ok(info)
    }
}

async fn secure_request(
    State(state): State<Arc<Inner>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.uri().path() == "/health" {
        return next.run(request).await;
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string())
        .unwrap_or_else(|| "unavailable-peer".to_string());
    if !take_rate_slot(&state, peer) {
        return error(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded");
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .as_bytes();
    let expected = state.config.token.as_bytes();
    let mut difference = 0u8;
    for (index, expected_byte) in expected.iter().enumerate() {
        difference |= supplied.get(index).copied().unwrap_or(0) ^ expected_byte;
    }
    let equal = bool::from(supplied.len().ct_eq(&expected.len())) && difference == 0;
    if !equal {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    match tokio::time::timeout(state.config.request_timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => error(
            StatusCode::GATEWAY_TIMEOUT,
            "request exceeded configured timeout",
        ),
    }
}

fn take_rate_slot(state: &Inner, peer: String) -> bool {
    let now = Instant::now();
    let mut map = state.rate.lock().unwrap_or_else(|e| e.into_inner());
    map.retain(|_, entry| now.duration_since(entry.started) < state.config.rate_limit_window);
    if !map.contains_key(&peer) && map.len() >= state.config.rate_limit_peers {
        // Preserve active budgets: a new peer must not reset an existing peer's limit.
        return false;
    }
    let entry = map.entry(peer).or_insert(RateEntry {
        started: now,
        requests: 0,
    });
    if entry.requests >= state.config.rate_limit_requests {
        return false;
    }
    entry.requests += 1;
    true
}

async fn health(State(state): State<Arc<Inner>>) -> Json<Value> {
    Json(
        serde_json::json!({"status":"ok","protocol":HUB_PROTOCOL,"uptimeSeconds":state.boot.elapsed().as_secs()}),
    )
}
async fn contract() -> Json<Value> {
    Json(
        serde_json::json!({"protocol":HUB_PROTOCOL,"version":1,"endpoints":["/health","/v1/contract","/v1/hubs","/v1/hubs/{id}","/v1/hubs/{id}/call","/v1/registries/import"],"schema":"/v1/contract/schema"}),
    )
}
async fn contract_schema() -> Response {
    match serde_json::from_str::<Value>(include_str!("../docs/hub-contract.schema.json")) {
        Ok(schema) => Json(schema).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "contract schema is unavailable",
        ),
    }
}

async fn list_hubs(State(state): State<Arc<Inner>>) -> Json<Value> {
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    let mut list: Vec<HubInfo> = hubs.values().map(public_info).collect();
    list.sort_by(|a, b| a.id.cmp(&b.id));
    Json(serde_json::json!({"hubs":list}))
}

async fn get_hub(State(state): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    match hubs.get(&id) {
        Some(record) => Json(public_info(record)).into_response(),
        None => error(StatusCode::NOT_FOUND, "hub not found"),
    }
}

async fn register_hub(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<HubRegistration>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(reg) = match payload {
        Ok(v) => v,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("invalid registration: {e}"),
            )
        }
    };
    if let Err(message) = validate_registration(&state, &reg) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
    let id = reg.id.trim().to_owned();
    let remote_id = reg.remote_hub_id.clone().unwrap_or_else(|| id.clone());
    let address = match reg.mode {
        HubMode::Internal if reg.address.is_some() || reg.remote_hub_id.is_some() => {
            return error(
                StatusCode::BAD_REQUEST,
                "internal hubs cannot set address or remoteHubId",
            )
        }
        HubMode::Internal => None,
        HubMode::External => reg.address.clone(),
    };
    if let Some(address) = address.as_deref() {
        let remote_token = reg.remote_token.as_deref().unwrap_or("");
        if let Err(message) = remote_handshake(&state, address, &remote_id, remote_token).await {
            return error(StatusCode::BAD_GATEWAY, &message);
        }
    }
    let ttl = match reg.ttl_seconds {
        Some(0) => {
            return error(
                StatusCode::BAD_REQUEST,
                "ttlSeconds must be greater than zero",
            )
        }
        Some(seconds) if Duration::from_secs(seconds) > state.config.max_ttl => {
            return error(
                StatusCode::BAD_REQUEST,
                "ttlSeconds exceeds configured maximum",
            )
        }
        Some(seconds) => Some(Duration::from_secs(seconds)),
        None => None,
    };
    let info = HubInfo {
        id: id.clone(),
        mode: reg.mode,
        address,
        remote_hub_id: remote_id,
        scopes: reg.scopes,
        scope_schema: reg.scope_schema,
        expires_in_seconds: ttl.map(|d| d.as_secs()),
    };
    let record = HubRecord {
        info,
        expires_at: ttl.map(|d| Instant::now() + d),
        handler: if reg.mode == HubMode::Internal {
            Some(Arc::new(Mutex::new(Box::new(Engine::new(
                state.config.seed,
            )))))
        } else {
            None
        },
        remote_token: reg.remote_token,
    };
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    if hubs.contains_key(&id) {
        return error(StatusCode::CONFLICT, "hub id already registered");
    }
    if hubs.len() >= state.config.max_hubs {
        return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
    }
    hubs.insert(id, record.clone());
    (StatusCode::CREATED, Json(public_info(&record))).into_response()
}

async fn import_registry(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<ImportRegistryRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(request) = match payload {
        Ok(v) => v,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("invalid import request: {e}"),
            )
        }
    };
    if request.token.len() < 24 {
        return error(StatusCode::BAD_REQUEST, "token must be at least 24 bytes");
    }
    let prefix = request.prefix.unwrap_or_default();
    if prefix.len() > 64
        || !prefix
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "prefix contains invalid characters",
        );
    }
    let base = match validated_remote_url(&state, &request.address) {
        Ok(url) => url,
        Err(e) => return error(StatusCode::BAD_REQUEST, &e),
    };
    let health = match endpoint(&base, "health") {
        Ok(url) => match remote_get(&state, url, &request.token).await {
            Ok(value) => value,
            Err(e) => return error(StatusCode::BAD_GATEWAY, &e),
        },
        Err(e) => return error(StatusCode::BAD_REQUEST, &e),
    };
    if health.get("protocol").and_then(Value::as_str) != Some(HUB_PROTOCOL) {
        return error(StatusCode::BAD_GATEWAY, "remote health protocol mismatch");
    }
    let contract = match endpoint(&base, "v1/contract") {
        Ok(url) => match remote_get(&state, url, &request.token).await {
            Ok(value) => value,
            Err(e) => return error(StatusCode::BAD_GATEWAY, &e),
        },
        Err(e) => return error(StatusCode::BAD_REQUEST, &e),
    };
    if contract.get("protocol").and_then(Value::as_str) != Some(HUB_PROTOCOL) {
        return error(StatusCode::BAD_GATEWAY, "remote contract protocol mismatch");
    }
    let list_url = match endpoint(&base, "v1/hubs") {
        Ok(url) => url,
        Err(e) => return error(StatusCode::BAD_REQUEST, &e),
    };
    let remote_list = match remote_get(&state, list_url, &request.token).await {
        Ok(v) => v,
        Err(e) => return error(StatusCode::BAD_GATEWAY, &e),
    };
    let Some(remote_hubs) = remote_list.get("hubs").and_then(Value::as_array) else {
        return error(
            StatusCode::BAD_GATEWAY,
            "remote registry returned invalid hub list",
        );
    };
    if remote_hubs.len() > state.config.max_hubs {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "remote registry exceeds hub limit",
        );
    }
    let mut candidates = Vec::new();
    let mut discovered_ids = std::collections::HashSet::new();
    for remote in remote_hubs {
        let Some(remote_id) = remote.get("id").and_then(Value::as_str) else {
            return error(
                StatusCode::BAD_GATEWAY,
                "remote registry contains an invalid hub",
            );
        };
        let id = format!("{prefix}{remote_id}");
        if !discovered_ids.insert(id.clone()) {
            return error(
                StatusCode::BAD_GATEWAY,
                "remote registry contains duplicate hub ids",
            );
        }
        let registration = HubRegistration {
            id: id.clone(),
            mode: HubMode::External,
            address: Some(request.address.clone()),
            remote_token: Some(request.token.clone()),
            remote_hub_id: Some(remote_id.to_string()),
            scopes: remote.get("scopes").cloned().unwrap_or_else(empty_object),
            scope_schema: remote.get("scopeSchema").cloned(),
            ttl_seconds: None,
        };
        if let Err(e) = validate_registration(&state, &registration) {
            return error(
                StatusCode::BAD_GATEWAY,
                &format!("remote registry contains invalid hub metadata: {e}"),
            );
        }
        let info = HubInfo {
            id,
            mode: HubMode::External,
            address: Some(request.address.clone()),
            remote_hub_id: remote_id.to_string(),
            scopes: registration.scopes,
            scope_schema: registration.scope_schema,
            expires_in_seconds: None,
        };
        candidates.push(HubRecord {
            info,
            expires_at: None,
            handler: None,
            remote_token: Some(request.token.clone()),
        });
    }
    let (existing_infos, new_ids) = {
        let mut hubs = state.hubs.write().await;
        expire_hubs(&mut hubs);
        let mut existing_infos = Vec::new();
        let mut new_ids = Vec::new();
        for candidate in &candidates {
            if let Some(existing) = hubs.get(&candidate.info.id) {
                if existing.info.address.as_deref() != Some(&request.address)
                    || existing.info.remote_hub_id != candidate.info.remote_hub_id
                    || existing.remote_token.as_deref() != Some(&request.token)
                {
                    return error(
                        StatusCode::CONFLICT,
                        "import id is already mapped to another target",
                    );
                }
                existing_infos.push(existing.info.clone());
            } else {
                new_ids.push(candidate.info.id.clone());
            }
        }
        if hubs.len().saturating_add(new_ids.len()) > state.config.max_hubs {
            return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
        }
        (existing_infos, new_ids)
    };
    for candidate in candidates.iter().filter(|c| new_ids.contains(&c.info.id)) {
        if let Err(e) = remote_handshake(
            &state,
            &request.address,
            &candidate.info.remote_hub_id,
            &request.token,
        )
        .await
        {
            return error(StatusCode::BAD_GATEWAY, &e);
        }
    }
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    if hubs.len().saturating_add(new_ids.len()) > state.config.max_hubs {
        return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
    }
    let mut imported = existing_infos;
    for entry in candidates
        .into_iter()
        .filter(|c| new_ids.contains(&c.info.id))
    {
        if hubs.contains_key(&entry.info.id) {
            return error(
                StatusCode::CONFLICT,
                "import id was concurrently registered",
            );
        }
        imported.push(entry.info.clone());
        hubs.insert(entry.info.id.clone(), entry);
    }
    imported.sort_by(|a, b| a.id.cmp(&b.id));
    Json(serde_json::json!({"hubs": imported})).into_response()
}

async fn delete_hub(State(state): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    if hubs.remove(&id).is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "hub not found")
    }
}

async fn call_hub(
    State(state): State<Arc<Inner>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<CallRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(call) = match payload {
        Ok(v) => v,
        Err(e) => return error(StatusCode::BAD_REQUEST, &format!("invalid call: {e}")),
    };
    if call.method.is_empty() || call.method.len() > 128 {
        return error(
            StatusCode::BAD_REQUEST,
            "method must contain 1 to 128 bytes",
        );
    }
    if serde_json::to_vec(&call)
        .map(|body| body.len())
        .unwrap_or(usize::MAX)
        > state.config.max_request_bytes
    {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "call exceeds configured request size limit",
        );
    }
    if call.args.len() > state.config.max_args {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "too many arguments");
    }
    if call.method == "generateUsers"
        && call
            .args
            .first()
            .and_then(Value::as_u64)
            .is_some_and(|n| n as usize > state.config.max_generates_per_call)
    {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "generate count exceeds configured maximum",
        );
    }
    if call.args.iter().any(|v| json_depth(v, 0) > 64) {
        return error(StatusCode::BAD_REQUEST, "argument nesting is too deep");
    }
    let record = {
        let mut hubs = state.hubs.write().await;
        expire_hubs(&mut hubs);
        hubs.get(&id).cloned()
    };
    let Some(record) = record else {
        return error(StatusCode::NOT_FOUND, "hub not found");
    };
    if let Some(schema) = record.info.scope_schema.as_ref() {
        if let Err(message) = validate_call_scopes(schema, &call) {
            return error(StatusCode::BAD_REQUEST, &message);
        }
    }
    let permit = match state.inflight_calls.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "hub call capacity is busy"),
    };
    let hop = match headers
        .get("x-liveengine-hop")
        .and_then(|v| v.to_str().ok())
    {
        None => 0,
        Some(value) => match value.parse::<u8>() {
            Ok(hop) if hop <= 2 => hop,
            _ => return error(StatusCode::BAD_REQUEST, "invalid forwarding hop"),
        },
    };
    if record.info.mode == HubMode::External && hop >= 2 {
        return error(StatusCode::LOOP_DETECTED, "forwarding hop limit reached");
    }
    let result = if let Some(handler) = record.handler {
        let config = state.config.clone();
        let method = call.method;
        let args = call.args;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut handler = match handler.try_lock() {
                Ok(handler) => handler,
                Err(std::sync::TryLockError::WouldBlock) => return Err(HandlerCallError::Busy),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(HandlerCallError::Unavailable)
                }
            };
            handler
                .preflight(&method, &args, &config)
                .map_err(HandlerCallError::Preflight)?;
            handler.call(&method, args).map_err(HandlerCallError::Call)
        })
        .await;
        match task {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(HandlerCallError::Busy)) => {
                return error(StatusCode::CONFLICT, "hub handler is busy")
            }
            Ok(Err(HandlerCallError::Unavailable)) => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "hub handler is unavailable",
                )
            }
            Ok(Err(HandlerCallError::Preflight(message))) => {
                return error(StatusCode::PAYLOAD_TOO_LARGE, &message)
            }
            Ok(Err(HandlerCallError::Call(message))) => Err(message),
            Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "hub handler task failed"),
        }
    } else {
        let _permit = permit;
        let Some(address) = record.info.address.as_deref() else {
            return error(StatusCode::BAD_GATEWAY, "external hub address is missing");
        };
        forward_call(
            &state,
            address,
            &record.info.remote_hub_id,
            record.remote_token.as_deref().unwrap_or(""),
            hop + 1,
            &call,
        )
        .await
    };
    match result {
        Ok(value) => {
            if let Some(reason) = validate_result(&value, &state.config) {
                return error(StatusCode::PAYLOAD_TOO_LARGE, &reason);
            }
            Json(serde_json::json!({"result":value})).into_response()
        }
        Err(message) if message.starts_with("remote:") => error(
            StatusCode::BAD_GATEWAY,
            message.trim_start_matches("remote: "),
        ),
        Err(message) => error(StatusCode::BAD_REQUEST, &message),
    }
}

async fn remote_handshake(
    state: &Inner,
    address: &str,
    remote_id: &str,
    token: &str,
) -> Result<(), String> {
    if token.len() < 24 {
        return Err("remoteToken must be at least 24 bytes".into());
    }
    let base = validated_remote_url(state, address)?;
    let health = remote_get(state, endpoint(&base, "health")?, token).await?;
    if health.get("protocol").and_then(Value::as_str) != Some(HUB_PROTOCOL) {
        return Err("remote health protocol mismatch".into());
    }
    let contract = remote_get(state, endpoint(&base, "v1/contract")?, token).await?;
    if contract.get("protocol").and_then(Value::as_str) != Some(HUB_PROTOCOL) {
        return Err("remote contract protocol mismatch".into());
    }
    let _hub = remote_get(
        state,
        endpoint(&base, &format!("v1/hubs/{remote_id}"))?,
        token,
    )
    .await
    .map_err(|_| "remote hub id is not present in the remote registry".to_string())?;
    Ok(())
}

fn validated_remote_url(state: &Inner, address: &str) -> Result<Url, String> {
    if address.len() > 2048 {
        return Err("remote address is too long".into());
    }
    let url = Url::parse(address)
        .map_err(|_| "remote address must be an absolute HTTP(S) URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("remote address must be a plain HTTP(S) base URL".into());
    }
    let host = url
        .host_str()
        .unwrap()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let allowed = state.config.allowed_remote_hosts.iter().any(|entry| {
        let entry = entry.trim().trim_end_matches('.').to_ascii_lowercase();
        entry == host
            || entry
                .strip_prefix("*.")
                .map(|suffix| host.ends_with(&format!(".{suffix}")) && host != suffix)
                .unwrap_or(false)
    });
    if !allowed {
        return Err("remote host is not in LIVEENGINE_ALLOWED_REMOTE_HOSTS".into());
    }
    Ok(url)
}

fn endpoint(base: &Url, suffix: &str) -> Result<Url, String> {
    let mut url = base.clone();
    let path = format!("{}/{}", base.path().trim_end_matches('/'), suffix);
    url.set_path(&path);
    Ok(url)
}

async fn remote_get(state: &Inner, url: Url, token: &str) -> Result<Value, String> {
    let response = state
        .client
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("remote: remote request failed: {e}"))?;
    read_remote_json(response, state.config.max_response_bytes).await
}

async fn read_remote_json(response: reqwest::Response, max_bytes: usize) -> Result<Value, String> {
    if !response.status().is_success() {
        return Err(format!(
            "remote: remote returned HTTP {}",
            response.status()
        ));
    }
    if response
        .content_length()
        .is_some_and(|len| len > max_bytes as u64)
    {
        return Err("remote: remote response exceeds size limit".into());
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("remote: failed reading remote response: {e}"))?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err("remote: remote response exceeds size limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "remote: remote returned invalid JSON".into())
}

async fn forward_call(
    state: &Inner,
    address: &str,
    remote_id: &str,
    token: &str,
    hop: u8,
    call: &CallRequest,
) -> Result<Value, String> {
    let base = validated_remote_url(state, address).map_err(|e| format!("remote: {e}"))?;
    let url = endpoint(&base, &format!("v1/hubs/{remote_id}/call"))
        .map_err(|e| format!("remote: {e}"))?;
    let response = state
        .client
        .post(url)
        .bearer_auth(token)
        .header("x-liveengine-hop", hop.to_string())
        .json(call)
        .send()
        .await
        .map_err(|e| format!("remote: remote request failed: {e}"))?;
    let body = read_remote_json(response, state.config.max_response_bytes).await?;
    body.get("result")
        .cloned()
        .ok_or_else(|| "remote: remote response has no result field".into())
}

fn validate_registration(state: &Inner, r: &HubRegistration) -> Result<(), String> {
    let id = r.id.trim();
    if id != r.id
        || matches!(id, "." | "..")
        || id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
    {
        return Err("id must be 1 to 128 ASCII letters, digits, '.', '_' or '-'".into());
    }
    if !r.scopes.is_object() {
        return Err("scopes must be an object".into());
    }
    if serde_json::to_vec(&r.scopes)
        .map(|x| x.len())
        .unwrap_or(usize::MAX)
        > state.config.max_request_bytes
    {
        return Err("scopes exceed size limit".into());
    }
    if let Some(schema) = &r.scope_schema {
        validate_scope_schema(&r.scopes, schema)?;
    }
    match r.mode {
        HubMode::Internal
            if r.address.is_some() || r.remote_hub_id.is_some() || r.remote_token.is_some() =>
        {
            return Err("internal hubs cannot set address, remoteHubId, or remoteToken".into())
        }
        HubMode::External if r.address.is_none() => {
            return Err("external hubs require address".into())
        }
        HubMode::External if r.remote_token.as_ref().is_none_or(|token| token.len() < 24) => {
            return Err("external hubs require remoteToken of at least 24 bytes".into())
        }
        HubMode::External
            if r.remote_hub_id.as_ref().is_some_and(|id| {
                matches!(id.as_str(), "." | "..")
                    || id.is_empty()
                    || id.len() > 128
                    || !id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
            }) =>
        {
            return Err(
                "remoteHubId must be 1 to 128 ASCII letters, digits, '.', '_' or '-'".into(),
            )
        }
        _ => (),
    }
    if let Some(address) = r.address.as_deref() {
        validated_remote_url(state, address)?;
    }
    Ok(())
}

/// `scopeSchema` supports a small JSON-schema subset: object type, required,
/// properties with primitive `type` and `enum`, and additionalProperties=false.
fn validate_scope_schema(scopes: &Value, schema: &Value) -> Result<(), String> {
    if schema
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t != "object")
    {
        return Err("scopeSchema type must be object".into());
    }
    let object = scopes.as_object().ok_or("scopes must be an object")?;
    if let Some(required) = schema.get("required") {
        let values = required
            .as_array()
            .ok_or("scopeSchema.required must be an array")?;
        for key in values {
            let key = key
                .as_str()
                .ok_or("scopeSchema.required entries must be strings")?;
            if !object.contains_key(key) {
                return Err(format!("scope '{key}' is required"));
            }
        }
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    if schema.get("properties").is_some() && properties.is_none() {
        return Err("scopeSchema.properties must be an object".into());
    }
    for (key, value) in object {
        let Some(rule) = properties.and_then(|p| p.get(key)) else {
            if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!("scope '{key}' is not allowed"));
            }
            continue;
        };
        if let Some(kind) = rule.get("type").and_then(Value::as_str) {
            let ok = match kind {
                "string" => value.is_string(),
                "number" => value.is_number(),
                "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                "null" => value.is_null(),
                _ => return Err(format!("scopeSchema has unsupported type '{kind}'")),
            };
            if !ok {
                return Err(format!("scope '{key}' must have type {kind}"));
            }
        }
        if let Some(choices) = rule.get("enum").and_then(Value::as_array) {
            if !choices.contains(value) {
                return Err(format!("scope '{key}' is not an allowed value"));
            }
        }
    }
    Ok(())
}

fn validate_call_scopes(schema: &Value, call: &CallRequest) -> Result<(), String> {
    let items: Vec<&Value> = match call.method.as_str() {
        "generateUsers" => return Err("generateUsers requires a hub without scopeSchema; generate locally and upsert validated users".into()),
        "upsertUser" | "upsertServer" => call.args.first().into_iter().collect(),
        "upsertUsers" | "upsertServers" => call
            .args
            .first()
            .and_then(Value::as_array)
            .map(|values| values.iter().collect())
            .unwrap_or_default(),
        _ => return Ok(()),
    };
    for item in items {
        let scopes = item.get("scopes").cloned().unwrap_or_else(empty_object);
        validate_scope_schema(&scopes, schema)?;
    }
    Ok(())
}

fn public_info(record: &HubRecord) -> HubInfo {
    let mut info = record.info.clone();
    info.expires_in_seconds = record
        .expires_at
        .map(|at| at.saturating_duration_since(Instant::now()).as_secs());
    info
}
fn expire_hubs(hubs: &mut HashMap<String, HubRecord>) {
    let now = Instant::now();
    hubs.retain(|_, r| r.expires_at.is_none_or(|at| at > now));
}
fn json_depth(value: &Value, depth: usize) -> usize {
    if depth > 64 {
        return depth;
    }
    match value {
        Value::Array(a) => a
            .iter()
            .map(|v| json_depth(v, depth + 1))
            .max()
            .unwrap_or(depth),
        Value::Object(o) => o
            .values()
            .map(|v| json_depth(v, depth + 1))
            .max()
            .unwrap_or(depth),
        _ => depth,
    }
}
fn count_named(value: &Value, name: &str) -> usize {
    match value {
        Value::Array(a) => a.iter().map(|v| count_named(v, name)).sum(),
        Value::Object(o) => o
            .iter()
            .map(|(k, v)| {
                if k == name {
                    v.as_array().map_or(1, Vec::len)
                } else {
                    count_named(v, name)
                }
            })
            .sum(),
        _ => 0,
    }
}
fn preflight_internal_call(
    engine: &mut Engine,
    call: &CallRequest,
    config: &HubConfig,
) -> Result<(), String> {
    let stats = engine.dispatch("stats", vec![])?;
    let users = stats.get("users").and_then(Value::as_u64).unwrap_or(0) as usize;
    let servers = stats.get("servers").and_then(Value::as_u64).unwrap_or(0) as usize;
    let pending = stats
        .get("pendingEvents")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    if call.method == "generateUsers" {
        let count = call
            .args
            .first()
            .and_then(Value::as_u64)
            .ok_or("count must be a number")? as usize;
        if count > config.max_generates_per_call {
            return Err("generate count exceeds configured maximum".into());
        }
        if users.saturating_add(servers).saturating_add(count) > config.max_entities {
            return Err("entity count exceeds configured maximum".into());
        }
    }
    let (kind, bulk) = match call.method.as_str() {
        "upsertUser" => (Some("user"), false),
        "upsertUsers" => (Some("user"), true),
        "upsertServer" => (Some("server"), false),
        "upsertServers" => (Some("server"), true),
        _ => (None, false),
    };
    if let Some(kind) = kind {
        let items: Vec<&Value> = if bulk {
            call.args
                .first()
                .and_then(Value::as_array)
                .ok_or("expected an array")?
                .iter()
                .collect()
        } else {
            vec![call.args.first().ok_or("missing entity")?]
        };
        if items.len() > config.max_entities {
            return Err("entity count exceeds configured maximum".into());
        }
        let mut newly_added = 0usize;
        let mut seen = std::collections::HashSet::new();
        for item in &items {
            let id = item.get("id").map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string())
            });
            if let Some(id) = id.filter(|id| seen.insert(id.clone())) {
                let method = if kind == "user" {
                    "getUser"
                } else {
                    "getServer"
                };
                if engine.dispatch(method, vec![Value::String(id)])?.is_null() {
                    newly_added += 1;
                }
            } else {
                newly_added += 1;
            }
        }
        if users.saturating_add(servers).saturating_add(newly_added) > config.max_entities {
            return Err("entity count exceeds configured maximum".into());
        }
    }
    let projected = match call.method.as_str() {
        "upsertUser" | "removeUser" | "setUserStatus" => 3,
        "upsertUsers" => call
            .args
            .first()
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        "upsertServer" | "setServerStatus" => 2,
        "upsertServers" => call
            .args
            .first()
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
            .saturating_mul(2),
        "generateUsers" => call.args.first().and_then(Value::as_u64).unwrap_or(0) as usize,
        "removeServer" => {
            let members = call
                .args
                .first()
                .and_then(Value::as_str)
                .and_then(|id| {
                    engine
                        .dispatch("getServer", vec![Value::String(id.to_owned())])
                        .ok()
                })
                .and_then(|server| {
                    server
                        .get("members")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                })
                .unwrap_or(0);
            members.saturating_mul(2).saturating_add(1)
        }
        "join" | "autoJoin" => 4,
        "leave" => 2,
        "matchAll" => users.saturating_mul(5),
        "advertise" => 1,
        "advertiseAll" => users,
        "clear" => 1,
        "drainEvents" | "getUser" | "getServer" | "listUsers" | "listServers" | "fits"
        | "stats" | "pendingEvents" | "findServersForUser" | "findUsersForServer" => 0,
        _ => 0,
    };
    let effective_pending = if call.method == "clear" { 0 } else { pending };
    if effective_pending.saturating_add(projected) > config.max_events {
        return Err("event count exceeds configured maximum; drain events first".into());
    }
    Ok(())
}
fn validate_result(value: &Value, config: &HubConfig) -> Option<String> {
    if serde_json::to_vec(value)
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
        > config.max_response_bytes
    {
        return Some("response exceeds size limit".into());
    }
    if count_named(value, "entities") > config.max_entities {
        return Some("entity count exceeds configured maximum".into());
    }
    if count_named(value, "events") > config.max_events {
        return Some("event count exceeds configured maximum".into());
    }
    None
}
fn empty_object() -> Value {
    Value::Object(Map::new())
}
fn env_limit(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| format!("{name} must be a positive integer")),
        Err(_) => Ok(default),
    }
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({"error":message}))).into_response()
}
