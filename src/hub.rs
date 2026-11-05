//! HTTP hosting and registry support for the `liveengine.hub/v1` protocol.
//!
//! A [`HubService`] can be mounted in an existing Axum application with
//! [`HubService::router`], or served directly by the `liveengine-hub` binary.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Query, State},
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
    pub max_depth: usize,
    pub instance_id: Option<String>,
    /// Optional durable registry snapshot path.
    pub state_file: Option<std::path::PathBuf>,
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
            max_depth: 8,
            instance_id: None,
            state_file: None,
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
        config.max_depth = env_limit("LIVEENGINE_MAX_DEPTH", config.max_depth)?;
        config.state_file = std::env::var_os("LIVEENGINE_STATE_FILE").map(Into::into);
        config.instance_id = if config.state_file.is_some() {
            std::env::var("LIVEENGINE_INSTANCE_ID").ok()
        } else {
            Some(instance_id_from_env()?)
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
        if self.max_depth == 0 || self.max_depth > 16 {
            return Err("LIVEENGINE_MAX_DEPTH must be between 1 and 16".into());
        }
        if self
            .instance_id
            .as_ref()
            .is_some_and(|id| !valid_trace_id(id))
        {
            return Err("invalid LIVEENGINE_INSTANCE_ID".into());
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

fn instance_id_from_env() -> Result<String, String> {
    if let Ok(id) = std::env::var("LIVEENGINE_INSTANCE_ID") {
        return Ok(id);
    }
    use std::hash::{Hash, Hasher};
    use std::io::Write;
    let path = if let Ok(file) = std::env::var("LIVEENGINE_INSTANCE_ID_FILE") {
        std::path::PathBuf::from(file)
    } else if let Ok(file) = std::env::var("LIVEENGINE_STATE_FILE") {
        std::path::PathBuf::from(format!("{file}.instance-id"))
    } else {
        let root = std::env::var_os("XDG_STATE_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".local/state"))
            })
            .ok_or("cannot locate instance id storage; set LIVEENGINE_INSTANCE_ID")?;
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        std::env::var("LIVEENGINE_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8787".into())
            .hash(&mut hash);
        root.join("liveengine")
            .join(format!("instance-{:016x}", hash.finish()))
    };
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|_| "cannot create instance id directory")?;
    }
    let id = format!("{:032x}", rand::random::<u128>());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&path) {
        Ok(mut file) => {
            file.write_all(id.as_bytes())
                .and_then(|_| file.sync_all())
                .map_err(|_| "cannot persist instance id")?;
            Ok(id)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = std::fs::read_to_string(&path).map_err(|_| "cannot read instance id")?;
            if !valid_trace_id(&existing) {
                return Err("invalid persisted instance id".into());
            }
            Ok(existing)
        }
        Err(_) => Err("cannot persist instance id".into()),
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
    Central,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_hubs: Option<usize>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CallRequest {
    method: String,
    #[serde(default)]
    args: Vec<Value>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImportRegistryRequest {
    address: String,
    token: String,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    refresh_seconds: Option<u64>,
}

#[derive(Clone)]
struct HubRecord {
    info: HubInfo,
    expires_at: Option<Instant>,
    handler: Option<Arc<Mutex<Box<dyn HubHandler>>>>,
    remote_token: Option<String>,
    child: Option<Arc<Inner>>,
    custom: bool,
    _slot: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
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
    schemas: Arc<RwLock<HashMap<String, Value>>>,
    templates: Arc<RwLock<HashMap<String, Value>>>,
    reaper_started: AtomicBool,
    client: Client,
    rate: Mutex<HashMap<String, RateEntry>>,
    boot: Instant,
    inflight_calls: Arc<Semaphore>,
    hub_slots: Arc<Semaphore>,
    global_max_hubs: usize,
    instance_id: String,
    inherited: [Option<Value>; 3],
    imports: Arc<RwLock<Vec<ImportRegistryRequest>>>,
    persistence_lock: Arc<tokio::sync::Mutex<()>>,
    root: bool,
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
        let global_max_hubs = config.max_hubs;
        let hub_slots = Arc::new(Semaphore::new(global_max_hubs));
        let instance_id = config
            .instance_id
            .clone()
            .unwrap_or_else(|| format!("{:032x}", rand::random::<u128>()));
        let snapshot = load_snapshot(config.state_file.as_deref())?;
        let instance_id = snapshot
            .as_ref()
            .map(|s| s.instance_id.clone())
            .unwrap_or(instance_id);
        let service = Self {
            inner: Arc::new(Inner {
                config,
                hubs: RwLock::new(HashMap::new()),
                schemas: Arc::new(RwLock::new(HashMap::from([(
                    "liveengine.default@1".to_string(),
                    serde_json::json!({"type":"object"}),
                )]))),
                templates: Arc::new(RwLock::new(HashMap::new())),
                reaper_started: AtomicBool::new(false),
                client,
                rate: Mutex::new(HashMap::new()),
                boot: Instant::now(),
                inflight_calls,
                global_max_hubs,
                hub_slots,
                instance_id,
                inherited: [None, None, None],
                imports: Arc::new(RwLock::new(Vec::new())),
                persistence_lock: Arc::new(tokio::sync::Mutex::new(())),
                root: true,
            }),
        };
        if let Some(snapshot) = snapshot {
            restore_snapshot(&service.inner, snapshot)?;
        }
        Ok(service)
    }

    /// Create or verify the initial durable snapshot before accepting requests.
    pub async fn initialize_state(&self) -> Result<(), String> {
        persist(&self.inner).await
    }

    /// Router for embedding in another Axum application. It owns routes rooted
    /// at `/health`, `/v1/contract`, and `/v1/hubs`.
    pub fn router(&self) -> Router {
        start_reaper(&self.inner);
        Router::new()
            .route("/health", get(health))
            .route("/v1/ping", get(ping))
            .route("/v1/contract", get(contract))
            .route("/v1/contract/schema", get(contract_schema))
            .route("/v1/schemas", get(list_schemas).post(create_schema))
            .route("/v1/schemas/{id}", get(get_schema).delete(delete_schema))
            .route("/v1/templates", get(list_templates).post(create_template))
            .route(
                "/v1/templates/{name}",
                get(get_template).delete(delete_template),
            )
            .route("/v1/route", post(route_hub))
            .route("/v1/tree", get(get_tree))
            .route("/v1/hubs", get(list_hubs).post(register_hub))
            .route("/v1/hubs/{id}", get(get_hub).delete(delete_hub))
            .route("/v1/hubs/{id}/call", post(call_hub))
            .route("/v1/registries/import", post(import_registry))
            .route("/v1/registry/export", get(export_registry))
            .layer(axum::extract::DefaultBodyLimit::max(
                self.inner.config.max_request_bytes,
            ))
            .route_layer(middleware::from_fn_with_state(
                self.inner.clone(),
                persist_request,
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
            schema_ref: None,
            user_schema: None,
            server_schema: None,
            max_hubs: None,
            stale: false,
        };
        let mut record = HubRecord {
            info: info.clone(),
            expires_at: ttl.map(|d| Instant::now() + d),
            handler: Some(Arc::new(Mutex::new(Box::new(handler)))),
            remote_token: None,
            child: None,
            custom: true,
            _slot: None,
        };
        let mut hubs = self.inner.hubs.write().await;
        expire_hubs(&mut hubs);
        if let Some(existing) = hubs.get_mut(&registration.id) {
            if existing.custom && existing.handler.is_none() {
                existing.handler = record.handler;
                let restored = existing.info.clone();
                drop(hubs);
                persist(&self.inner).await?;
                return Ok(restored);
            }
            return Err("hub id already registered".into());
        }
        if hubs.len() >= self.inner.config.max_hubs {
            return Err("hub registry is full".into());
        }
        record._slot = Some(Arc::new(
            self.inner
                .hub_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| "hub registry is full")?,
        ));
        hubs.insert(registration.id, record);
        drop(hubs);
        persist(&self.inner).await?;
        Ok(info)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u8,
    instance_id: String,
    schemas: HashMap<String, Value>,
    templates: HashMap<String, Value>,
    imports: Vec<ImportRegistryRequest>,
    hubs: Vec<SavedHub>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedHub {
    info: HubInfo,
    remote_token: Option<String>,
    expires_at: Option<u64>,
    children: Vec<SavedHub>,
    #[serde(default)]
    custom: bool,
}

fn load_snapshot(path: Option<&std::path::Path>) -> Result<Option<Snapshot>, String> {
    let Some(path) = path else { return Ok(None) };
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot open state file".into()),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if file
            .metadata()
            .map_err(|_| "cannot inspect state file")?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err("state file must not be group- or world-accessible".into());
        }
    }
    let snapshot: Snapshot = serde_json::from_reader(file).map_err(|_| "invalid state file")?;
    if snapshot.version != 1 || !valid_trace_id(&snapshot.instance_id) {
        return Err("invalid state file".into());
    }
    Ok(Some(snapshot))
}

fn restore_snapshot(state: &Arc<Inner>, snapshot: Snapshot) -> Result<(), String> {
    if snapshot.schemas.len() > state.config.max_hubs
        || snapshot.templates.len() > state.config.max_hubs
        || snapshot.imports.len() > state.config.max_hubs
    {
        return Err("state file exceeds registry limits".into());
    }
    for schema in snapshot.schemas.values() {
        compile_schema(schema).map_err(|_| "invalid state file schema")?;
    }
    if snapshot.schemas.get("liveengine.default@1") != Some(&serde_json::json!({"type":"object"})) {
        return Err("invalid state file schemas".into());
    }
    for import in &snapshot.imports {
        validated_remote_url(state, &import.address).map_err(|_| "invalid state file import")?;
        if import.token.len() < 24 || import.refresh_seconds.is_none_or(|s| s == 0 || s > 86400) {
            return Err("invalid state file import".into());
        }
    }
    *state
        .schemas
        .try_write()
        .map_err(|_| "state initialization failed")? = snapshot.schemas;
    *state
        .templates
        .try_write()
        .map_err(|_| "state initialization failed")? = snapshot.templates;
    *state
        .imports
        .try_write()
        .map_err(|_| "state initialization failed")? = snapshot.imports;
    restore_hubs(state, snapshot.hubs, 0)
}
fn restore_hubs(state: &Arc<Inner>, saved: Vec<SavedHub>, depth: usize) -> Result<(), String> {
    if depth > state.config.max_depth || saved.len() > state.config.max_hubs {
        return Err("state file exceeds hub limits".into());
    }
    let mut hubs = state
        .hubs
        .try_write()
        .map_err(|_| "state initialization failed")?;
    for saved in saved {
        let info = saved.info;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "invalid clock")?
            .as_secs();
        if saved.expires_at.is_some_and(|t| t <= now) {
            continue;
        }
        let reg = HubRegistration {
            id: info.id.clone(),
            mode: info.mode,
            address: info.address.clone(),
            remote_token: saved.remote_token.clone(),
            remote_hub_id: if info.mode != HubMode::External {
                None
            } else {
                Some(info.remote_hub_id.clone())
            },
            scopes: info.scopes.clone(),
            scope_schema: info.scope_schema.clone(),
            ttl_seconds: None,
        };
        validate_registration(state, &reg).map_err(|_| "invalid state file hub")?;
        let child = if info.mode == HubMode::Central && info.address.is_none() {
            let mut config = state.config.clone();
            config.state_file = None;
            config.max_hubs = info.max_hubs.unwrap_or(state.global_max_hubs);
            let service = HubService::new(config)?;
            let mut inner =
                Arc::try_unwrap(service.inner).map_err(|_| "state initialization failed")?;
            inner.root = false;
            inner.global_max_hubs = state.global_max_hubs;
            inner.hub_slots = state.hub_slots.clone();
            inner.schemas = state.schemas.clone();
            inner.templates = state.templates.clone();
            inner.imports = state.imports.clone();
            inner.persistence_lock = state.persistence_lock.clone();
            inner.inflight_calls = state.inflight_calls.clone();
            inner.instance_id = state.instance_id.clone();
            let schemas = state
                .schemas
                .try_read()
                .map_err(|_| "state initialization failed")?;
            inner.inherited = [
                info.schema_ref
                    .as_ref()
                    .and_then(|key| schemas.get(key).cloned())
                    .or(info.scope_schema.clone())
                    .or(state.inherited[0].clone()),
                info.user_schema.clone().or(state.inherited[1].clone()),
                info.server_schema.clone().or(state.inherited[2].clone()),
            ];
            drop(schemas);
            let child = Arc::new(inner);
            restore_hubs(&child, saved.children, depth + 1)?;
            Some(child)
        } else {
            if !saved.children.is_empty() {
                return Err("invalid state file tree".into());
            }
            None
        };
        let slot = state
            .hub_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "state file exceeds global hub limit")?;
        let record = HubRecord {
            handler: if info.mode == HubMode::Internal && !saved.custom {
                Some(Arc::new(Mutex::new(Box::new(Engine::new(
                    state.config.seed,
                )))))
            } else {
                None
            },
            expires_at: saved
                .expires_at
                .map(|t| Instant::now() + Duration::from_secs(t - now)),
            remote_token: saved.remote_token,
            custom: saved.custom,
            child,
            _slot: Some(Arc::new(slot)),
            info,
        };
        if hubs.insert(record.info.id.clone(), record).is_some() {
            return Err("duplicate hub in state file".into());
        }
    }
    Ok(())
}
fn snapshot_hubs(
    state: &Inner,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<SavedHub>> + Send + '_>> {
    Box::pin(async move {
        let records: Vec<_> = state.hubs.read().await.values().cloned().collect();
        let mut saved = Vec::new();
        for record in records {
            let remaining = record
                .expires_at
                .map(|t| t.saturating_duration_since(Instant::now()));
            if remaining.is_some_and(|d| d.is_zero()) {
                continue;
            }
            let expires_at = remaining.map(|d| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    .saturating_add(d.as_secs().max(1))
            });
            let children = match record.child {
                Some(child) => snapshot_hubs(&child).await,
                None => Vec::new(),
            };
            saved.push(SavedHub {
                info: record.info,
                remote_token: record.remote_token,
                expires_at,
                children,
                custom: record.custom,
            });
        }
        saved
    })
}
async fn persist(state: &Inner) -> Result<(), String> {
    let Some(path) = &state.config.state_file else {
        return Ok(());
    };
    let _guard = state.persistence_lock.lock().await;
    let snapshot = Snapshot {
        version: 1,
        instance_id: state.instance_id.clone(),
        schemas: state.schemas.read().await.clone(),
        templates: state.templates.read().await.clone(),
        imports: state.imports.read().await.clone(),
        hubs: snapshot_hubs(state).await,
    };
    let bytes = serde_json::to_vec(&snapshot).map_err(|_| "cannot encode state file")?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let temp = parent.join(format!(".liveengine-{:032x}.tmp", rand::random::<u128>()));
    let result = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
        return Err("cannot persist state file".into());
    }
    Ok(())
}
async fn persist_request(
    State(state): State<Arc<Inner>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let mutation = request.method() == axum::http::Method::POST
        || request.method() == axum::http::Method::DELETE;
    let response = next.run(request).await;
    if mutation && response.status().is_success() && persist(&state).await.is_err() {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "cannot persist state file",
        );
    }
    response
}

fn start_reaper(state: &Arc<Inner>) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        if !state.reaper_started.swap(true, Ordering::AcqRel) {
            let weak = Arc::downgrade(state);
            runtime.spawn(async move {
                let mut last_refresh: HashMap<(String, Option<String>), Instant> = HashMap::new();
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let Some(state) = weak.upgrade() else { break };
                    let expired = {
                        let mut hubs = state.hubs.write().await;
                        let before = hubs.len();
                        expire_hubs(&mut hubs);
                        before != hubs.len()
                    };
                    if expired {
                        let _ = persist(&state).await;
                    }
                    let imports = if state.root {
                        state.imports.read().await.clone()
                    } else {
                        Vec::new()
                    };
                    for request in imports {
                        let key = (request.address.clone(), request.prefix.clone());
                        let interval =
                            Duration::from_secs(request.refresh_seconds.unwrap_or(86400));
                        if last_refresh
                            .get(&key)
                            .is_none_or(|last| last.elapsed() >= interval)
                        {
                            last_refresh.insert(key, Instant::now());
                            let result = tokio::time::timeout(
                                state.config.request_timeout,
                                import_registry(State(state.clone()), Ok(Json(request))),
                            )
                            .await;
                            if result.is_ok_and(|response| response.status().is_success()) {
                                let _ = persist(&state).await;
                            }
                        }
                    }
                }
            });
        }
    }
}

async fn secure_request(
    State(state): State<Arc<Inner>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    start_reaper(&state);
    if matches!(request.uri().path(), "/health" | "/v1/ping") {
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
    if header_depth(request.headers()).is_none() {
        return error(StatusCode::BAD_REQUEST, "invalid forwarding depth");
    }
    if let Err(status) = trace(&state, request.headers()) {
        return error(status, "invalid or repeated forwarding trace");
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

/// Lightweight, unauthenticated latency probe. The server reports its own
/// processing time so callers can separate server work from network RTT.
async fn ping(State(state): State<Arc<Inner>>) -> Json<Value> {
    let started = Instant::now();
    let server_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let processing_micros = started.elapsed().as_micros() as u64;
    Json(serde_json::json!({
        "protocol": HUB_PROTOCOL,
        "pong": true,
        "serverTimeMs": server_time_ms,
        "processingMicros": processing_micros,
        "uptimeSeconds": state.boot.elapsed().as_secs(),
    }))
}

async fn contract() -> Json<Value> {
    Json(
        serde_json::json!({"protocol":HUB_PROTOCOL,"version":1,"endpoints":["/health","/v1/ping","/v1/contract","/v1/hubs","/v1/hubs/{id}","/v1/hubs/{id}/call","/v1/registries/import","/v1/registry/export","/v1/schemas","/v1/schemas/{id}","/v1/templates","/v1/templates/{name}","/v1/route","/v1/tree"],"schema":"/v1/contract/schema"}),
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaRegistration {
    id: String,
    version: String,
    schema: Value,
}
fn schema_key(id: &str, version: &str) -> Option<String> {
    if [id, version].iter().all(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }) {
        Some(format!("{id}@{version}"))
    } else {
        None
    }
}
async fn create_schema(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<SchemaRegistration>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(reg) = match payload {
        Ok(v) => v,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid schema registration"),
    };
    let Some(key) = schema_key(&reg.id, &reg.version) else {
        return error(StatusCode::BAD_REQUEST, "invalid schema id or version");
    };
    if let Err(e) = compile_schema(&reg.schema) {
        return error(StatusCode::BAD_REQUEST, &e);
    }
    let mut schemas = state.schemas.write().await;
    if schemas.contains_key(&key) {
        return error(StatusCode::CONFLICT, "schema already exists");
    }
    if schemas.len() >= state.config.max_hubs {
        return error(StatusCode::INSUFFICIENT_STORAGE, "schema registry is full");
    }
    schemas.insert(key.clone(), reg.schema.clone());
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"id":reg.id,"version":reg.version,"ref":key,"schema":reg.schema})),
    )
        .into_response()
}
async fn list_schemas(State(state): State<Arc<Inner>>) -> Json<Value> {
    let schemas = state.schemas.read().await;
    let mut list: Vec<_> = schemas
        .iter()
        .map(|(key, schema)| serde_json::json!({"ref":key,"schema":schema}))
        .collect();
    list.sort_by(|a, b| a["ref"].as_str().cmp(&b["ref"].as_str()));
    Json(serde_json::json!({"schemas":list}))
}
async fn get_schema(State(state): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    match state.schemas.read().await.get(&id) {
        Some(schema) => Json(serde_json::json!({"ref":id,"schema":schema})).into_response(),
        None => error(StatusCode::NOT_FOUND, "schema not found"),
    }
}
async fn delete_schema(State(state): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    let mut schemas = state.schemas.write().await;
    if id == "liveengine.default@1" {
        return error(StatusCode::CONFLICT, "built-in schema cannot be deleted");
    }
    if schema_used(&state, &id).await {
        return error(StatusCode::CONFLICT, "schema is in use");
    }
    if schemas.remove(&id).is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "schema not found")
    }
}

fn schema_used<'a>(
    state: &'a Inner,
    id: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
    Box::pin(async move {
        let records: Vec<_> = {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            hubs.values().cloned().collect()
        };
        for record in records {
            if record.info.schema_ref.as_deref() == Some(id) {
                return true;
            }
            if let Some(child) = record.child {
                if schema_used(&child, id).await {
                    return true;
                }
            }
        }
        false
    })
}

async fn export_registry(State(state): State<Arc<Inner>>) -> Json<Value> {
    let schemas = state.schemas.read().await;
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    let mut list: Vec<_> = hubs.values().map(public_info).collect();
    list.sort_by(|a, b| a.id.cmp(&b.id));
    let mut definitions: Vec<_> = schemas
        .iter()
        .map(|(key, schema)| serde_json::json!({"ref":key,"schema":schema}))
        .collect();
    definitions.sort_by(|a, b| a["ref"].as_str().cmp(&b["ref"].as_str()));
    Json(serde_json::json!({"protocol":HUB_PROTOCOL,"hubs":list,"schemas":definitions}))
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

fn valid_max_hubs(value: Option<&Value>, object: &Map<String, Value>, global: usize) -> bool {
    match value {
        None => true,
        Some(value) => {
            object.get("mode").and_then(Value::as_str) == Some("central")
                && value.as_u64().is_some_and(|n| n > 0 && n <= global as u64)
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RouteRequest {
    path: Vec<String>,
    op: String,
    #[serde(default)]
    body: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inherited_schemas: Option<[Option<Value>; 3]>,
}

async fn route_hub(
    State(mut state): State<Arc<Inner>>,
    mut headers: HeaderMap,
    payload: Result<Json<RouteRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(request)) = payload else {
        return error(StatusCode::BAD_REQUEST, "invalid route request");
    };
    let mut inherited = request.inherited_schemas.clone().unwrap_or_default();
    if inherited
        .iter()
        .flatten()
        .any(|schema| compile_schema(schema).is_err())
    {
        return error(StatusCode::BAD_REQUEST, "invalid inherited schema");
    }
    let mut depth = header_depth(&headers).unwrap_or(usize::MAX);
    for (index, segment) in request.path.iter().enumerate() {
        depth = depth.saturating_add(1);
        if depth > state.config.max_depth {
            return error(StatusCode::LOOP_DETECTED, "route depth exceeded");
        }
        let record = {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            hubs.get(segment).cloned()
        };
        let Some(record) = record else {
            return error(StatusCode::NOT_FOUND, "central hub not found");
        };
        if record.info.mode != HubMode::Central {
            return error(StatusCode::NOT_FOUND, "central hub not found");
        }
        inherited = merge_schemas(&state, &record.info, &inherited).await;
        if let Some(child) = record.child {
            state = child;
            continue;
        }
        let Some(address) = record.info.address.as_deref() else {
            return error(StatusCode::BAD_GATEWAY, "central address missing");
        };
        let forwarded = RouteRequest {
            path: request.path[index + 1..].to_vec(),
            op: request.op,
            body: request.body,
            inherited_schemas: Some(inherited),
        };
        return forward_route(
            &state,
            address,
            record.remote_token.as_deref().unwrap_or(""),
            &headers,
            depth,
            &forwarded,
        )
        .await;
    }
    if let Ok(value) = depth.to_string().parse() {
        headers.insert("x-liveengine-depth", value);
    }
    match request.op.as_str() {
        "tree" => {
            tree_response(
                state,
                headers,
                request
                    .body
                    .get("depth")
                    .and_then(Value::as_u64)
                    .unwrap_or(1) as usize,
                request
                    .body
                    .get("maxNodes")
                    .and_then(Value::as_u64)
                    .unwrap_or(u64::MAX) as usize,
            )
            .await
        }
        "register" => register_hub_inherited(state, Ok(Json(request.body)), inherited).await,
        "list" => list_hubs(State(state)).await.into_response(),
        "get" | "delete" => {
            let Some(id) = request.body.get("id").and_then(Value::as_str) else {
                return error(StatusCode::BAD_REQUEST, "missing hub id");
            };
            if request.op == "get" {
                get_hub(State(state), Path(id.to_string())).await
            } else {
                delete_hub(State(state), Path(id.to_string())).await
            }
        }
        "call" => {
            let Some(id) = request.body.get("id").and_then(Value::as_str) else {
                return error(StatusCode::BAD_REQUEST, "missing hub id");
            };
            let mut body = request.body.as_object().cloned().unwrap_or_default();
            let id = id.to_string();
            body.remove("id");
            match serde_json::from_value::<CallRequest>(Value::Object(body)) {
                Ok(call) => call_hub_inherited(state, id, headers, Ok(Json(call)), inherited).await,
                Err(_) => error(StatusCode::BAD_REQUEST, "invalid call"),
            }
        }
        _ => error(StatusCode::BAD_REQUEST, "invalid route operation"),
    }
}

fn valid_trace_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}

fn trace(state: &Inner, headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(value) = headers.get("x-liveengine-trace") else {
        return Ok(());
    };
    let value = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    if value.len() > 1040 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let ids: Vec<_> = value.split(',').collect();
    if ids.len() > state.config.max_depth || ids.iter().any(|id| !valid_trace_id(id)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut seen = std::collections::HashSet::new();
    if ids
        .iter()
        .any(|id| !seen.insert(*id) || *id == state.instance_id)
    {
        return Err(StatusCode::LOOP_DETECTED);
    }
    Ok(())
}
fn outgoing_trace(state: &Inner, headers: &HeaderMap) -> String {
    match headers
        .get("x-liveengine-trace")
        .and_then(|v| v.to_str().ok())
    {
        Some(value) => format!("{value},{}", state.instance_id),
        None => state.instance_id.clone(),
    }
}
fn header_depth(headers: &HeaderMap) -> Option<usize> {
    match headers.get("x-liveengine-depth") {
        None => Some(0),
        Some(v) => v.to_str().ok()?.parse::<usize>().ok().filter(|n| *n <= 16),
    }
}
async fn merge_schemas(
    state: &Inner,
    info: &HubInfo,
    inherited: &[Option<Value>; 3],
) -> [Option<Value>; 3] {
    let schemas = state.schemas.read().await;
    let explicit = info
        .schema_ref
        .as_ref()
        .and_then(|id| schemas.get(id))
        .or(info.scope_schema.as_ref())
        .cloned();
    let base = explicit
        .clone()
        .or_else(|| state.inherited[0].clone())
        .or_else(|| inherited[0].clone());
    let user = info
        .user_schema
        .clone()
        .or_else(|| explicit.clone())
        .or_else(|| state.inherited[1].clone())
        .or_else(|| inherited[1].clone())
        .or_else(|| base.clone());
    let server = info
        .server_schema
        .clone()
        .or(explicit)
        .or_else(|| state.inherited[2].clone())
        .or_else(|| inherited[2].clone())
        .or_else(|| base.clone());
    [base, user, server]
}
fn enforce_schemas(schemas: &[Option<Value>; 3], call: &CallRequest) -> Result<(), String> {
    if call.method == "generateUsers" && schemas.iter().any(Option::is_some) {
        return Err("generateUsers requires an unbound hub".into());
    }
    let schema = if call.method.contains("User") {
        &schemas[1]
    } else if call.method.contains("Server") {
        &schemas[2]
    } else {
        &schemas[0]
    };
    if let Some(schema) = schema {
        validate_call_scopes(schema, call)?;
    }
    Ok(())
}
async fn forward_route(
    state: &Inner,
    address: &str,
    token: &str,
    headers: &HeaderMap,
    depth: usize,
    request: &RouteRequest,
) -> Response {
    let _permit = match state.inflight_calls.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "hub call capacity is busy"),
    };
    let url =
        match validated_remote_url(state, address).and_then(|base| endpoint(&base, "v1/route")) {
            Ok(url) => url,
            Err(_) => return error(StatusCode::BAD_GATEWAY, "invalid remote address"),
        };
    let response = match state
        .client
        .post(url)
        .bearer_auth(token)
        .header("x-liveengine-trace", outgoing_trace(state, headers))
        .header("x-liveengine-depth", depth.to_string())
        .json(request)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "remote request failed"),
    };
    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return status.into_response();
    }
    if !status.is_success() {
        return error(status, "remote route failed");
    }
    match read_remote_json(response, state.config.max_response_bytes).await {
        Ok(value) => (status, Json(value)).into_response(),
        Err(_) => error(StatusCode::BAD_GATEWAY, "invalid remote response"),
    }
}
#[derive(Deserialize)]
struct TreeQuery {
    depth: Option<usize>,
}
async fn get_tree(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Query(query): Query<TreeQuery>,
) -> Response {
    tree_response(state, headers, query.depth.unwrap_or(1), usize::MAX).await
}
async fn tree_response(
    state: Arc<Inner>,
    headers: HeaderMap,
    depth: usize,
    max_nodes: usize,
) -> Response {
    let base = header_depth(&headers).unwrap_or(usize::MAX);
    if depth > state.config.max_depth || base.saturating_add(depth) > state.config.max_depth {
        return error(StatusCode::BAD_REQUEST, "tree depth exceeded");
    }
    let mut budget = state.global_max_hubs.min(1024).min(max_nodes);
    let deadline = Instant::now() + state.config.request_timeout / 2;
    let branches = tree_nodes(state.clone(), headers, depth, base, &mut budget, deadline).await;
    let value = serde_json::json!({"status":"ok","hubs":branches});
    if serde_json::to_vec(&value).map_or(true, |v| v.len() > state.config.max_response_bytes) {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "tree exceeds configured response size limit",
        );
    }
    Json(value).into_response()
}
fn tree_nodes<'a>(
    state: Arc<Inner>,
    headers: HeaderMap,
    depth: usize,
    base: usize,
    budget: &'a mut usize,
    deadline: Instant,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<Value>> + Send + 'a>> {
    Box::pin(async move {
        if depth == 0 || *budget == 0 {
            return vec![];
        }
        let mut records: Vec<_> = {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            hubs.values().cloned().collect()
        };
        records.sort_by(|a, b| a.info.id.cmp(&b.info.id));
        let mut result = Vec::new();
        let count = records.len();
        for (index, record) in records.into_iter().enumerate() {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            let mut branch = serde_json::json!({"hub":public_info(&record),"status":"ok"});
            if record.info.mode == HubMode::Central {
                if let Some(child) = record.child.filter(|_| depth > 1) {
                    branch["hubs"] = Value::Array(
                        tree_nodes(
                            child,
                            headers.clone(),
                            depth - 1,
                            base + 1,
                            budget,
                            deadline,
                        )
                        .await,
                    );
                } else if let Some(address) = record.info.address.as_deref() {
                    let req = RouteRequest {
                        path: vec![],
                        op: "tree".into(),
                        body: serde_json::json!({"depth":depth-1,"maxNodes":*budget}),
                        inherited_schemas: None,
                    };
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let response = tokio::time::timeout(
                        remaining / (count - index) as u32,
                        forward_route(
                            &state,
                            address,
                            record.remote_token.as_deref().unwrap_or(""),
                            &headers,
                            base + 1,
                            &req,
                        ),
                    )
                    .await;
                    if let Ok(response) = response {
                        if !response.status().is_success() {
                            branch["status"] = "error".into();
                            result.push(branch);
                            continue;
                        }
                        let bytes = axum::body::to_bytes(
                            response.into_body(),
                            state.config.max_response_bytes,
                        )
                        .await;
                        if let Ok(value) = bytes
                            .ok()
                            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                            .ok_or(())
                        {
                            let children = value
                                .get("hubs")
                                .and_then(Value::as_array)
                                .cloned()
                                .unwrap_or_default();
                            branch["hubs"] = Value::Array(truncate_tree(children, budget));
                        } else {
                            branch["status"] = "error".into();
                        }
                    } else {
                        branch["status"] = "error".into();
                    }
                }
            }
            result.push(branch);
        }
        result
    })
}
fn truncate_tree(nodes: Vec<Value>, budget: &mut usize) -> Vec<Value> {
    let mut result = Vec::new();
    for mut node in nodes {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        if let Some(children) = node.get_mut("hubs").and_then(Value::as_array_mut) {
            *children = truncate_tree(std::mem::take(children), budget);
        }
        result.push(node);
    }
    result
}

// Templates hold defaults, including write-only remote credentials. Read paths redact them.
fn public_template(name: &str, template: &Value) -> Value {
    let mut value = template.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("remoteToken");
        object.insert("name".into(), Value::String(name.into()));
    }
    value
}
async fn create_template(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(Json(Value::Object(mut object))) = payload else {
        return error(StatusCode::BAD_REQUEST, "invalid template");
    };
    let Some(Value::String(name)) = object.remove("name") else {
        return error(StatusCode::BAD_REQUEST, "invalid template name");
    };
    if schema_key(&name, "1").is_none() || name == "." || name == ".." {
        return error(StatusCode::BAD_REQUEST, "invalid template name");
    }
    if object.contains_key("id")
        || object.contains_key("template")
        || object.contains_key("ifAbsent")
    {
        return error(StatusCode::BAD_REQUEST, "invalid template fields");
    }
    let mut validation = object.clone();
    validation.insert("id".into(), Value::String("template-validation".into()));
    if validation
        .get("schemaRef")
        .is_some_and(|value| !value.is_string())
    {
        return error(StatusCode::BAD_REQUEST, "invalid schemaRef");
    }
    validation.remove("schemaRef");
    let max_hubs = validation.remove("maxHubs");
    if !valid_max_hubs(max_hubs.as_ref(), &validation, state.global_max_hubs) {
        return error(StatusCode::BAD_REQUEST, "invalid maxHubs");
    }
    let user_schema = validation.remove("userSchema");
    let server_schema = validation.remove("serverSchema");
    let Ok(reg) = serde_json::from_value::<HubRegistration>(Value::Object(validation)) else {
        return error(StatusCode::BAD_REQUEST, "invalid template");
    };
    if let Err(e) = validate_registration(&state, &reg) {
        return error(StatusCode::BAD_REQUEST, &e);
    }
    if reg
        .ttl_seconds
        .is_some_and(|s| s == 0 || Duration::from_secs(s) > state.config.max_ttl)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "ttlSeconds is outside configured bounds",
        );
    }
    for schema in [user_schema, server_schema].into_iter().flatten() {
        if let Err(e) = compile_schema(&schema) {
            return error(StatusCode::BAD_REQUEST, &e);
        }
    }
    let value = Value::Object(object);
    let mut templates = state.templates.write().await;
    if templates.contains_key(&name) {
        return error(StatusCode::CONFLICT, "template already exists");
    }
    if templates.len() >= state.config.max_hubs {
        return error(
            StatusCode::INSUFFICIENT_STORAGE,
            "template registry is full",
        );
    }
    let public = public_template(&name, &value);
    templates.insert(name, value);
    (StatusCode::CREATED, Json(public)).into_response()
}
async fn list_templates(State(state): State<Arc<Inner>>) -> Json<Value> {
    let templates = state.templates.read().await;
    let mut list: Vec<_> = templates
        .iter()
        .map(|(name, value)| public_template(name, value))
        .collect();
    list.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Json(serde_json::json!({"templates":list}))
}
async fn get_template(State(state): State<Arc<Inner>>, Path(name): Path<String>) -> Response {
    match state.templates.read().await.get(&name) {
        Some(value) => Json(public_template(&name, value)).into_response(),
        None => error(StatusCode::NOT_FOUND, "template not found"),
    }
}
async fn delete_template(State(state): State<Arc<Inner>>, Path(name): Path<String>) -> Response {
    if state.templates.write().await.remove(&name).is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "template not found")
    }
}

async fn register_hub(
    State(state): State<Arc<Inner>>,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> Response {
    register_hub_inherited(state, payload, [None, None, None]).await
}

async fn register_hub_inherited(
    state: Arc<Inner>,
    payload: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
    inherited: [Option<Value>; 3],
) -> Response {
    let Json(mut raw) = match payload {
        Ok(v) => v,
        Err(e) => {
            return error(
                StatusCode::BAD_REQUEST,
                &format!("invalid registration: {e}"),
            )
        }
    };
    let Some(object) = raw.as_object_mut() else {
        return error(StatusCode::BAD_REQUEST, "invalid registration");
    };
    let if_absent = match object.remove("ifAbsent") {
        None => false,
        Some(Value::Bool(value)) => value,
        _ => return error(StatusCode::BAD_REQUEST, "invalid ifAbsent"),
    };
    if if_absent {
        if let Some(id) = object.get("id").and_then(Value::as_str) {
            let mut hubs = state.hubs.write().await;
            expire_hubs(&mut hubs);
            if let Some(existing) = hubs.get(id) {
                return Json(public_info(existing)).into_response();
            }
        }
    }
    if let Some(template) = object.remove("template") {
        let Some(name) = template.as_str() else {
            return error(StatusCode::BAD_REQUEST, "invalid template");
        };
        let templates = state.templates.read().await;
        let Some(defaults) = templates.get(name).and_then(Value::as_object) else {
            return error(StatusCode::NOT_FOUND, "template not found");
        };
        for (key, value) in defaults {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    let max_hubs_value = object.remove("maxHubs");
    if !valid_max_hubs(max_hubs_value.as_ref(), object, state.global_max_hubs) {
        return error(StatusCode::BAD_REQUEST, "invalid maxHubs");
    }
    let max_hubs = max_hubs_value.and_then(|v| v.as_u64()).map(|v| v as usize);
    let schema_ref = match object.remove("schemaRef") {
        None => None,
        Some(Value::String(s)) => Some(s),
        _ => return error(StatusCode::BAD_REQUEST, "invalid schemaRef"),
    };
    let user_schema = object.remove("userSchema");
    let server_schema = object.remove("serverSchema");
    let reg: HubRegistration = match serde_json::from_value(raw) {
        Ok(v) => v,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid registration"),
    };
    if schema_ref.is_some() && reg.scope_schema.is_some() {
        return error(
            StatusCode::BAD_REQUEST,
            "scopeSchema and schemaRef are mutually exclusive",
        );
    }
    let schemas = state.schemas.read().await;
    let effective = match &schema_ref {
        Some(id) => match schemas.get(id) {
            Some(s) => Some(s.clone()),
            None => return error(StatusCode::BAD_REQUEST, "schema not found"),
        },
        None => reg
            .scope_schema
            .clone()
            .or_else(|| state.inherited[0].clone())
            .or_else(|| inherited[0].clone()),
    };
    if let Some(schema) = &effective {
        if let Err(e) = validate_scope_schema(&reg.scopes, schema) {
            return error(StatusCode::BAD_REQUEST, &e);
        }
    }
    for schema in [user_schema.as_ref(), server_schema.as_ref()]
        .into_iter()
        .flatten()
    {
        if let Err(e) = compile_schema(schema) {
            return error(StatusCode::BAD_REQUEST, &e);
        }
    }
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
        HubMode::Central => reg.address.clone(),
        HubMode::External => reg.address.clone(),
    };
    if let Some(address) = address.as_deref() {
        let remote_token = reg.remote_token.as_deref().unwrap_or("");
        if let Err(message) = remote_handshake(
            &state,
            address,
            if reg.mode == HubMode::Central {
                ""
            } else {
                &remote_id
            },
            remote_token,
        )
        .await
        {
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
        schema_ref,
        user_schema,
        server_schema,
        max_hubs,
        stale: false,
    };
    let child = if reg.mode == HubMode::Central && info.address.is_none() {
        let mut config = state.config.clone();
        config.max_hubs = max_hubs.unwrap_or(state.global_max_hubs);
        config.state_file = None;
        let service = match HubService::new(config) {
            Ok(service) => service,
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "central initialization failed",
                )
            }
        };
        let mut inner = Arc::try_unwrap(service.inner).ok().unwrap();
        inner.global_max_hubs = state.global_max_hubs;
        inner.hub_slots = state.hub_slots.clone();
        inner.templates = state.templates.clone();
        inner.schemas = state.schemas.clone();
        inner.inflight_calls = state.inflight_calls.clone();
        inner.root = false;
        inner.imports = state.imports.clone();
        inner.persistence_lock = state.persistence_lock.clone();
        inner.instance_id = state.instance_id.clone();
        inner.inherited = merge_schemas(&state, &info, &[None, None, None]).await;
        let child = Arc::new(inner);
        start_reaper(&child);
        Some(child)
    } else {
        None
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
        custom: false,
        child,
        _slot: None,
    };
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    if let Some(existing) = hubs.get(&id) {
        if if_absent {
            return Json(public_info(existing)).into_response();
        }
        return error(StatusCode::CONFLICT, "hub id already registered");
    }
    if hubs.len() >= state.config.max_hubs {
        return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
    }
    let slot = match state.hub_slots.clone().try_acquire_owned() {
        Ok(slot) => slot,
        Err(_) => {
            return error(
                StatusCode::INSUFFICIENT_STORAGE,
                "global hub budget is full",
            )
        }
    };
    let mut record = record;
    record._slot = Some(Arc::new(slot));
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
    if request.refresh_seconds.is_some_and(|s| s == 0 || s > 86400) {
        return error(StatusCode::BAD_REQUEST, "invalid refreshSeconds");
    }
    if request.refresh_seconds.is_some() {
        let imports = state.imports.read().await;
        if imports.len() >= state.config.max_hubs
            && !imports
                .iter()
                .any(|i| i.address == request.address && i.prefix == request.prefix)
        {
            return error(StatusCode::INSUFFICIENT_STORAGE, "import registry is full");
        }
    }
    let prefix = request.prefix.clone().unwrap_or_default();
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
    let mut imported_schemas = HashMap::new();
    if contract
        .get("endpoints")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|v| v.as_str() == Some("/v1/schemas")))
    {
        let url = match endpoint(&base, "v1/schemas") {
            Ok(url) => url,
            Err(e) => return error(StatusCode::BAD_REQUEST, &e),
        };
        let list = match remote_get(&state, url, &request.token).await {
            Ok(v) => v,
            Err(e) => return error(StatusCode::BAD_GATEWAY, &e),
        };
        let Some(entries) = list.get("schemas").and_then(Value::as_array) else {
            return error(StatusCode::BAD_GATEWAY, "invalid remote schemas");
        };
        if entries.len() > state.config.max_hubs {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "remote schema registry exceeds limit",
            );
        }
        for entry in entries {
            let Some(key) = entry.get("ref").and_then(Value::as_str) else {
                return error(StatusCode::BAD_GATEWAY, "invalid remote schema");
            };
            let Some((id, version)) = key.split_once('@') else {
                return error(StatusCode::BAD_GATEWAY, "invalid remote schema id");
            };
            let Some(schema) = entry.get("schema") else {
                return error(StatusCode::BAD_GATEWAY, "invalid remote schema");
            };
            if schema_key(id, version).as_deref() != Some(key)
                || compile_schema(schema).is_err()
                || imported_schemas
                    .insert(key.to_string(), schema.clone())
                    .is_some()
            {
                return error(StatusCode::BAD_GATEWAY, "invalid remote schema");
            }
        }
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
            schema_ref: remote
                .get("schemaRef")
                .and_then(Value::as_str)
                .map(str::to_owned),
            user_schema: remote.get("userSchema").cloned(),
            server_schema: remote.get("serverSchema").cloned(),
            max_hubs: None,
            stale: false,
        };
        if info
            .schema_ref
            .as_ref()
            .is_some_and(|id| !imported_schemas.contains_key(id))
            || [info.user_schema.as_ref(), info.server_schema.as_ref()]
                .into_iter()
                .flatten()
                .any(|schema| compile_schema(schema).is_err())
        {
            return error(StatusCode::BAD_GATEWAY, "invalid remote schema metadata");
        }
        if let Some(schema) = info
            .schema_ref
            .as_ref()
            .and_then(|id| imported_schemas.get(id))
        {
            if validate_scope_schema(&info.scopes, schema).is_err() {
                return error(StatusCode::BAD_GATEWAY, "invalid remote schema metadata");
            }
        }
        candidates.push(HubRecord {
            info,
            expires_at: None,
            handler: None,
            remote_token: Some(request.token.clone()),
            child: None,
            custom: false,
            _slot: None,
        });
    }
    let new_ids = {
        let mut hubs = state.hubs.write().await;
        expire_hubs(&mut hubs);
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
            } else {
                new_ids.push(candidate.info.id.clone());
            }
        }
        if hubs.len().saturating_add(new_ids.len()) > state.config.max_hubs {
            return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
        }
        new_ids
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
    let mut schemas = state.schemas.write().await;
    if imported_schemas
        .iter()
        .any(|(id, schema)| schemas.get(id).is_some_and(|existing| existing != schema))
    {
        return error(
            StatusCode::CONFLICT,
            "imported schema conflicts with existing schema",
        );
    }
    if schemas.len()
        + imported_schemas
            .keys()
            .filter(|id| !schemas.contains_key(*id))
            .count()
        > state.config.max_hubs
    {
        return error(StatusCode::INSUFFICIENT_STORAGE, "schema registry is full");
    }
    let mut hubs = state.hubs.write().await;
    expire_hubs(&mut hubs);
    if hubs.len().saturating_add(new_ids.len()) > state.config.max_hubs {
        return error(StatusCode::INSUFFICIENT_STORAGE, "hub registry is full");
    }
    if candidates
        .iter()
        .filter(|c| new_ids.contains(&c.info.id))
        .any(|entry| hubs.contains_key(&entry.info.id))
    {
        return error(
            StatusCode::CONFLICT,
            "import id was concurrently registered",
        );
    }
    let mut slots = Vec::new();
    for _ in &new_ids {
        match state.hub_slots.clone().try_acquire_owned() {
            Ok(slot) => slots.push(Arc::new(slot)),
            Err(_) => {
                return error(
                    StatusCode::INSUFFICIENT_STORAGE,
                    "global hub budget is full",
                )
            }
        }
    }
    schemas.extend(imported_schemas);
    let mut imported = Vec::new();
    for mut entry in candidates {
        if !new_ids.contains(&entry.info.id) {
            if let Some(existing) = hubs.get_mut(&entry.info.id) {
                existing.info = entry.info.clone();
                imported.push(entry.info);
                continue;
            }
            return error(StatusCode::CONFLICT, "import target changed concurrently");
        }
        if hubs.contains_key(&entry.info.id) {
            return error(
                StatusCode::CONFLICT,
                "import id was concurrently registered",
            );
        }
        entry._slot = slots.pop();
        imported.push(entry.info.clone());
        hubs.insert(entry.info.id.clone(), entry);
    }
    for candidate in &mut imported {
        candidate.stale = false;
    }
    for candidate in &imported {
        if let Some(entry) = hubs.get_mut(&candidate.id) {
            entry.info.stale = false;
        }
    }
    for entry in hubs.values_mut() {
        if entry.info.address.as_deref() == Some(&request.address)
            && entry.remote_token.as_deref() == Some(&request.token)
            && entry.info.id == format!("{}{}", prefix, entry.info.remote_hub_id)
        {
            entry.info.stale = !discovered_ids.contains(&entry.info.id);
        }
    }
    drop(hubs);
    drop(schemas);
    if request.refresh_seconds.is_some() {
        let mut imports = state.imports.write().await;
        imports.retain(|i| !(i.address == request.address && i.prefix == request.prefix));
        if imports.len() >= state.config.max_hubs {
            return error(StatusCode::INSUFFICIENT_STORAGE, "import registry is full");
        }
        imports.push(request);
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
    call_hub_inherited(state, id, headers, payload, [None, None, None]).await
}

async fn call_hub_inherited(
    state: Arc<Inner>,
    id: String,
    headers: HeaderMap,
    payload: Result<Json<CallRequest>, axum::extract::rejection::JsonRejection>,
    inherited: [Option<Value>; 3],
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
    if record.custom && record.handler.is_none() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "custom hub handler is unavailable",
        );
    }
    let schemas = merge_schemas(&state, &record.info, &inherited).await;
    if let Err(message) = enforce_schemas(&schemas, &call) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
    let permit = match state.inflight_calls.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "hub call capacity is busy"),
    };
    let depth = header_depth(&headers).unwrap_or(usize::MAX);
    if record.handler.is_none() && depth >= state.config.max_depth {
        return error(StatusCode::LOOP_DETECTED, "forwarding depth exceeded");
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
            &headers,
            depth + 1,
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
        Err(message) if message == "remote: remote returned HTTP 508 Loop Detected" => {
            error(StatusCode::LOOP_DETECTED, "remote forwarding loop")
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
    if remote_id.is_empty() {
        return Ok(());
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

/// Native round-trip latency samples for a hub's `/v1/ping` endpoint.
///
/// The measurement is performed by the Rust process, so callers do not need a
/// JavaScript runtime. The endpoint is unauthenticated and returns only timing
/// metadata.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PingStats {
    pub samples: Vec<f64>,
    pub min_ms: f64,
    pub max_ms: f64,
    pub avg_ms: f64,
    pub jitter_ms: f64,
    pub server_time_ms: Option<u64>,
}

pub async fn ping_hub(
    address: &str,
    samples: usize,
    timeout: Duration,
) -> Result<PingStats, String> {
    if !(1..=32).contains(&samples) {
        return Err("samples must be between 1 and 32".into());
    }
    if timeout.is_zero() {
        return Err("timeout must be greater than zero".into());
    }
    let base =
        Url::parse(address).map_err(|_| "address must be an absolute HTTP(S) URL".to_string())?;
    if !matches!(base.scheme(), "http" | "https")
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err("address must be a plain HTTP(S) base URL".into());
    }
    let url = endpoint(&base, "v1/ping")?;
    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(timeout)
        .build()
        .map_err(|e| format!("could not create HTTP client: {e}"))?;
    let mut rtts = Vec::with_capacity(samples);
    let mut server_time_ms = None;
    for _ in 0..samples {
        let started = Instant::now();
        let response = client
            .get(url.clone())
            .send()
            .await
            .map_err(|e| format!("ping request failed: {e}"))?;
        let value = read_remote_json(response, 64 * 1024)
            .await
            .map_err(|e| e.trim_start_matches("remote: ").to_string())?;
        if value.get("protocol").and_then(Value::as_str) != Some(HUB_PROTOCOL)
            || value.get("pong").and_then(Value::as_bool) != Some(true)
        {
            return Err("incompatible ping response".into());
        }
        server_time_ms = value.get("serverTimeMs").and_then(Value::as_u64);
        rtts.push(round_ms(started.elapsed().as_secs_f64() * 1000.0));
    }
    let min_ms = rtts.iter().copied().fold(f64::INFINITY, f64::min);
    let max_ms = rtts.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let avg_ms = rtts.iter().sum::<f64>() / rtts.len() as f64;
    let jitter_ms = if rtts.len() < 2 {
        0.0
    } else {
        rtts.windows(2)
            .map(|pair| (pair[1] - pair[0]).abs())
            .sum::<f64>()
            / (rtts.len() - 1) as f64
    };
    Ok(PingStats {
        samples: rtts,
        min_ms: round_ms(min_ms),
        max_ms: round_ms(max_ms),
        avg_ms: round_ms(avg_ms),
        jitter_ms: round_ms(jitter_ms),
        server_time_ms,
    })
}

fn round_ms(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
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
    headers: &HeaderMap,
    depth: usize,
    call: &CallRequest,
) -> Result<Value, String> {
    let base = validated_remote_url(state, address).map_err(|e| format!("remote: {e}"))?;
    let url = endpoint(&base, &format!("v1/hubs/{remote_id}/call"))
        .map_err(|e| format!("remote: {e}"))?;
    let response = state
        .client
        .post(url)
        .bearer_auth(token)
        .header("x-liveengine-trace", outgoing_trace(state, headers))
        .header("x-liveengine-depth", depth.to_string())
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
        HubMode::Central
            if r.address.is_none() && (r.remote_token.is_some() || r.remote_hub_id.is_some()) =>
        {
            return Err("local central cannot set remote credentials or id".into())
        }
        HubMode::Central
            if r.address.is_some()
                && (r.remote_token.as_ref().is_none_or(|t| t.len() < 24)
                    || r.remote_hub_id.is_some()) =>
        {
            return Err("remote central requires remoteToken and no remoteHubId".into())
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

// Reject non-fragment references before compilation, including in nested resources.
fn compile_schema(schema: &Value) -> Result<jsonschema::Validator, String> {
    if serde_json::to_vec(schema).map_or(true, |v| v.len() > 65536) || json_depth(schema, 0) > 32 {
        return Err("schema exceeds size or depth limit".into());
    }
    fn inspect(v: &Value, count: &mut usize) -> bool {
        *count += 1;
        if *count > 2048 {
            return false;
        }
        match v {
            Value::Object(map) => map.iter().all(|(k, v)| {
                if matches!(k.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef")
                    && !v.as_str().is_some_and(|s| s.starts_with('#'))
                {
                    return false;
                }
                if k == "$id" {
                    return false;
                }
                if k == "pattern" && v.as_str().is_some_and(|s| s.len() > 512) {
                    return false;
                }
                inspect(v, count)
            }),
            Value::Array(a) => a.iter().all(|v| inspect(v, count)),
            _ => true,
        }
    }
    if !inspect(schema, &mut 0) {
        return Err("schema contains forbidden references or exceeds complexity limits".into());
    }
    // Cap reference expansion and reject cycles before the validator can recurse.
    fn references_safe(
        root: &Value,
        value: &Value,
        active: &mut Vec<String>,
        budget: &mut usize,
    ) -> bool {
        if *budget == 0 {
            return false;
        }
        *budget -= 1;
        match value {
            Value::Object(map) => map.iter().all(|(key, value)| {
                if matches!(key.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef") {
                    let Some(reference) = value.as_str() else {
                        return false;
                    };
                    let Some(pointer) = reference.strip_prefix('#') else {
                        return false;
                    };
                    // Restrict local references to JSON pointers so that cycle checks are unambiguous.
                    if !pointer.is_empty() && !pointer.starts_with('/') {
                        return false;
                    }
                    if active.iter().any(|entry| entry == pointer) {
                        return false;
                    }
                    let Some(target) = root.pointer(pointer) else {
                        return false;
                    };
                    active.push(pointer.to_string());
                    let ok = references_safe(root, target, active, budget);
                    active.pop();
                    ok
                } else {
                    references_safe(root, value, active, budget)
                }
            }),
            Value::Array(items) => items
                .iter()
                .all(|item| references_safe(root, item, active, budget)),
            _ => true,
        }
    }
    if !references_safe(schema, schema, &mut vec![String::new()], &mut 8192) {
        return Err("schema references are cyclic, unsupported, or too complex".into());
    }
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(schema)
        .map_err(|_| "invalid JSON schema".into())
}
fn validate_scope_schema(scopes: &Value, schema: &Value) -> Result<(), String> {
    if !scopes.is_object() {
        return Err("scopes must be an object".into());
    }
    let validator = compile_schema(schema)?;
    if !validator.is_valid(scopes) {
        return Err("scopes do not match schema".into());
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
    let validator = compile_schema(schema)?;
    for item in items {
        let scopes = item.get("scopes").cloned().unwrap_or_else(empty_object);
        if !scopes.is_object() || !validator.is_valid(&scopes) {
            return Err("scopes do not match schema".into());
        }
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
