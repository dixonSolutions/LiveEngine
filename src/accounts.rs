//! Local accounts and bearer sessions for the hub service.
use crate::hub::{error, Inner};
use axum::{
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ring::{
    digest, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Account {
    pub(crate) username: String,
    pub(crate) role: String,
    pub(crate) enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) claims: Option<Value>,
    created_at: u64,
}
// Snapshot serialization must include the password hash, while public responses never do.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoredAccount {
    username: String,
    role: String,
    enabled: bool,
    claims: Option<Value>,
    created_at: u64,
    password_hash: String,
}
impl From<&StoredAccount> for Account {
    fn from(a: &StoredAccount) -> Self {
        Self {
            username: a.username.clone(),
            role: a.role.clone(),
            enabled: a.enabled,
            claims: a.claims.clone(),
            created_at: a.created_at,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Session {
    username: String,
    expires_at: u64,
    created_at: u64,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Data {
    accounts: HashMap<String, StoredAccount>,
    sessions: HashMap<String, Session>,
}
#[derive(Default)]
struct Failure {
    count: u32,
    first: u64,
    until: u64,
}
pub(crate) struct Accounts {
    pub data: RwLock<Data>,
    failures: std::sync::Mutex<HashMap<String, Failure>>,
    iterations: u32,
    ttl: u64,
    max_failures: u32,
}
fn setting(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .ok()
            .filter(|n| *n >= min && *n <= max)
            .ok_or_else(|| format!("invalid {name}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(format!("invalid {name}")),
    }
}
impl Accounts {
    pub fn new(data: Data) -> Result<Self, String> {
        Ok(Self {
            data: RwLock::new(data),
            failures: Default::default(),
            iterations: setting("LIVEENGINE_PASSWORD_ITERATIONS", 310000, 100000, 10000000)? as u32,
            ttl: setting("LIVEENGINE_SESSION_TTL_SECONDS", 43200, 1, 31536000)?,
            max_failures: setting("LIVEENGINE_LOGIN_MAX_FAILURES", 5, 1, 1000)? as u32,
        })
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn valid_name(s: &str) -> bool {
    (3..=64).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}
fn valid_password(s: &str) -> bool {
    (10..=1024).contains(&s.len())
}
fn hash_token(token: &str) -> String {
    URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, token.as_bytes()).as_ref())
}
fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}
fn random(len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    SystemRandom::new()
        .fill(&mut bytes)
        .expect("system random unavailable");
    bytes
}
async fn make_hash(password: String, iterations: u32) -> Result<String, ()> {
    tokio::task::spawn_blocking(move || {
        let salt = random(16);
        let mut out = [0; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).unwrap(),
            &salt,
            password.as_bytes(),
            &mut out,
        );
        format!(
            "pbkdf2-sha256$v1${iterations}${}${}",
            URL_SAFE_NO_PAD.encode(salt),
            URL_SAFE_NO_PAD.encode(out)
        )
    })
    .await
    .map_err(|_| ())
}
async fn verify(password: String, encoded: Option<String>, iterations: u32) -> bool {
    tokio::task::spawn_blocking(move || {
        let dummy_salt = [0u8; 16];
        let dummy_hash = [0u8; 32];
        let parsed = encoded.as_ref().and_then(|s| {
            let p: Vec<_> = s.split('$').collect();
            if p.len() != 5 || p[0] != "pbkdf2-sha256" || p[1] != "v1" {
                return None;
            }
            let n = p[2]
                .parse::<u32>()
                .ok()
                .filter(|n| (100000..=10000000).contains(n))?;
            let salt = URL_SAFE_NO_PAD.decode(p[3]).ok()?;
            let hash = URL_SAFE_NO_PAD.decode(p[4]).ok()?;
            if salt.len() != 16 || hash.len() != 32 {
                return None;
            }
            Some((n, salt, hash))
        });
        let (n, salt, hash) =
            parsed.unwrap_or((iterations, dummy_salt.to_vec(), dummy_hash.to_vec()));
        pbkdf2::verify(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(n).unwrap(),
            &salt,
            password.as_bytes(),
            &hash,
        )
        .is_ok()
            && encoded.is_some()
    })
    .await
    .unwrap_or(false)
}
pub(crate) async fn session(state: &Inner, token: &str) -> Option<Account> {
    if token.is_empty() {
        return None;
    }
    let data = state.accounts.data.read().await;
    let s = data.sessions.get(&hash_token(token))?;
    if s.expires_at <= now() {
        return None;
    }
    let a = data.accounts.get(&s.username)?;
    if !a.enabled {
        return None;
    }
    Some(a.into())
}
pub(crate) async fn account_enabled(state: &Inner, username: &str) -> bool {
    state
        .accounts
        .data
        .read()
        .await
        .accounts
        .get(&username.to_ascii_lowercase())
        .is_some_and(|a| a.enabled)
}
pub(crate) async fn is_admin(state: &Inner, token: &str) -> bool {
    session(state, token)
        .await
        .is_some_and(|a| a.role == "admin")
}
pub(crate) async fn allowed(
    state: &Inner,
    token: &str,
    method: &axum::http::Method,
    path: &str,
) -> bool {
    let Some(a) = session(state, token).await else {
        return false;
    };
    if a.role == "admin" {
        return true;
    }
    (path == "/v1/sessions/me" && *method == axum::http::Method::GET)
        || (path == "/v1/sessions" && *method == axum::http::Method::DELETE)
        || (path == "/v1/accounts/me/password" && *method == axum::http::Method::POST)
        || (path
            .strip_prefix("/v1/accounts/")
            .is_some_and(|n| n.eq_ignore_ascii_case(&a.username))
            && *method == axum::http::Method::GET)
}
fn revoke(data: &mut Data, name: &str) {
    data.sessions.retain(|_, s| s.username != name);
}
fn bad() -> Response {
    error(StatusCode::BAD_REQUEST, "invalid account")
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Create {
    username: String,
    password: String,
    role: String,
    #[serde(default = "yes")]
    enabled: bool,
    claims: Option<Value>,
}
fn yes() -> bool {
    true
}
pub(crate) async fn create(State(s): State<Arc<Inner>>, Json(v): Json<Create>) -> Response {
    if !valid_name(&v.username)
        || !valid_password(&v.password)
        || !matches!(v.role.as_str(), "admin" | "user")
        || v.claims.as_ref().is_some_and(|v| !v.is_object())
    {
        return bad();
    }
    let key = v.username.to_ascii_lowercase();
    let Ok(hash) = make_hash(v.password, s.accounts.iterations).await else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "hashing failed");
    };
    let mut d = s.accounts.data.write().await;
    if d.accounts.contains_key(&key) {
        return error(StatusCode::CONFLICT, "account already exists");
    }
    if d.accounts.len() >= 10000 {
        return error(StatusCode::SERVICE_UNAVAILABLE, "account limit reached");
    }
    let a = StoredAccount {
        username: v.username,
        role: v.role,
        enabled: v.enabled,
        claims: v.claims,
        created_at: now(),
        password_hash: hash,
    };
    let public = Account::from(&a);
    d.accounts.insert(key, a);
    (StatusCode::CREATED, Json(public)).into_response()
}
pub(crate) async fn list(State(s): State<Arc<Inner>>) -> Response {
    let d = s.accounts.data.read().await;
    let mut a: Vec<_> = d.accounts.values().map(Account::from).collect();
    a.sort_by_key(|a| a.username.to_ascii_lowercase());
    Json(a).into_response()
}
pub(crate) async fn get(State(s): State<Arc<Inner>>, Path(name): Path<String>) -> Response {
    let d = s.accounts.data.read().await;
    match d.accounts.get(&name.to_ascii_lowercase()) {
        Some(a) => Json(Account::from(a)).into_response(),
        None => error(StatusCode::NOT_FOUND, "account not found"),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Patch {
    role: Option<String>,
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "nullable_claims")]
    claims: Option<Option<Value>>,
    password: Option<String>,
}
fn nullable_claims<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<Value>>, D::Error> {
    Option::<Value>::deserialize(deserializer).map(Some)
}
pub(crate) async fn patch(
    State(s): State<Arc<Inner>>,
    Path(name): Path<String>,
    Json(v): Json<Patch>,
) -> Response {
    if v.role
        .as_ref()
        .is_some_and(|r| !matches!(r.as_str(), "admin" | "user"))
        || v.password.as_ref().is_some_and(|p| !valid_password(p))
        || v.claims
            .as_ref()
            .is_some_and(|c| c.as_ref().is_some_and(|c| !c.is_object()))
    {
        return bad();
    }
    let hash = if let Some(p) = v.password {
        match make_hash(p, s.accounts.iterations).await {
            Ok(h) => Some(h),
            Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "hashing failed"),
        }
    } else {
        None
    };
    let key = name.to_ascii_lowercase();
    let mut d = s.accounts.data.write().await;
    let Some(a) = d.accounts.get_mut(&key) else {
        return error(StatusCode::NOT_FOUND, "account not found");
    };
    let mut end = hash.is_some();
    if let Some(h) = hash {
        a.password_hash = h;
    }
    if let Some(role) = v.role {
        end |= a.role == "admin" && role == "user";
        a.role = role;
    }
    if let Some(enabled) = v.enabled {
        end |= !enabled;
        a.enabled = enabled;
    }
    if let Some(claims) = v.claims {
        a.claims = claims;
    }
    let public = Account::from(&*a);
    if end {
        revoke(&mut d, &key);
    }
    let mut response = Json(public).into_response();
    if v.enabled == Some(false) {
        response.extensions_mut().insert(BanAccount(key));
    }
    response
}
#[derive(Clone)]
pub(crate) struct BanAccount(pub(crate) String);
pub(crate) async fn delete(State(s): State<Arc<Inner>>, Path(name): Path<String>) -> Response {
    let key = name.to_ascii_lowercase();
    let mut d = s.accounts.data.write().await;
    if d.accounts.remove(&key).is_none() {
        return error(StatusCode::NOT_FOUND, "account not found");
    }
    revoke(&mut d, &key);
    StatusCode::NO_CONTENT.into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Login {
    username: String,
    password: String,
}
pub(crate) async fn login(
    State(s): State<Arc<Inner>>,
    peer: Option<axum::Extension<ConnectInfo<std::net::SocketAddr>>>,
    Json(v): Json<Login>,
) -> Response {
    let key = v.username.to_ascii_lowercase();
    let peer = peer
        .map(|p| p.0 .0.ip().to_string())
        .unwrap_or_else(|| "unknown".into());
    let keys = [format!("u:{}", hash_token(&key)), format!("p:{peer}")];
    let time = now();
    let blocked = {
        let mut f = s
            .accounts
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        f.retain(|_, v| time.saturating_sub(v.first) < 900 || v.until > time);
        f.len()
            .saturating_add(keys.iter().filter(|k| !f.contains_key(*k)).count())
            > 8192
            || keys
                .iter()
                .any(|k| f.get(k).is_some_and(|v| v.until > time))
    };
    let saved = { s.accounts.data.read().await.accounts.get(&key).cloned() };
    let valid = verify(
        v.password,
        saved.as_ref().map(|a| a.password_hash.clone()),
        s.accounts.iterations,
    )
    .await
        && saved.as_ref().is_some_and(|a| a.enabled)
        && !blocked;
    if !valid {
        let time = now();
        let mut f = s
            .accounts
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for k in keys {
            if f.len() >= 8192 && !f.contains_key(&k) {
                continue;
            }
            let e = f.entry(k).or_insert(Failure {
                count: 0,
                first: time,
                until: 0,
            });
            e.count = e.count.saturating_add(1);
            if e.count >= s.accounts.max_failures {
                e.until =
                    time.saturating_add(1 + (1u64 << (e.count - s.accounts.max_failures).min(10)));
            }
        }
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    }
    let token = URL_SAFE_NO_PAD.encode(random(32));
    let mut d = s.accounts.data.write().await;
    let Some(a) = d.accounts.get(&key) else {
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    };
    if !a.enabled
        || saved
            .as_ref()
            .is_none_or(|old| old.password_hash != a.password_hash)
    {
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    }
    let account = Account::from(a);
    let expiry = time.saturating_add(s.accounts.ttl);
    d.sessions.retain(|_, v| v.expires_at > time);
    while d.sessions.len() >= 10000
        || d.sessions.values().filter(|v| v.username == key).count() >= 16
    {
        let own = d.sessions.values().filter(|v| v.username == key).count() >= 16;
        if let Some(old) = d
            .sessions
            .iter()
            .filter(|(_, v)| !own || v.username == key)
            .min_by_key(|(_, v)| v.created_at)
            .map(|(k, _)| k.clone())
        {
            d.sessions.remove(&old);
        } else {
            break;
        }
    }
    d.sessions.insert(
        hash_token(&token),
        Session {
            username: key,
            expires_at: expiry,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        },
    );
    Json(json!({"token":token,"account":account,"expiresAt":expiry})).into_response()
}
pub(crate) async fn logout(State(s): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    s.accounts
        .data
        .write()
        .await
        .sessions
        .remove(&hash_token(bearer(&headers)));
    StatusCode::NO_CONTENT.into_response()
}
pub(crate) async fn me(State(s): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    match session(&s, bearer(&headers)).await {
        Some(a) => Json(a).into_response(),
        None => error(StatusCode::UNAUTHORIZED, "unauthorized"),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Password {
    current: String,
    next: String,
}
pub(crate) async fn change_password(
    State(s): State<Arc<Inner>>,
    headers: HeaderMap,
    Json(v): Json<Password>,
) -> Response {
    if !valid_password(&v.next) {
        return bad();
    }
    let Some(a) = session(&s, bearer(&headers)).await else {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    let key = a.username.to_ascii_lowercase();
    let old = {
        s.accounts
            .data
            .read()
            .await
            .accounts
            .get(&key)
            .map(|a| a.password_hash.clone())
    };
    if !verify(v.current, old.clone(), s.accounts.iterations).await {
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    }
    let Ok(hash) = make_hash(v.next, s.accounts.iterations).await else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "hashing failed");
    };
    let mut d = s.accounts.data.write().await;
    let Some(a) = d.accounts.get_mut(&key) else {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    if Some(&a.password_hash) != old.as_ref() || !a.enabled {
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    a.password_hash = hash;
    revoke(&mut d, &key);
    StatusCode::NO_CONTENT.into_response()
}
