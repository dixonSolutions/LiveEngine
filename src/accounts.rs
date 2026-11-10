//! Local accounts and bearer sessions for the hub service.
use crate::hub::{error, Inner};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
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
    #[serde(default)]
    generation: u64,
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
    #[cfg(test)]
    verifications: std::sync::atomic::AtomicUsize,
}
impl Accounts {
    /// `limits` were validated with the rest of the `HubConfig`.
    pub fn new(data: Data, limits: &crate::hub::AccountLimits) -> Self {
        Self {
            data: RwLock::new(data),
            failures: Default::default(),
            #[cfg(test)]
            verifications: Default::default(),
            iterations: limits.password_iterations,
            ttl: limits.session_ttl.as_secs(),
            max_failures: limits.login_max_failures,
        }
    }
    /// Most attempts one key may hold, in flight or failed, within the window.
    ///
    /// * A username from one address (`a:`) gets `max_failures`, so wrong passwords sent from
    ///   one address lock that username out for that address only.
    /// * An address (`p:`) gets ten times that, across every username, because many people share
    ///   one (a hub API in front of the service, a NAT) and sign in together; their attempts
    ///   reserve budget while they hash, so a tight address budget would refuse honest,
    ///   concurrent logins.
    /// * A username from all addresses together (`u:`) gets a hundred times that: a backstop
    ///   against guessing spread over many addresses, high enough that one address cannot reach it.
    fn failure_limit(&self, key: &str) -> u32 {
        if key.starts_with("p:") {
            self.max_failures.saturating_mul(10)
        } else if key.starts_with("u:") {
            self.max_failures.saturating_mul(100)
        } else {
            self.max_failures
        }
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
fn hash_cost(hash: &str) -> Option<u32> {
    let mut parts = hash.split('$');
    if parts.next()? != "pbkdf2-sha256" || parts.next()? != "v1" {
        return None;
    }
    parts
        .next()?
        .parse::<u32>()
        .ok()
        .filter(|n| (100000..=10000000).contains(n))
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
pub(crate) async fn credential_state(state: &Inner, username: &str) -> Option<(u64, bool, String)> {
    let data = state.accounts.data.read().await;
    let account = data.accounts.get(&username.to_ascii_lowercase())?;
    Some((account.generation, account.enabled, account.role.clone()))
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
/// A body the JSON layer refused (not JSON, a missing or unknown field, too large) answers with
/// the same `{ "error": ... }` shape as every other refusal, keeping the layer's status.
fn rejected(rejection: JsonRejection, message: &str) -> Response {
    let status = rejection.status();
    let status = if status.is_client_error() {
        status
    } else {
        StatusCode::BAD_REQUEST
    };
    error(status, message)
}
fn locked(retry_after: u64) -> Response {
    let mut response = error(StatusCode::TOO_MANY_REQUESTS, "too many sign-in attempts");
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry_after.max(1)));
    response
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
pub(crate) async fn create(
    State(s): State<Arc<Inner>>,
    payload: Result<Json<Create>, JsonRejection>,
) -> Response {
    let v = match payload {
        Ok(Json(v)) => v,
        Err(e) => return rejected(e, "invalid account"),
    };
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
        generation: u64::from_le_bytes(random(8).try_into().unwrap()),
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
    payload: Result<Json<Patch>, JsonRejection>,
) -> Response {
    let v = match payload {
        Ok(Json(v)) => v,
        Err(e) => return rejected(e, "invalid account"),
    };
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
        end |= a.role != role;
        a.role = role;
    }
    if let Some(enabled) = v.enabled {
        end |= a.enabled && !enabled;
        a.enabled = enabled;
    }
    if let Some(claims) = v.claims {
        a.claims = claims;
    }
    if end {
        a.generation = a.generation.wrapping_add(1);
    }
    let public = Account::from(&*a);
    if end {
        revoke(&mut d, &key);
    }
    let mut response = Json(public).into_response();
    if end {
        let code = if v.enabled == Some(false) {
            "banned"
        } else {
            "credentials-changed"
        };
        response.extensions_mut().insert(AccountChanged(key, code));
    }
    response
}
#[derive(Clone)]
pub(crate) struct AccountChanged(pub(crate) String, pub(crate) &'static str);
pub(crate) async fn delete(State(s): State<Arc<Inner>>, Path(name): Path<String>) -> Response {
    let key = name.to_ascii_lowercase();
    let mut d = s.accounts.data.write().await;
    if d.accounts.remove(&key).is_none() {
        return error(StatusCode::NOT_FOUND, "account not found");
    }
    revoke(&mut d, &key);
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .extensions_mut()
        .insert(AccountChanged(key, "account-deleted"));
    response
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
    payload: Result<Json<Login>, JsonRejection>,
) -> Response {
    let v = match payload {
        Ok(Json(v)) => v,
        Err(e) => return rejected(e, "invalid sign-in request"),
    };
    let key = v.username.to_ascii_lowercase();
    let peer = peer
        .map(|p| p.0 .0.ip().to_string())
        .unwrap_or_else(|| "unknown".into());
    let user = hash_token(&key);
    // The username from this address, this address for every username, and the username from
    // every address: see `failure_limit`.
    let keys = [
        format!("a:{user}:{peer}"),
        format!("p:{peer}"),
        format!("u:{user}"),
    ];
    let time = now();
    // Reserve every budget before starting expensive work, including work already in flight.
    {
        let mut failures = s
            .accounts
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        failures.retain(|_, v| time.saturating_sub(v.first) < 900 || v.until > time);
        if let Some(until) = keys
            .iter()
            .filter_map(|k| failures.get(k).map(|v| v.until))
            .filter(|until| *until > time)
            .max()
        {
            return locked(until - time);
        }
        for key in &keys {
            if !failures.contains_key(key) && failures.len() >= 8192 {
                if let Some(oldest) = failures
                    .iter()
                    .filter(|(k, _)| !keys.contains(k))
                    .min_by_key(|(_, v)| v.first)
                    .map(|(k, _)| k.clone())
                {
                    failures.remove(&oldest);
                }
            }
            let entry = failures.entry(key.clone()).or_insert(Failure {
                count: 0,
                first: time,
                until: 0,
            });
            entry.count = entry.count.saturating_add(1);
            let limit = s.accounts.failure_limit(key);
            if entry.count >= limit {
                entry.until = time.saturating_add(1 + (1u64 << (entry.count - limit).min(10)));
            }
        }
    }
    let (saved, dummy_cost) = {
        let data = s.accounts.data.read().await;
        let mut costs = HashMap::<u32, usize>::new();
        for account in data.accounts.values() {
            if let Some(cost) = hash_cost(&account.password_hash) {
                *costs.entry(cost).or_default() += 1;
            }
        }
        (
            data.accounts.get(&key).cloned(),
            costs
                .into_iter()
                .max_by_key(|(cost, count)| (*count, *cost))
                .map(|(cost, _)| cost)
                .unwrap_or(s.accounts.iterations),
        )
    };
    #[cfg(test)]
    s.accounts
        .verifications
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let valid = verify(
        v.password.clone(),
        saved.as_ref().map(|a| a.password_hash.clone()),
        dummy_cost,
    )
    .await
        && saved.as_ref().is_some_and(|a| a.enabled);
    if !valid {
        // A lock runs its full length from the failure that set it, not from when the attempt
        // was reserved: the hashing in between would otherwise use part of it up.
        let time = now();
        let mut failures = s
            .accounts
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for key in &keys {
            let limit = s.accounts.failure_limit(key);
            if let Some(entry) = failures.get_mut(key) {
                if entry.count >= limit {
                    let until = time.saturating_add(1 + (1u64 << (entry.count - limit).min(10)));
                    entry.until = entry.until.max(until);
                }
            }
        }
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    }
    let replacement = if saved.as_ref().and_then(|a| hash_cost(&a.password_hash))
        != Some(s.accounts.iterations)
    {
        match make_hash(v.password, s.accounts.iterations).await {
            Ok(hash) => Some(hash),
            Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "hashing failed"),
        }
    } else {
        None
    };
    let token = URL_SAFE_NO_PAD.encode(random(32));
    let mut d = s.accounts.data.write().await;
    let Some(a) = d.accounts.get_mut(&key) else {
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    };
    if !a.enabled
        || saved.as_ref().is_none_or(|old| {
            old.password_hash != a.password_hash || old.generation != a.generation
        })
    {
        return error(StatusCode::UNAUTHORIZED, "invalid username or password");
    }
    if let Some(hash) = replacement {
        a.password_hash = hash;
    }
    let account = Account::from(&*a);
    {
        let mut failures = s
            .accounts
            .failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for key in &keys {
            if let Some(entry) = failures.get_mut(key) {
                entry.count = entry.count.saturating_sub(1);
                if entry.count < s.accounts.failure_limit(key) {
                    entry.until = 0;
                }
            }
        }
    }
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
    payload: Result<Json<Password>, JsonRejection>,
) -> Response {
    let v = match payload {
        Ok(Json(v)) => v,
        Err(e) => return rejected(e, "invalid account"),
    };
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
    a.generation = a.generation.wrapping_add(1);
    revoke(&mut d, &key);
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .extensions_mut()
        .insert(AccountChanged(key, "credentials-changed"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{HubConfig, HubService};

    async fn setup() -> Arc<Inner> {
        let service = HubService::new(HubConfig {
            token: "test-administrator-token-0123456789".into(),
            ..Default::default()
        })
        .unwrap();
        let state = service.inner;
        let result = create(
            State(state.clone()),
            Ok(Json(Create {
                username: "alice".into(),
                password: "correct-password-123".into(),
                role: "user".into(),
                enabled: true,
                claims: None,
            })),
        )
        .await;
        assert_eq!(result.status(), StatusCode::CREATED);
        state
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_wrong_logins_reserve_before_verification() {
        let state = setup().await;
        let mut tasks = Vec::new();
        for _ in 0..40 {
            let state = state.clone();
            tasks.push(tokio::spawn(async move {
                login(
                    State(state),
                    None,
                    Ok(Json(Login {
                        username: "alice".into(),
                        password: "wrong-password-123".into(),
                    })),
                )
                .await
                .status()
            }));
        }
        let mut statuses = Vec::new();
        for task in tasks {
            statuses.push(task.await.unwrap());
        }
        let refused = |code| statuses.iter().filter(|s| **s == code).count();
        assert_eq!(refused(StatusCode::UNAUTHORIZED), 5);
        assert_eq!(refused(StatusCode::TOO_MANY_REQUESTS), 35);
        assert_eq!(
            state
                .accounts
                .verifications
                .load(std::sync::atomic::Ordering::SeqCst),
            state.accounts.max_failures as usize
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn honest_logins_from_one_address_in_parallel_are_not_refused() {
        let state = setup().await;
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 4000));
        let mut tasks = Vec::new();
        for i in 0..12 {
            let state = state.clone();
            tasks.push(tokio::spawn(async move {
                let created = create(
                    State(state.clone()),
                    Ok(Json(Create {
                        username: format!("player-{i}"),
                        password: "correct-password-123".into(),
                        role: "user".into(),
                        enabled: true,
                        claims: None,
                    })),
                )
                .await;
                assert_eq!(created.status(), StatusCode::CREATED);
                login(
                    State(state),
                    Some(axum::Extension(ConnectInfo(peer))),
                    Ok(Json(Login {
                        username: format!("player-{i}"),
                        password: "correct-password-123".into(),
                    })),
                )
                .await
                .status()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn full_failure_map_evicts_oldest_and_correct_login_succeeds() {
        let state = setup().await;
        {
            let mut failures = state.accounts.failures.lock().unwrap();
            for i in 0..8192 {
                failures.insert(
                    format!("old:{i}"),
                    Failure {
                        count: 1,
                        first: now(),
                        until: 0,
                    },
                );
            }
        }
        let response = login(
            State(state.clone()),
            None,
            Ok(Json(Login {
                username: "alice".into(),
                password: "correct-password-123".into(),
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.accounts.failures.lock().unwrap().len(), 8192);
    }

    #[tokio::test]
    async fn successful_login_rehashes_old_cost() {
        let state = setup().await;
        let old = make_hash("correct-password-123".into(), 100000)
            .await
            .unwrap();
        state
            .accounts
            .data
            .write()
            .await
            .accounts
            .get_mut("alice")
            .unwrap()
            .password_hash = old.clone();
        let response = login(
            State(state.clone()),
            None,
            Ok(Json(Login {
                username: "alice".into(),
                password: "correct-password-123".into(),
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let data = state.accounts.data.read().await;
        let hash = &data.accounts["alice"].password_hash;
        assert_ne!(hash, &old);
        assert_eq!(hash_cost(hash), Some(state.accounts.iterations));
        assert!(verify("correct-password-123".into(), Some(hash.clone()), 100000).await);
    }

    async fn attempt(state: &Arc<Inner>, address: [u8; 4], name: &str, password: &str) -> Response {
        login(
            State(state.clone()),
            Some(axum::Extension(ConnectInfo(std::net::SocketAddr::from((
                address, 4000,
            ))))),
            Ok(Json(Login {
                username: name.into(),
                password: password.into(),
            })),
        )
        .await
    }

    #[tokio::test]
    async fn failures_from_one_address_do_not_lock_out_another() {
        let state = setup().await;
        let attacker = [10, 0, 0, 66];
        for _ in 0..state.accounts.max_failures {
            let response = attempt(&state, attacker, "alice", "wrong-password-123").await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        // The attacker's address is now locked out of this username, and told for how long.
        let response = attempt(&state, attacker, "alice", "correct-password-123").await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry: u64 = response.headers()[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!((1..=3).contains(&retry), "{retry}");
        // The owner, from an address that has not been failing, signs in.
        let response = attempt(&state, [10, 0, 0, 7], "alice", "correct-password-123").await;
        assert_eq!(response.status(), StatusCode::OK);
        // The attacker's lock covers only that username: another one from the same address is
        // checked (and refused for its wrong password, not for the lock).
        let response = attempt(&state, attacker, "bob", "wrong-password-123").await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn failure_lock_is_held_in_state_and_global_ceiling_locks_every_address() {
        let state = setup().await;
        let user = hash_token("alice");
        for _ in 0..state.accounts.max_failures {
            attempt(&state, [10, 0, 0, 1], "alice", "wrong-password-123").await;
        }
        {
            let failures = state.accounts.failures.lock().unwrap();
            let own = &failures[&format!("a:{user}:10.0.0.1")];
            assert_eq!(own.count, state.accounts.max_failures);
            assert!(own.until > now());
            // Neither wider budget is anywhere near its limit.
            assert_eq!(failures[&format!("u:{user}")].until, 0);
            assert_eq!(failures["p:10.0.0.1"].until, 0);
        }
        // Guessing spread over many addresses fills the per-username ceiling, which then holds
        // every address, a clean one included.
        let ceiling = state.accounts.failure_limit("u:");
        assert_eq!(ceiling, state.accounts.max_failures * 100);
        state.accounts.failures.lock().unwrap().insert(
            format!("u:{user}"),
            Failure {
                count: ceiling,
                first: now(),
                until: now() + 2,
            },
        );
        let response = attempt(&state, [10, 0, 0, 9], "alice", "correct-password-123").await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
    }
}
