//! Pluggable per-hub credential verification.
//!
//! A hub may carry an `auth` definition: `token`, `hmac`, `jwt`, `webhook` or `custom`
//! (a compiled [`AuthVerifier`] registered on the `HubService`). A presented credential
//! resolves to an identity plus user-defined claims before a ticket is issued. Core never
//! interprets claims. Secrets are persisted with the hub definition and never returned.
use crate::hub::{error, validated_remote_url, Inner};
use axum::{http::StatusCode, response::Response};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ring::{digest, hmac, signature};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;

const MAX_CREDENTIAL_BYTES: usize = 8192;
const MAX_CLAIMS_BYTES: usize = 16 * 1024;
const MAX_TOKENS: usize = 256;
const MAX_WEBHOOK_RESPONSE: usize = 64 * 1024;
const MIN_SECRET_BYTES: usize = 32;
const MIN_TOKEN_BYTES: usize = 24;
const DEFAULT_WEBHOOK_TIMEOUT_MS: u64 = 3000;
const MAX_WEBHOOK_TIMEOUT_MS: u64 = 10_000;
const MAX_LEEWAY_SECONDS: u64 = 300;

/// The verified result of a credential. Claims are opaque and user-defined.
#[derive(Clone, Debug, PartialEq)]
pub struct Identity {
    pub identity: String,
    pub claims: Map<String, Value>,
}

/// A verification failure. It carries no detail so callers cannot leak any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthError;

/// What a verifier is asked to check.
pub struct AuthRequest<'a> {
    /// Path of the central holding the hub, without the hub itself.
    pub hub_path: &'a [String],
    pub hub: &'a str,
    pub room: &'a str,
    pub user: &'a str,
    pub role: &'a str,
    pub credential: &'a str,
}

/// A compiled custom verifier. Register one with `HubService::register_verifier`
/// and reference it from a hub with `{"kind":"custom","name":"..."}`.
pub trait AuthVerifier: Send + Sync + 'static {
    fn verify<'a>(
        &'a self,
        request: &'a AuthRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Identity, AuthError>> + Send + 'a>>;
}

/// Verifiers registered on a service, shared with every nested central.
pub(crate) type Verifiers =
    Arc<std::sync::RwLock<std::collections::HashMap<String, Arc<dyn AuthVerifier>>>>;

/// The only algorithms a hub can be configured for. `none` cannot be expressed.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum JwtAlg {
    #[serde(rename = "HS256")]
    Hs256,
    #[serde(rename = "EdDSA")]
    EdDsa,
}
impl JwtAlg {
    fn name(self) -> &'static str {
        match self {
            Self::Hs256 => "HS256",
            Self::EdDsa => "EdDSA",
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TokenEntry {
    token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    claims: Map<String, Value>,
}

/// A hub's auth definition, including secrets. Never serialized into any response.
#[derive(Clone, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "lowercase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum AuthConfig {
    Token {
        tokens: Vec<TokenEntry>,
    },
    Hmac {
        secret: String,
        #[serde(default)]
        leeway_seconds: Option<u64>,
    },
    Jwt {
        alg: JwtAlg,
        /// HS256 shared secret.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret: Option<String>,
        /// EdDSA public key: 32 raw bytes, base64url without padding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        public_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        issuer: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        audience: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity_claim: Option<String>,
        #[serde(default)]
        leeway_seconds: Option<u64>,
    },
    Webhook {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret: Option<String>,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    Custom {
        name: String,
    },
}

/// The non-secret description of a hub's auth that appears in `HubInfo`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthInfo {
    pub kind: String,
}

impl AuthConfig {
    pub(crate) fn info(&self) -> AuthInfo {
        AuthInfo {
            kind: match self {
                Self::Token { .. } => "token",
                Self::Hmac { .. } => "hmac",
                Self::Jwt { .. } => "jwt",
                Self::Webhook { .. } => "webhook",
                Self::Custom { .. } => "custom",
            }
            .into(),
        }
    }

    pub(crate) fn validate(&self, state: &Inner, registering: bool) -> Result<(), String> {
        match self {
            Self::Token { tokens } => {
                if tokens.is_empty() || tokens.len() > MAX_TOKENS {
                    return Err("auth tokens must hold 1 to 256 entries".into());
                }
                for entry in tokens {
                    if entry.token.len() < MIN_TOKEN_BYTES || entry.token.len() > 512 {
                        return Err("auth tokens must be 24 to 512 bytes".into());
                    }
                    if entry.identity.as_deref().is_some_and(str::is_empty) {
                        return Err("auth identity must not be empty".into());
                    }
                    if serde_json::to_vec(&entry.claims)
                        .map_or(true, |v| v.len() > MAX_CLAIMS_BYTES)
                    {
                        return Err("auth claims exceed size limit".into());
                    }
                }
            }
            Self::Hmac {
                secret,
                leeway_seconds,
            } => {
                if secret.len() < MIN_SECRET_BYTES || secret.len() > 512 {
                    return Err("auth secret must be 32 to 512 bytes".into());
                }
                valid_leeway(*leeway_seconds)?;
            }
            Self::Jwt {
                alg,
                secret,
                public_key,
                identity_claim,
                leeway_seconds,
                ..
            } => {
                valid_leeway(*leeway_seconds)?;
                if identity_claim.as_deref().is_some_and(str::is_empty) {
                    return Err("auth identityClaim must not be empty".into());
                }
                match (alg, secret, public_key) {
                    (JwtAlg::Hs256, Some(s), None)
                        if (MIN_SECRET_BYTES..=512).contains(&s.len()) => {}
                    (JwtAlg::Hs256, ..) => {
                        return Err(
                            "HS256 requires a secret of 32 to 512 bytes and no publicKey".into(),
                        )
                    }
                    (JwtAlg::EdDsa, None, Some(key))
                        if URL_SAFE_NO_PAD
                            .decode(key)
                            .is_ok_and(|bytes| bytes.len() == 32) => {}
                    (JwtAlg::EdDsa, ..) => {
                        return Err(
                            "EdDSA requires a 32 byte base64url publicKey and no secret".into()
                        )
                    }
                }
            }
            Self::Webhook {
                url,
                secret,
                timeout_ms,
            } => {
                validated_remote_url(state, url)?;
                if secret
                    .as_deref()
                    .is_some_and(|s| s.is_empty() || s.len() > 512)
                {
                    return Err("auth secret must be 1 to 512 bytes".into());
                }
                if timeout_ms.is_some_and(|t| !(100..=MAX_WEBHOOK_TIMEOUT_MS).contains(&t)) {
                    return Err("auth timeoutMs must be 100 to 10000".into());
                }
            }
            Self::Custom { name } => {
                if name.is_empty()
                    || name.len() > 64
                    || !name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                {
                    return Err(
                        "auth name must be 1 to 64 ASCII letters, digits, '.', '_' or '-'".into(),
                    );
                }
                if registering && !state.has_verifier(name) {
                    return Err("auth verifier is not registered".into());
                }
            }
        }
        Ok(())
    }
}

fn valid_leeway(leeway: Option<u64>) -> Result<(), String> {
    if leeway.is_some_and(|l| l > MAX_LEEWAY_SECONDS) {
        return Err("auth leewaySeconds must be at most 300".into());
    }
    Ok(())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn sha(value: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, value.as_bytes()).as_ref());
    out
}

fn verify_token(tokens: &[TokenEntry], credential: &str) -> Result<Identity, AuthError> {
    let presented = sha(credential);
    // Every entry is compared, with no early exit, so timing does not reveal a match.
    let mut found: Option<&TokenEntry> = None;
    for entry in tokens {
        if bool::from(sha(&entry.token).ct_eq(&presented)) {
            found = Some(entry);
        }
    }
    let entry = found.ok_or(AuthError)?;
    Ok(Identity {
        identity: entry.identity.clone().unwrap_or_default(),
        claims: entry.claims.clone(),
    })
}

fn decode_object(segment: &str) -> Result<Map<String, Value>, AuthError> {
    let bytes = URL_SAFE_NO_PAD.decode(segment).map_err(|_| AuthError)?;
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(AuthError),
    }
}

/// Registered time claims are checked here; everything else is returned as claims.
fn identity_from_payload(
    mut payload: Map<String, Value>,
    identity_claim: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
    leeway: u64,
) -> Result<Identity, AuthError> {
    let now = now();
    let exp = payload
        .get("exp")
        .and_then(Value::as_u64)
        .ok_or(AuthError)?;
    if exp.saturating_add(leeway) <= now {
        return Err(AuthError);
    }
    if let Some(nbf) = payload.get("nbf") {
        if nbf.as_u64().ok_or(AuthError)? > now.saturating_add(leeway) {
            return Err(AuthError);
        }
    }
    if let Some(issuer) = issuer {
        if payload.get("iss").and_then(Value::as_str) != Some(issuer) {
            return Err(AuthError);
        }
    }
    if let Some(audience) = audience {
        let ok = match payload.get("aud") {
            Some(Value::String(a)) => a == audience,
            Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(audience)),
            _ => false,
        };
        if !ok {
            return Err(AuthError);
        }
    }
    let identity = match payload.remove(identity_claim) {
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => return Err(AuthError),
    };
    for registered in ["exp", "nbf", "iat"] {
        payload.remove(registered);
    }
    if serde_json::to_vec(&payload).map_or(true, |v| v.len() > MAX_CLAIMS_BYTES) {
        return Err(AuthError);
    }
    Ok(Identity {
        identity,
        claims: payload,
    })
}

/// `credential` is `base64url(payload JSON).base64url(HMAC-SHA256(secret, first segment))`.
/// The payload holds `sub`, a required `exp` (Unix seconds) and any user-defined claims.
fn verify_hmac(secret: &str, leeway: Option<u64>, credential: &str) -> Result<Identity, AuthError> {
    let (payload, mac) = credential.split_once('.').ok_or(AuthError)?;
    if mac.contains('.') {
        return Err(AuthError);
    }
    let mac = URL_SAFE_NO_PAD.decode(mac).map_err(|_| AuthError)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, payload.as_bytes(), &mac).map_err(|_| AuthError)?;
    identity_from_payload(
        decode_object(payload)?,
        "sub",
        None,
        None,
        leeway.unwrap_or(0),
    )
}

#[allow(clippy::too_many_arguments)]
fn verify_jwt(
    alg: JwtAlg,
    secret: Option<&str>,
    public_key: Option<&str>,
    issuer: Option<&str>,
    audience: Option<&str>,
    identity_claim: &str,
    leeway: u64,
    token: &str,
) -> Result<Identity, AuthError> {
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(sig), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(AuthError);
    };
    let header = decode_object(header)?;
    // The algorithm comes only from hub config. The token must name exactly that one,
    // so `none`, a different algorithm, or a key-confusion attempt is refused.
    if header.get("alg").and_then(Value::as_str) != Some(alg.name()) || header.contains_key("crit")
    {
        return Err(AuthError);
    }
    let signing_input = &token[..header_len(token, payload)];
    let sig = URL_SAFE_NO_PAD.decode(sig).map_err(|_| AuthError)?;
    match alg {
        JwtAlg::Hs256 => {
            let key = hmac::Key::new(hmac::HMAC_SHA256, secret.ok_or(AuthError)?.as_bytes());
            hmac::verify(&key, signing_input.as_bytes(), &sig).map_err(|_| AuthError)?;
        }
        JwtAlg::EdDsa => {
            let key = URL_SAFE_NO_PAD
                .decode(public_key.ok_or(AuthError)?)
                .map_err(|_| AuthError)?;
            signature::UnparsedPublicKey::new(&signature::ED25519, key)
                .verify(signing_input.as_bytes(), &sig)
                .map_err(|_| AuthError)?;
        }
    }
    identity_from_payload(
        decode_object(payload)?,
        identity_claim,
        issuer,
        audience,
        leeway,
    )
}

/// Length of `header.payload` within `token`.
fn header_len(token: &str, payload: &str) -> usize {
    let payload_start = payload.as_ptr() as usize - token.as_ptr() as usize;
    payload_start + payload.len()
}

async fn verify_webhook(
    state: &Inner,
    url: &str,
    secret: Option<&str>,
    timeout_ms: Option<u64>,
    request: &AuthRequest<'_>,
) -> Result<Identity, AuthError> {
    // Same allowed-host rules as remote registration, checked again at call time.
    let url = validated_remote_url(state, url).map_err(|_| AuthError)?;
    let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_WEBHOOK_TIMEOUT_MS));
    let body = serde_json::json!({
        "hub": request.hub,
        "path": request.hub_path,
        "userId": request.user,
        "serverId": request.room,
        "role": request.role,
        "credential": request.credential,
    });
    let call = async {
        let mut builder = state.client.post(url).timeout(timeout).json(&body);
        if let Some(secret) = secret {
            builder = builder.bearer_auth(secret);
        }
        let mut response = builder.send().await.map_err(|_| AuthError)?;
        if response.status().as_u16() != 200 {
            return Err(AuthError);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| AuthError)? {
            if bytes.len() + chunk.len() > MAX_WEBHOOK_RESPONSE {
                return Err(AuthError);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    };
    // The whole exchange, body included, is bounded; any miss fails closed.
    let bytes = tokio::time::timeout(timeout, call)
        .await
        .map_err(|_| AuthError)??;
    let Value::Object(mut value) = serde_json::from_slice(&bytes).map_err(|_| AuthError)? else {
        return Err(AuthError);
    };
    if value.get("allow") != Some(&Value::Bool(true)) {
        return Err(AuthError);
    }
    let identity = match value.remove("identity") {
        None => String::new(),
        Some(Value::String(s)) if !s.is_empty() => s,
        _ => return Err(AuthError),
    };
    let claims = match value.remove("claims") {
        None => Map::new(),
        Some(Value::Object(map)) => map,
        _ => return Err(AuthError),
    };
    if serde_json::to_vec(&claims).map_or(true, |v| v.len() > MAX_CLAIMS_BYTES) {
        return Err(AuthError);
    }
    Ok(Identity { identity, claims })
}

/// Resolve a credential for a user joining a room. The identity must be the user the
/// ticket is for, so a credential can never mint a ticket for someone else. No lock is
/// held by callers or taken here across the network call.
pub(crate) async fn verify(
    state: &Inner,
    config: &AuthConfig,
    hub: &str,
    user: &str,
    room: &str,
    role: &str,
    credential: Option<&str>,
) -> Result<Identity, AuthError> {
    let credential = credential
        .filter(|c| !c.is_empty() && c.len() <= MAX_CREDENTIAL_BYTES)
        .ok_or(AuthError)?;
    let request = AuthRequest {
        hub_path: &state.hub_path,
        hub,
        room,
        user,
        role,
        credential,
    };
    let mut identity = match config {
        AuthConfig::Token { tokens } => verify_token(tokens, credential)?,
        AuthConfig::Hmac {
            secret,
            leeway_seconds,
        } => verify_hmac(secret, *leeway_seconds, credential)?,
        AuthConfig::Jwt {
            alg,
            secret,
            public_key,
            issuer,
            audience,
            identity_claim,
            leeway_seconds,
        } => verify_jwt(
            *alg,
            secret.as_deref(),
            public_key.as_deref(),
            issuer.as_deref(),
            audience.as_deref(),
            identity_claim.as_deref().unwrap_or("sub"),
            leeway_seconds.unwrap_or(0),
            credential,
        )?,
        AuthConfig::Webhook {
            url,
            secret,
            timeout_ms,
        } => verify_webhook(state, url, secret.as_deref(), *timeout_ms, &request).await?,
        AuthConfig::Custom { name } => {
            let verifier = state.verifier(name).ok_or(AuthError)?;
            verifier.verify(&request).await?
        }
    };
    if identity.identity.is_empty() {
        identity.identity = user.to_string();
    }
    if identity.identity != user {
        return Err(AuthError);
    }
    Ok(identity)
}

/// Gate used before a ticket is issued. Any failure is the same opaque refusal.
pub(crate) async fn authorize(
    state: &Inner,
    config: Option<&AuthConfig>,
    hub: &str,
    user: &str,
    room: &str,
    role: &str,
    credential: Option<&str>,
) -> Result<(), Response> {
    let Some(config) = config else { return Ok(()) };
    match verify(state, config, hub, user, room, role, credential).await {
        Ok(_) => Ok(()),
        Err(_) => Err(error(StatusCode::FORBIDDEN, "credential refused")),
    }
}
