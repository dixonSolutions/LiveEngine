//! Provider-hosted hubs and their hub-scoped credential.
//!
//! A central created with `"hosting":"provider"` runs on this service, and its creator is
//! handed one credential that manages only that hub and its subtree. Only a SHA-256 digest
//! is stored. The credential is a security boundary, so the rules are default deny:
//!
//! * it is accepted on exactly one endpoint, `POST /v1/route`; every other route, method and
//!   path refuses it, including schemas, templates, links, export, import and `/v1/tree`;
//! * the request path must lie inside the hub that owns the credential: its first segments
//!   are that hub's full path, so a sibling, a parent and the service root are all refused;
//! * only the operations in [`ALLOWED_OPS`] are accepted, and request fields that carry
//!   server-side forwarding state are refused;
//! * the owning hub is located by walking local centrals only, before anything is forwarded,
//!   so the request cannot reach a remote service on the strength of this credential until
//!   the credential itself has been matched inside the subtree.
use crate::hub::{error, expire_hubs, HubMode, Inner, RouteRequest};
use axum::{
    http::{header, HeaderMap, Method, StatusCode},
    response::Response,
};
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Credentials start with this, so an admin token or link credential is never mistaken for one.
const PREFIX: &str = "lehub_";
const MAX_CREDENTIAL_BYTES: usize = 128;

/// Route operations a hub-scoped credential may run, against its own subtree only.
/// Anything not listed, including operations added later, is refused.
pub(crate) const ALLOWED_OPS: [&str; 12] = [
    "discover",
    "pair",
    "tree",
    "register",
    "list",
    "get",
    "delete",
    "watchTicket",
    "ticket",
    "authorize",
    "authPolicy",
    "call",
];

/// Marks a request that presented a hub-scoped credential. It proves nothing by itself:
/// `/v1/route` must still match the digest to a hub and check the path.
#[derive(Clone)]
pub(crate) struct ScopedAuth {
    digest: String,
}
impl ScopedAuth {
    pub(crate) fn digest(&self) -> &str {
        &self.digest
    }
}

/// A new credential and the digest that is stored in its place.
pub(crate) fn mint() -> (String, String) {
    let token = format!(
        "{PREFIX}{:032x}{:032x}",
        rand::random::<u128>(),
        rand::random::<u128>()
    );
    let digest = crate::links::digest(&token);
    (token, digest)
}

/// A digest must look like one before it is trusted from a state file.
pub(crate) fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Constant-time comparison of a stored digest with the presented one.
pub(crate) fn matches(stored: Option<&str>, presented: &str) -> bool {
    stored.is_some_and(|stored| bool::from(stored.as_bytes().ct_eq(presented.as_bytes())))
}

/// Recognise a hub-scoped credential, only on the one endpoint that accepts it.
pub(crate) fn candidate(method: &Method, path: &str, headers: &HeaderMap) -> Option<ScopedAuth> {
    if method != Method::POST || path != "/v1/route" {
        return None;
    }
    let token = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    if !token.starts_with(PREFIX) || token.len() > MAX_CREDENTIAL_BYTES {
        return None;
    }
    Some(ScopedAuth {
        digest: crate::links::digest(token),
    })
}

/// Decide whether a scoped request may run. Returns the index in `request.path` of the hub
/// that owns the credential; the route walk checks the credential again there.
pub(crate) async fn authorize_route(
    state: &Arc<Inner>,
    auth: &ScopedAuth,
    request: &RouteRequest,
) -> Result<usize, Box<Response>> {
    let unauthorized = || error(StatusCode::UNAUTHORIZED, "unauthorized");
    let forbidden = || {
        error(
            StatusCode::FORBIDDEN,
            "credential is not valid for this request",
        )
    };
    // Find the owner on local centrals only. A remote central, or any segment that is not
    // a local central, ends the search: nothing is forwarded for an unmatched credential.
    let mut current = state.clone();
    let mut owner = None;
    for (index, segment) in request.path.iter().enumerate() {
        let (held, child) = {
            let mut hubs = current.hubs.write().await;
            expire_hubs(&mut hubs);
            let Some(record) = hubs.get(segment) else {
                break;
            };
            if record.info.mode != HubMode::Central {
                break;
            }
            (
                matches(record.hosted_credential.as_deref(), &auth.digest),
                record.child.clone(),
            )
        };
        if held {
            owner = Some(index);
            break;
        }
        let Some(child) = child else { break };
        current = child;
    }
    let Some(owner) = owner else {
        return Err(Box::new(unauthorized()));
    };
    if !ALLOWED_OPS.contains(&request.op.as_str()) {
        return Err(Box::new(forbidden()));
    }
    // Forwarding state is set by this service; a client must not supply it.
    if request.inherited_schemas.is_some() || request.inherited_channels.is_some() {
        return Err(Box::new(forbidden()));
    }
    // Templates are admin-owned defaults, including write-only credentials.
    if request.op == "register" && request.body.get("template").is_some() {
        return Err(Box::new(forbidden()));
    }
    Ok(owner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_compare_exactly() {
        let (token, digest) = mint();
        assert!(token.starts_with(PREFIX) && valid_digest(&digest));
        assert!(matches(Some(&digest), &digest));
        assert!(!matches(None, &digest));
        assert!(!matches(Some(&digest), &crate::links::digest("other")));
        assert!(!matches(Some(&digest), &digest[..63]));
        assert!(!matches(Some(&digest), ""));
        assert!(!valid_digest("zz"));
        assert!(!valid_digest(&"g".repeat(64)));
    }

    #[test]
    fn only_post_route_with_a_hub_credential_is_a_candidate() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer lehub_abc".parse().unwrap());
        assert!(candidate(&Method::POST, "/v1/route", &headers).is_some());
        for (method, path) in [
            (Method::GET, "/v1/route"),
            (Method::POST, "/v1/route/"),
            (Method::POST, "/v1/hubs"),
            (Method::POST, "/v1/tree"),
            (Method::DELETE, "/v1/route"),
        ] {
            assert!(
                candidate(&method, path, &headers).is_none(),
                "{method} {path}"
            );
        }
        headers.insert(header::AUTHORIZATION, "Bearer other".parse().unwrap());
        assert!(candidate(&Method::POST, "/v1/route", &headers).is_none());
        headers.insert(header::AUTHORIZATION, "lehub_abc".parse().unwrap());
        assert!(candidate(&Method::POST, "/v1/route", &headers).is_none());
    }
}
