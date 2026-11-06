#![cfg(feature = "host")]
//! Provider-hosted hubs and the boundary of their hub-scoped credential.
use liveengine::hub::{HubConfig, HubService, HubSpec};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "hosting-integration-admin-token-123456";
const REMOTE: &str = "hosting-remote-admin-token-123456789";

async fn serve(service: &HubService) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = service.clone();
    tokio::spawn(async move {
        axum::serve(
            listener,
            server
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    base
}
fn config(token: &str) -> HubConfig {
    HubConfig {
        token: token.into(),
        public_url: Some("https://live.example.test/v1/live".into()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        rate_limit_requests: 1_000_000,
        ..HubConfig::default()
    }
}
async fn node(token: &str) -> (HubService, String) {
    let service = HubService::new(config(token)).unwrap();
    let base = serve(&service).await;
    (service, base)
}
async fn send(
    base: &str,
    method: &str,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut request = client
        .request(method.parse().unwrap(), format!("{base}{path}"))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}
async fn route(base: &str, token: &str, path: Value, op: &str, body: Value) -> (u16, Value) {
    send(
        base,
        "POST",
        "/v1/route",
        token,
        Some(json!({"path":path,"op":op,"body":body})),
    )
    .await
}
/// Create a provider-hosted central as the administrator and return its credential.
async fn hosted(base: &str, id: &str) -> String {
    let (status, body) = send(
        base,
        "POST",
        "/v1/hubs",
        TOKEN,
        Some(json!({"id":id,"mode":"central","hosting":"provider"})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    body["credential"].as_str().unwrap().to_owned()
}
/// Create a child through the hosted hub's own credential.
async fn child(base: &str, credential: &str, path: Value, body: Value) -> (u16, Value) {
    route(base, credential, path, "register", body).await
}
fn refused(status: u16) -> bool {
    status == 401 || status == 403
}
fn digest(value: &str) -> String {
    ring::digest::digest(&ring::digest::SHA256, value.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[tokio::test]
async fn provider_hosting_returns_a_credential_once_and_stores_only_its_hash() {
    let path = std::env::temp_dir().join(format!(
        "hosting-state-{}-{}.json",
        std::process::id(),
        rand::random::<u64>()
    ));
    let mut cfg = config(TOKEN);
    cfg.state_file = Some(path.clone());
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let base = serve(&service).await;
    let credential = hosted(&base, "acme").await;
    assert!(credential.starts_with("lehub_") && credential.len() == 70);
    // It is never readable again.
    for (method, path) in [("GET", "/v1/hubs/acme"), ("GET", "/v1/hubs")] {
        let (status, body) = send(&base, method, path, TOKEN, None).await;
        assert_eq!(status, 200);
        assert!(!body.to_string().contains(&credential), "{body}");
        assert!(!body.to_string().contains("credential"), "{body}");
    }
    let (_, tree) = send(&base, "GET", "/v1/tree", TOKEN, None).await;
    let (_, export) = send(&base, "GET", "/v1/registry/export", TOKEN, None).await;
    assert!(!tree.to_string().contains(&credential));
    assert!(!export.to_string().contains(&credential));
    // The state file holds the digest, not the credential.
    let state = std::fs::read_to_string(&path).unwrap();
    assert!(!state.contains(&credential));
    assert!(state.contains(&digest(&credential)));
    // A restart keeps the credential working.
    let restarted = HubService::new(cfg).unwrap();
    let base2 = serve(&restarted).await;
    let (status, body) = route(&base2, &credential, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 200, "{body}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn provider_hosting_requests_are_validated() {
    let (service, base) = node(TOKEN).await;
    for body in [
        json!({"id":"a","mode":"internal","hosting":"provider"}),
        json!({"id":"a","mode":"external","address":"http://127.0.0.1:1","remoteToken":"x".repeat(30),"hosting":"provider"}),
        json!({"id":"a","mode":"central","address":"http://127.0.0.1:1","remoteToken":"x".repeat(30),"hosting":"provider"}),
        json!({"id":"a","mode":"central","hosting":"elsewhere"}),
        json!({"id":"a","mode":"central","hosting":true}),
        json!({"id":"a","mode":"central","hosting":"provider","auth":{"kind":"token","tokens":[]}}),
    ] {
        let (status, body) = send(&base, "POST", "/v1/hubs", TOKEN, Some(body)).await;
        assert_eq!(status, 400, "{body}");
    }
    // A hub without hosting never carries a credential.
    let (status, body) = send(
        &base,
        "POST",
        "/v1/hubs",
        TOKEN,
        Some(json!({"id":"plain","mode":"central"})),
    )
    .await;
    assert_eq!(status, 201);
    assert!(body.get("credential").is_none());
    // The Rust API: register_hosted returns the credential; register cannot drop it.
    let hosted = service
        .register_hosted(HubSpec {
            id: "rust".into(),
            hosting: Some("provider".into()),
            scopes: json!({}),
            ..HubSpec::default()
        })
        .await
        .unwrap();
    assert!(hosted.credential.starts_with("lehub_"));
    assert_eq!(hosted.info.id, "rust");
    assert!(service
        .register(HubSpec {
            id: "other".into(),
            mode: Some(liveengine::hub::HubMode::Central),
            hosting: Some("provider".into()),
            scopes: json!({}),
            ..HubSpec::default()
        })
        .await
        .is_err());
    assert!(service
        .register_hosted(HubSpec {
            id: "x".into(),
            scopes: json!({}),
            ..HubSpec::default()
        })
        .await
        .is_err());
    let (status, _) = route(
        &base,
        &hosted.credential,
        json!(["rust"]),
        "list",
        json!({}),
    )
    .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn provider_credential_manages_its_hub_and_its_subtree() {
    let (_service, base) = node(TOKEN).await;
    let credential = hosted(&base, "acme").await;
    let at = |path: Value, op: &'static str, body: Value| {
        let (base, credential) = (base.clone(), credential.clone());
        async move { route(&base, &credential, path, op, body).await }
    };
    // Internal hub in the hosted hub.
    let (status, body) = at(
        json!(["acme"]),
        "register",
        json!({"id":"game","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert!(body.get("credential").is_none());
    let (status, body) = at(json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(body["hubs"][0]["id"], "game");
    let (status, _) = at(json!(["acme"]), "get", json!({"id":"game"})).await;
    assert_eq!(status, 200);
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        let (status, body) = at(
            json!(["acme"]),
            "call",
            json!({"id":"game","method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let (status, body) = at(
        json!(["acme"]),
        "ticket",
        json!({"id":"game","userId":"alice","serverId":"room"}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["ticket"].is_string());
    let (status, _) = at(
        json!(["acme"]),
        "authorize",
        json!({"id":"game","userId":"alice","serverId":"room"}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, body) = at(json!(["acme"]), "tree", json!({"depth":2})).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = at(json!(["acme"]), "discover", json!({"kind":"hub"})).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = at(json!(["acme"]), "watchTicket", json!({"depth":1})).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = at(
        json!(["acme"]),
        "pair",
        json!({"subject":{"kind":"peer","id":"carol","scopes":{}},"target":{"kind":"hub"}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    // Nested central, then a hub inside it.
    let (status, body) = at(
        json!(["acme"]),
        "register",
        json!({"id":"dept","mode":"central"}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = at(
        json!(["acme", "dept"]),
        "register",
        json!({"id":"deep","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = at(json!(["acme", "dept"]), "list", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(body["hubs"][0]["id"], "deep");
    // The convenience endpoints reach the same subtree through the route op.
    let (status, _) = at(json!(["acme", "dept"]), "delete", json!({"id":"deep"})).await;
    assert_eq!(status, 204);
    let (status, _) = at(json!(["acme"]), "delete", json!({"id":"game"})).await;
    assert_eq!(status, 204);
    // The administrator sees the same state.
    let (status, body) = route(&base, TOKEN, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(body["hubs"].as_array().unwrap().len(), 1);
    assert_eq!(body["hubs"][0]["id"], "dept");
}

#[tokio::test]
async fn provider_credential_can_host_a_nested_hub_with_a_narrower_credential() {
    let (_service, base) = node(TOKEN).await;
    let outer = hosted(&base, "acme").await;
    let (status, body) = child(
        &base,
        &outer,
        json!(["acme"]),
        json!({"id":"sub","mode":"central","hosting":"provider"}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let inner = body["credential"].as_str().unwrap().to_owned();
    assert_ne!(inner, outer);
    // The outer credential reaches the inner subtree; the inner does not reach upward.
    let (status, _) = route(&base, &outer, json!(["acme", "sub"]), "list", json!({})).await;
    assert_eq!(status, 200);
    let (status, _) = route(&base, &inner, json!(["acme", "sub"]), "list", json!({})).await;
    assert_eq!(status, 200);
    for op in ["list", "tree", "discover"] {
        let (status, body) = route(&base, &inner, json!(["acme"]), op, json!({"kind":"hub"})).await;
        assert!(refused(status), "{op} reached the parent: {status} {body}");
    }
    let (status, _) = route(&base, &inner, json!([]), "list", json!({})).await;
    assert!(refused(status));
}

#[tokio::test]
async fn provider_credential_is_refused_on_a_sibling_a_parent_and_the_root() {
    let (_service, base) = node(TOKEN).await;
    let acme = hosted(&base, "acme").await;
    let beta = hosted(&base, "beta").await;
    // Content in the sibling, so a leak or a change would be visible.
    let (status, _) = child(
        &base,
        &beta,
        json!(["beta"]),
        json!({"id":"secret","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) = child(
        &base,
        &acme,
        json!(["acme"]),
        json!({"id":"sub","mode":"central","hosting":"provider"}),
    )
    .await;
    assert_eq!(status, 201);
    let sub = body["credential"].as_str().unwrap().to_owned();
    let (status, _) = child(
        &base,
        &acme,
        json!(["acme"]),
        json!({"id":"other","mode":"central"}),
    )
    .await;
    assert_eq!(status, 201);

    let bodies = [
        ("discover", json!({"kind":"any"})),
        (
            "pair",
            json!({"subject":{"kind":"peer","id":"mallory","scopes":{}},"target":{"kind":"hub"}}),
        ),
        ("tree", json!({"depth":3})),
        ("register", json!({"id":"planted","mode":"internal"})),
        ("list", json!({})),
        ("get", json!({"id":"secret"})),
        ("delete", json!({"id":"secret"})),
        ("watchTicket", json!({"depth":2})),
        ("ticket", json!({"id":"secret","userId":"u","serverId":"r"})),
        (
            "authorize",
            json!({"id":"secret","userId":"u","serverId":"r"}),
        ),
        (
            "call",
            json!({"id":"secret","method":"getUser","args":["u"]}),
        ),
    ];
    let attempts: Vec<(&str, &str, Value)> = vec![
        ("acme on sibling", &acme, json!(["beta"])),
        ("acme inside sibling", &acme, json!(["beta", "x"])),
        ("acme on the root", &acme, json!([])),
        ("sub on its parent", &sub, json!(["acme"])),
        (
            "sub on a sibling under the same parent",
            &sub,
            json!(["acme", "other"]),
        ),
        ("sub on the outer sibling", &sub, json!(["beta"])),
        ("beta on sibling", &beta, json!(["acme"])),
        ("beta inside sibling subtree", &beta, json!(["acme", "sub"])),
        ("beta on the root", &beta, json!([])),
    ]
    .into_iter()
    .map(|(name, token, path)| (name, token.as_str(), path))
    .collect();
    for (name, token, path) in attempts {
        for (op, body) in &bodies {
            let (status, answer) = route(&base, token, path.clone(), op, body.clone()).await;
            assert!(
                refused(status),
                "{name}: {op} on {path} gave {status} {answer}"
            );
        }
    }
    // Path tricks never leave the subtree: ids are matched exactly, never normalised.
    for path in [
        json!(["acme", ".."]),
        json!(["acme/../beta"]),
        json!(["acme", "..", "beta"]),
        json!(["ACME"]),
        json!(["acme "]),
        json!(["%61cme"]),
        json!([""]),
        json!(["beta", "acme"]),
        json!(["acme\u{0}", "beta"]),
    ] {
        let (status, answer) = route(&base, &acme, path.clone(), "list", json!({})).await;
        assert!(
            !(200..300).contains(&status),
            "{path} gave {status} {answer}"
        );
        assert!(
            !answer.to_string().contains("secret"),
            "{path} leaked {answer}"
        );
    }
    // None of that changed or exposed the sibling.
    let (status, listed) = route(&base, TOKEN, json!(["beta"]), "list", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(listed["hubs"].as_array().unwrap().len(), 1);
    assert_eq!(listed["hubs"][0]["id"], "secret");
}

#[tokio::test]
async fn provider_credential_is_refused_on_every_admin_only_route() {
    let (_service, base) = node(TOKEN).await;
    let credential = hosted(&base, "acme").await;
    let routes: Vec<(&str, &str, Option<Value>)> = vec![
        ("GET", "/v1/contract", None),
        ("GET", "/v1/contract/schema", None),
        ("GET", "/v1/schemas", None),
        (
            "POST",
            "/v1/schemas",
            Some(json!({"id":"x","version":"1","schema":{"type":"object"}})),
        ),
        ("GET", "/v1/schemas/liveengine.default@1", None),
        ("DELETE", "/v1/schemas/liveengine.default@1", None),
        ("GET", "/v1/templates", None),
        (
            "POST",
            "/v1/templates",
            Some(json!({"name":"t","mode":"internal"})),
        ),
        ("GET", "/v1/templates/t", None),
        ("DELETE", "/v1/templates/t", None),
        (
            "POST",
            "/v1/tickets/watch",
            Some(json!({"path":["acme"],"depth":1})),
        ),
        (
            "POST",
            "/v1/discover",
            Some(json!({"path":["acme"],"kind":"any"})),
        ),
        (
            "POST",
            "/v1/pair",
            Some(
                json!({"path":["acme"],"subject":{"kind":"peer","id":"c","scopes":{}},"target":{"kind":"hub"}}),
            ),
        ),
        ("GET", "/v1/links", None),
        (
            "POST",
            "/v1/links",
            Some(
                json!({"id":"l","address":"http://127.0.0.1:1","remoteToken":"x".repeat(30),"hubId":"h","userId":"u","serverId":"s"}),
            ),
        ),
        ("DELETE", "/v1/links", None),
        ("GET", "/v1/links/l", None),
        ("DELETE", "/v1/links/l", None),
        ("GET", "/v1/tree", None),
        ("GET", "/v1/tree?depth=5", None),
        ("GET", "/v1/hubs", None),
        (
            "POST",
            "/v1/hubs",
            Some(json!({"id":"made","mode":"internal"})),
        ),
        (
            "POST",
            "/v1/hubs",
            Some(json!({"id":"made","mode":"central","hosting":"provider"})),
        ),
        ("GET", "/v1/hubs/acme", None),
        ("DELETE", "/v1/hubs/acme", None),
        (
            "POST",
            "/v1/hubs/acme/call",
            Some(json!({"method":"getUser","args":["u"]})),
        ),
        (
            "POST",
            "/v1/hubs/acme/tickets",
            Some(json!({"userId":"u","serverId":"s","role":"service"})),
        ),
        ("POST", "/v1/hubs/acme/link-credentials", None),
        ("DELETE", "/v1/hubs/acme/link-credentials", None),
        (
            "POST",
            "/v1/registries/import",
            Some(json!({"address":"http://127.0.0.1:1","token":"x".repeat(30)})),
        ),
        ("GET", "/v1/registry/export", None),
        ("GET", "/v1/route", None),
        (
            "PUT",
            "/v1/route",
            Some(json!({"path":["acme"],"op":"list"})),
        ),
        ("DELETE", "/v1/route", None),
        (
            "POST",
            "/v1/route/",
            Some(json!({"path":["acme"],"op":"list"})),
        ),
        (
            "POST",
            "//v1/route",
            Some(json!({"path":["acme"],"op":"list"})),
        ),
        (
            "POST",
            "/v1/route?x=1/../hubs",
            Some(json!({"path":[],"op":"list"})),
        ),
    ];
    for (method, path, body) in routes {
        let (status, answer) = send(&base, method, path, &credential, body).await;
        assert!(
            !(200..300).contains(&status),
            "{method} {path} accepted the hub credential: {status} {answer}"
        );
        // Routes that exist answer 401; a wrong method or a route that does not exist answers
        // 404 or 405. Never anything that ran.
        assert!(
            matches!(status, 401 | 404 | 405 | 403),
            "{method} {path}: {status} {answer}"
        );
        assert!(
            !answer.to_string().contains("liveengine.default"),
            "{method} {path} leaked content: {answer}"
        );
    }
    // The existing routes answer 401 exactly, so the test is not passing on 404s.
    for (method, path) in [
        ("GET", "/v1/contract"),
        ("GET", "/v1/schemas"),
        ("POST", "/v1/schemas"),
        ("GET", "/v1/templates"),
        ("POST", "/v1/tickets/watch"),
        ("POST", "/v1/discover"),
        ("POST", "/v1/pair"),
        ("GET", "/v1/links"),
        ("POST", "/v1/links"),
        ("GET", "/v1/tree"),
        ("GET", "/v1/hubs"),
        ("POST", "/v1/hubs"),
        ("GET", "/v1/hubs/acme"),
        ("DELETE", "/v1/hubs/acme"),
        ("POST", "/v1/hubs/acme/call"),
        ("POST", "/v1/hubs/acme/tickets"),
        ("POST", "/v1/hubs/acme/link-credentials"),
        ("POST", "/v1/registries/import"),
        ("GET", "/v1/registry/export"),
    ] {
        let (status, _) = send(&base, method, path, &credential, Some(json!({}))).await;
        assert_eq!(status, 401, "{method} {path}");
    }
    // The administrator still reaches them, and nothing above changed the registry.
    let (status, _) = send(&base, "GET", "/v1/tree", TOKEN, None).await;
    assert_eq!(status, 200);
    let (_, hubs) = send(&base, "GET", "/v1/hubs", TOKEN, None).await;
    assert_eq!(hubs["hubs"].as_array().unwrap().len(), 1);
    assert_eq!(hubs["hubs"][0]["id"], "acme");
}

#[tokio::test]
async fn provider_credential_is_limited_to_listed_operations_and_plain_requests() {
    let (_service, base) = node(TOKEN).await;
    let credential = hosted(&base, "acme").await;
    // An operation that is not on the list, whatever its name.
    for op in [
        "",
        "register ",
        "Register",
        "export",
        "import",
        "links",
        "schemas",
        "templates",
        "admin",
        "unknown",
    ] {
        let (status, body) = route(&base, &credential, json!(["acme"]), op, json!({})).await;
        assert_eq!(status, 403, "{op:?}: {body}");
    }
    // Forwarding state is set by the service, never supplied by a scoped caller.
    for extra in [
        json!({"inherited_schemas":[null,null,null]}),
        json!({"inherited_channels":{"channels":{},"schemas":{}}}),
    ] {
        let mut request = json!({"path":["acme"],"op":"list","body":{}});
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (status, body) = send(&base, "POST", "/v1/route", &credential, Some(request)).await;
        assert!(refused(status), "{status} {body}");
    }
    // Templates are admin-owned defaults.
    let (status, _) = send(
        &base,
        "POST",
        "/v1/templates",
        TOKEN,
        Some(json!({"name":"t","mode":"internal"})),
    )
    .await;
    assert!(status == 200 || status == 201);
    let (status, body) = route(
        &base,
        &credential,
        json!(["acme"]),
        "register",
        json!({"id":"fromtemplate","template":"t"}),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    // The administrator token is not needed and not accepted in its place on a scope check:
    // a hub credential for a hub that does not exist is a plain 401.
    let (status, _) = route(
        &base,
        "lehub_00000000000000000000000000000000",
        json!(["acme"]),
        "list",
        json!({}),
    )
    .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn provider_credential_must_match_exactly_and_dies_with_its_hub() {
    let (_service, base) = node(TOKEN).await;
    let credential = hosted(&base, "acme").await;
    let _other = hosted(&base, "beta").await;
    let flipped = {
        let mut bytes = credential.clone().into_bytes();
        let last = bytes.len() - 1;
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };
        String::from_utf8(bytes).unwrap()
    };
    for bad in [
        flipped,
        credential[..credential.len() - 1].to_owned(),
        format!("{credential}0"),
        credential.to_uppercase(),
        credential.replacen("lehub_", "", 1),
        String::new(),
        "lehub_".into(),
        format!("lehub_{}", "a".repeat(5000)),
        digest(&credential),
    ] {
        let (status, _) = route(&base, &bad, json!(["acme"]), "list", json!({})).await;
        assert_eq!(status, 401, "{bad:?}");
    }
    let (status, _) = route(&base, &credential, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 200);
    // Deleting the hub ends the credential; a hub recreated under the same id gets a new one.
    let (status, _) = send(&base, "DELETE", "/v1/hubs/acme", TOKEN, None).await;
    assert_eq!(status, 204);
    let (status, _) = route(&base, &credential, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 401);
    let replacement = hosted(&base, "acme").await;
    assert_ne!(replacement, credential);
    let (status, _) = route(&base, &credential, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 401);
    let (status, _) = route(&base, &replacement, json!(["acme"]), "list", json!({})).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn provider_credential_cannot_reach_through_a_remote_central() {
    let (_remote, remote_base) = node(REMOTE).await;
    let remote_credential = {
        let (status, body) = send(
            &remote_base,
            "POST",
            "/v1/hubs",
            REMOTE,
            Some(json!({"id":"acme","mode":"central","hosting":"provider"})),
        )
        .await;
        assert_eq!(status, 201, "{body}");
        body["credential"].as_str().unwrap().to_owned()
    };
    let (_front, front_base) = node(TOKEN).await;
    let (status, body) = send(
        &front_base,
        "POST",
        "/v1/hubs",
        TOKEN,
        Some(json!({"id":"r","mode":"central","address":remote_base,"remoteToken":REMOTE})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    // On its own service the credential works.
    let (status, _) = route(
        &remote_base,
        &remote_credential,
        json!(["acme"]),
        "list",
        json!({}),
    )
    .await;
    assert_eq!(status, 200);
    // On a service that forwards to it, the walk reaches a remote central before it can match
    // the credential, so nothing is forwarded with the stored admin token.
    for path in [
        json!(["r"]),
        json!(["r", "acme"]),
        json!(["acme"]),
        json!([]),
    ] {
        let (status, body) = route(
            &front_base,
            &remote_credential,
            path.clone(),
            "list",
            json!({}),
        )
        .await;
        assert_eq!(status, 401, "{path}: {body}");
        let (status, body) = route(
            &front_base,
            &remote_credential,
            path.clone(),
            "register",
            json!({"id":"leak","mode":"internal"}),
        )
        .await;
        assert_eq!(status, 401, "{path}: {body}");
    }
    let (_, remote_hubs) = route(&remote_base, REMOTE, json!(["acme"]), "list", json!({})).await;
    assert_eq!(remote_hubs["hubs"], json!([]));
    // A hosted hub on the front service may still manage a remote central it registered itself.
    let (status, body) = send(&front_base, "POST", "/v1/hubs", TOKEN,
        Some(json!({"id":"mine","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":true}}))).await;
    assert_eq!(status, 201, "{body}");
    let front_credential = body["credential"].as_str().unwrap().to_owned();
    let (status, body) = child(
        &front_base,
        &front_credential,
        json!(["mine"]),
        json!({"id":"far","mode":"central","address":remote_base,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, _) = route(
        &front_base,
        &front_credential,
        json!(["mine", "far"]),
        "list",
        json!({}),
    )
    .await;
    assert_eq!(status, 200);
    // But it cannot name a sibling of `mine` that is a remote central.
    let (status, _) = route(
        &front_base,
        &front_credential,
        json!(["r"]),
        "list",
        json!({}),
    )
    .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn hosted_hub_state_is_not_shared_between_siblings_in_the_registry_views() {
    let (_service, base) = node(TOKEN).await;
    let acme = hosted(&base, "acme").await;
    let beta = hosted(&base, "beta").await;
    for (credential, name) in [(&acme, "acme"), (&beta, "beta")] {
        let (status, _) = child(
            &base,
            credential,
            json!([name]),
            json!({"id":format!("{name}-game"),"mode":"internal"}),
        )
        .await;
        assert_eq!(status, 201);
    }
    // A tree of the hub shows only its subtree.
    let (status, tree) = route(&base, &acme, json!(["acme"]), "tree", json!({"depth":3})).await;
    assert_eq!(status, 200, "{tree}");
    let text = tree.to_string();
    assert!(text.contains("acme-game"), "{text}");
    assert!(!text.contains("beta"), "{text}");
    let (status, found) = route(
        &base,
        &acme,
        json!(["acme"]),
        "discover",
        json!({"kind":"hub","depth":3}),
    )
    .await;
    assert_eq!(status, 200, "{found}");
    assert!(!found.to_string().contains("beta"), "{found}");
}

#[tokio::test]
async fn hosted_policy_defaults_to_local_and_cannot_be_widened() {
    let (_service, base) = node(TOKEN).await;
    let credential = hosted(&base, "tenant").await;
    for body in [
        json!({"id":"external","mode":"external","address":"http://127.0.0.1:1","remoteToken":REMOTE}),
        json!({"id":"remote","mode":"central","address":"http://127.0.0.1:1","remoteToken":REMOTE}),
        json!({"id":"webhook","mode":"internal","auth":{"kind":"webhook","url":"http://127.0.0.1:1"}}),
        json!({"id":"nested","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":false}}),
        json!({"id":"nested","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":true}}),
    ] {
        let (status, answer) = child(&base, &credential, json!(["tenant"]), body.clone()).await;
        assert_eq!(status, 403, "{body}: {answer}");
    }
    let (status, answer) = route(&base, TOKEN, json!(["tenant"]), "register", json!({"id":"nested","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":true}})).await;
    assert_eq!(status, 403, "{answer}");
    for body in [
        json!({"id":"local","mode":"internal"}),
        json!({"id":"central","mode":"central"}),
    ] {
        let (status, answer) = child(&base, &credential, json!(["tenant"]), body).await;
        assert_eq!(status, 201, "{answer}");
    }
}

#[tokio::test]
async fn hosted_opt_in_and_subtree_quota_survive_restart() {
    let path = std::env::temp_dir().join(format!(
        "hosted-policy-{}-{}.json",
        std::process::id(),
        rand::random::<u64>()
    ));
    let (_remote, remote) = node(REMOTE).await;
    let mut cfg = config(TOKEN);
    cfg.state_file = Some(path.clone());
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let base = serve(&service).await;
    let (status, answer) = send(&base, "POST", "/v1/hubs", TOKEN, Some(json!({"id":"tenant","mode":"central","hosting":"provider","maxHubs":3,"hostedPolicy":{"allowRemote":true}}))).await;
    assert_eq!(status, 201, "{answer}");
    let credential = answer["credential"].as_str().unwrap();
    let (status, answer) = child(
        &base,
        credential,
        json!(["tenant"]),
        json!({"id":"nested","mode":"central","maxHubs":20}),
    )
    .await;
    assert_eq!(status, 201, "{answer}");
    let restarted = HubService::new(cfg).unwrap();
    let base2 = serve(&restarted).await;
    let (status, answer) = child(
        &base2,
        credential,
        json!(["tenant", "nested"]),
        json!({"id":"remote","mode":"central","address":remote,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{answer}");
    let (status, answer) = child(
        &base2,
        credential,
        json!(["tenant", "nested"]),
        json!({"id":"local","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 201, "{answer}");
    let (status, answer) = child(
        &base2,
        credential,
        json!(["tenant"]),
        json!({"id":"over","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 507, "{answer}");
    assert_eq!(answer["error"], "hub registry is full");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn opted_in_policy_allows_remote_hubs_and_webhook_and_can_be_narrowed() {
    let (_remote, remote) = node(REMOTE).await;
    let (status, answer) = send(
        &remote,
        "POST",
        "/v1/hubs",
        REMOTE,
        Some(json!({"id":"game","mode":"internal"})),
    )
    .await;
    assert_eq!(status, 201, "{answer}");
    let (_front, base) = node(TOKEN).await;
    let (status, answer) = send(&base, "POST", "/v1/hubs", TOKEN, Some(json!({"id":"tenant","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":true}}))).await;
    assert_eq!(status, 201, "{answer}");
    let credential = answer["credential"].as_str().unwrap();
    for body in [
        json!({"id":"external","mode":"external","address":remote,"remoteHubId":"game","remoteToken":REMOTE}),
        json!({"id":"webhook","mode":"internal","auth":{"kind":"webhook","url":format!("{remote}/verify")}}),
    ] {
        let (status, answer) = child(&base, credential, json!(["tenant"]), body).await;
        assert_eq!(status, 201, "{answer}");
    }
    let (status, answer) = route(&base, TOKEN, json!(["tenant"]), "register", json!({"id":"narrow","mode":"central","hosting":"provider","hostedPolicy":{"allowRemote":false}})).await;
    assert_eq!(status, 201, "{answer}");
    let nested = answer["credential"].as_str().unwrap();
    for token in [credential, nested] {
        let (status, answer) = child(&base, token, json!(["tenant","narrow"]), json!({"id":"webhook","mode":"internal","auth":{"kind":"webhook","url":format!("{remote}/verify")}})).await;
        assert_eq!(status, 403, "{answer}");
    }
}
