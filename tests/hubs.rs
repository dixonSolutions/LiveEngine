#![cfg(feature = "host")]

use std::{net::SocketAddr, time::Duration};

use axum::Router;
use liveengine::hub::{HubConfig, HubHandler, HubMode, HubRegistration, HubService};
use serde_json::{json, Value};
use tower::ServiceExt;

const TOKEN: &str = "test-administrator-token-long-enough";
const REMOTE_TOKEN: &str = "different-remote-administrator-token";

fn config() -> HubConfig {
    HubConfig {
        token: TOKEN.into(),
        ..HubConfig::default()
    }
}

async fn request(
    app: Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    token: bool,
) -> (u16, Value) {
    request_auth(
        app,
        method,
        uri,
        body,
        if token { Some(TOKEN) } else { None },
    )
    .await
}

async fn request_auth(
    app: Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    token: Option<&str>,
) -> (u16, Value) {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let request = builder
        .body(axum::body::Body::from(
            body.map(|v| v.to_string()).unwrap_or_default(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn internal_hub_auth_and_calls_share_the_protocol() {
    let service = HubService::new(config()).unwrap();
    let app = service.router();
    assert_eq!(
        request(app.clone(), "GET", "/v1/hubs", None, false).await.0,
        401
    );
    let (status, created) = request(
        app.clone(),
        "POST",
        "/v1/hubs",
        Some(json!({"id":"main","mode":"internal","scopes":{"region":"au"}})),
        true,
    )
    .await;
    assert_eq!(status, 201, "{created}");
    let (status, inserted) = request(
        app.clone(),
        "POST",
        "/v1/hubs/main/call",
        Some(json!({"method":"upsertUser","args":[{"id":"u1","scopes":{"region":"au"}}]})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{inserted}");
    assert_eq!(inserted["result"], true);
    let (status, fetched) = request(
        app,
        "POST",
        "/v1/hubs/main/call",
        Some(json!({"method":"getUser","args":["u1"]})),
        true,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["result"]["id"], "u1");
}

#[tokio::test]
async fn external_hubs_handshake_and_forward_calls() {
    let remote = HubService::new(HubConfig {
        token: REMOTE_TOKEN.into(),
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_addr: SocketAddr = listener.local_addr().unwrap();
    let remote_app = remote.router();
    tokio::spawn(async move {
        axum::serve(
            listener,
            remote_app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{remote_addr}");
    let auth = format!("Bearer {REMOTE_TOKEN}");
    let response = client
        .post(format!("{base}/v1/hubs"))
        .header("authorization", &auth)
        .json(&json!({"id":"remote","mode":"internal"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let response = client
        .post(format!("{base}/v1/hubs/remote/call"))
        .header("authorization", &auth)
        .json(&json!({"method":"upsertUser","args":[{"id":"forwarded"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let mut local_config = config();
    local_config.allowed_remote_hosts = vec!["127.0.0.1".into()];
    let local = HubService::new(local_config).unwrap();
    let app = local.router();
    let (status, body) = request(app.clone(), "POST", "/v1/hubs", Some(json!({"id":"alias","mode":"external","address":base,"remoteHubId":"remote","remoteToken":REMOTE_TOKEN})), true).await;
    assert_eq!(status, 201, "{body}");
    let (status, result) = request(
        app,
        "POST",
        "/v1/hubs/alias/call",
        Some(json!({"method":"getUser","args":["forwarded"]})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["result"]["id"], "forwarded");

    let mut import_config = config();
    import_config.allowed_remote_hosts = vec!["127.0.0.1".into()];
    let importer = HubService::new(import_config).unwrap();
    let importer_app = importer.router();
    let (status, imported) = request(
        importer_app.clone(),
        "POST",
        "/v1/registries/import",
        Some(json!({"address":base,"token":REMOTE_TOKEN,"prefix":"copy-"})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{imported}");
    assert_eq!(imported["hubs"][0]["id"], "copy-remote");
    assert!(
        imported.to_string().find(REMOTE_TOKEN).is_none(),
        "remote token leaked in import response"
    );
    let (status, hub_list) = request(importer_app.clone(), "GET", "/v1/hubs", None, true).await;
    assert_eq!(status, 200);
    assert!(
        hub_list.to_string().find(REMOTE_TOKEN).is_none(),
        "remote token leaked from GET /v1/hubs"
    );
    let (status, copied) = request(
        importer_app.clone(),
        "POST",
        "/v1/hubs/copy-remote/call",
        Some(json!({"method":"getUser","args":["forwarded"]})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{copied}");
    assert_eq!(copied["result"]["id"], "forwarded");
    // An import after startup discovers newly registered remote hubs and is idempotent.
    let response = client
        .post(format!("{base}/v1/hubs"))
        .header("authorization", &auth)
        .json(&json!({"id":"remote2","mode":"internal"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let (status, updated) = request(
        importer_app.clone(),
        "POST",
        "/v1/registries/import",
        Some(json!({"address":base,"token":REMOTE_TOKEN,"prefix":"copy-"})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{updated}");
    assert_eq!(updated["hubs"].as_array().unwrap().len(), 2);
    let (status, list) = request(importer_app, "GET", "/v1/hubs", None, true).await;
    assert_eq!(status, 200);
    assert_eq!(list["hubs"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn rejects_bad_routes_ssrf_and_expired_hubs() {
    let mut cfg = config();
    cfg.max_ttl = Duration::from_secs(3);
    let service = HubService::new(cfg).unwrap();
    let app = service.router();
    assert_eq!(
        request(app.clone(), "GET", "/v1/no-such-route", None, true)
            .await
            .0,
        404
    );
    let (status, _) = request(app.clone(), "POST", "/v1/hubs", Some(json!({"id":"nope","mode":"external","address":"http://127.0.0.1:1","remoteToken":TOKEN})), true).await;
    assert_eq!(
        status, 400,
        "host must be rejected before attempting a connection"
    );
    let (status, _) = request(
        app.clone(),
        "POST",
        "/v1/hubs",
        Some(json!({"id":"short","mode":"internal","ttlSeconds":1})),
        true,
    )
    .await;
    assert_eq!(status, 201);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        request(app, "GET", "/v1/hubs/short", None, true).await.0,
        404
    );
}

#[tokio::test]
async fn rate_limit_uses_bounded_peer_bucket() {
    let mut cfg = config();
    cfg.rate_limit_requests = 1;
    cfg.rate_limit_window = Duration::from_secs(10);
    let service = HubService::new(cfg).unwrap();
    let app = service.router();
    assert_eq!(
        request(app.clone(), "GET", "/v1/hubs", None, false).await.0,
        401
    );
    assert_eq!(request(app, "GET", "/v1/hubs", None, false).await.0, 429);
}

struct EchoHandler;

impl HubHandler for EchoHandler {
    fn call(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String> {
        Ok(json!({"method":method,"args":args}))
    }
}

struct BlockingHandler {
    started: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl HubHandler for BlockingHandler {
    fn call(&mut self, _method: &str, _args: Vec<Value>) -> Result<Value, String> {
        let _ = self.started.send(());
        let _ = self.release.recv();
        Ok(json!("finished"))
    }
}

#[tokio::test]
async fn custom_internal_handlers_share_http_limits_and_schema_validation() {
    let service = HubService::new(config()).unwrap();
    let registration = HubRegistration {
        id: "custom".into(),
        mode: HubMode::Internal,
        address: None,
        remote_token: None,
        remote_hub_id: None,
        scopes: json!({"region":"au"}),
        scope_schema: None,
        ttl_seconds: None,
    };
    service
        .register_internal(registration, EchoHandler)
        .await
        .unwrap();
    let (status, result) = request(
        service.router(),
        "POST",
        "/v1/hubs/custom/call",
        Some(json!({"method":"echo","args":[1,"x"]})),
        true,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(result["result"], json!({"method":"echo","args":[1,"x"]}));

    let service = HubService::new(config()).unwrap();
    let app = service.router();
    let malformed = json!({"id":"bad-schema","mode":"internal","scopes":{},"scopeSchema":{"required":"region"}});
    assert_eq!(
        request(app.clone(), "POST", "/v1/hubs", Some(malformed), true)
            .await
            .0,
        400
    );
    let schema = json!({"type":"object","required":["region"],"properties":{"region":{"type":"string"}},"additionalProperties":false});
    let (status, body) = request(app.clone(), "POST", "/v1/hubs", Some(json!({"id":"validated","mode":"internal","scopes":{"region":"au"},"scopeSchema":schema})), true).await;
    assert_eq!(status, 201, "{body}");
    let (status, _) = request(
        app.clone(),
        "POST",
        "/v1/hubs/validated/call",
        Some(json!({"method":"upsertUser","args":[{"id":"invalid","scopes":{"region":3}}]})),
        true,
    )
    .await;
    assert_eq!(status, 400);
    let (status, absent) = request(
        app.clone(),
        "POST",
        "/v1/hubs/validated/call",
        Some(json!({"method":"getUser","args":["invalid"]})),
        true,
    )
    .await;
    assert_eq!(status, 200);
    assert!(absent["result"].is_null());
    let (status, _) = request(app, "POST", "/v1/hubs/validated/call", Some(json!({"method":"upsertUser","args":[{"id":"unexpected","scopes":{"region":"au","extra":true}}]})), true).await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn event_budget_rejects_mutation_before_it_changes_engine_state() {
    let mut cfg = config();
    cfg.max_events = 3;
    let app = HubService::new(cfg).unwrap().router();
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs",
            Some(json!({"id":"events","mode":"internal"})),
            true
        )
        .await
        .0,
        201
    );
    let insert = json!({"method":"upsertUser","args":[{"id":"first"}]});
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs/events/call",
            Some(insert),
            true
        )
        .await
        .0,
        200
    );
    let insert_second = json!({"method":"upsertUser","args":[{"id":"second"}]});
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs/events/call",
            Some(insert_second),
            true
        )
        .await
        .0,
        413
    );
    let (status, absent) = request(
        app,
        "POST",
        "/v1/hubs/events/call",
        Some(json!({"method":"getUser","args":["second"]})),
        true,
    )
    .await;
    assert_eq!(status, 200);
    assert!(absent["result"].is_null());
}

#[tokio::test]
async fn timed_out_blocking_handler_keeps_its_global_permit_until_finished() {
    let mut cfg = config();
    cfg.request_timeout = Duration::from_millis(100);
    cfg.max_inflight_calls = 1;
    let service = HubService::new(cfg).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let registration = HubRegistration {
        id: "slow".into(),
        mode: HubMode::Internal,
        address: None,
        remote_token: None,
        remote_hub_id: None,
        scopes: json!({}),
        scope_schema: None,
        ttl_seconds: None,
    };
    service
        .register_internal(
            registration,
            BlockingHandler {
                started: started_tx,
                release: release_rx,
            },
        )
        .await
        .unwrap();

    let app = service.router();
    let first = tokio::spawn(request(
        app.clone(),
        "POST",
        "/v1/hubs/slow/call",
        Some(json!({"method":"block","args":[]})),
        true,
    ));
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();

    let (status, _) = first.await.unwrap();
    assert_eq!(
        status, 504,
        "the route should time out while its worker continues"
    );
    assert_eq!(
        request(app.clone(), "GET", "/health", None, false).await.0,
        200
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs/slow/call",
            Some(json!({"method":"block","args":[]})),
            true
        )
        .await
        .0,
        503,
        "the timed-out worker must continue holding its permit"
    );

    release_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (status, response) = request(
        app,
        "POST",
        "/v1/hubs/slow/call",
        Some(json!({"method":"block","args":[]})),
        true,
    )
    .await;
    assert_eq!(
        status, 504,
        "the handler's second wait should also be bounded"
    );
    assert_eq!(response["error"], "request exceeded configured timeout");
}
