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
    let (status, _) = request(
        app.clone(),
        "POST",
        "/v1/hubs/validated/call",
        Some(json!({"method":"generateUsers","args":[1]})),
        true,
    )
    .await;
    assert_eq!(
        status, 400,
        "generation must not bypass the hub scope schema"
    );
    let (_, stats) = request(
        app.clone(),
        "POST",
        "/v1/hubs/validated/call",
        Some(json!({"method":"stats"})),
        true,
    )
    .await;
    assert_eq!(stats["result"]["users"], 0);
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

#[tokio::test]
async fn new_peers_cannot_evict_and_reset_an_active_rate_budget() {
    let service = HubService::new(HubConfig {
        rate_limit_peers: 1,
        rate_limit_requests: 1,
        ..config()
    })
    .unwrap();
    let app = service.router();
    for (ip, expected) in [
        ([127, 0, 0, 1], 200),
        ([127, 0, 0, 2], 429),
        ([127, 0, 0, 1], 429),
    ] {
        let mut request = axum::http::Request::builder()
            .uri("/v1/hubs")
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(axum::body::Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(SocketAddr::from((ip, 1234))));
        assert_eq!(
            app.clone()
                .oneshot(request)
                .await
                .unwrap()
                .status()
                .as_u16(),
            expected
        );
    }
}

#[tokio::test]
async fn rejects_hub_ids_that_normalize_as_url_path_segments() {
    let app = HubService::new(config()).unwrap().router();
    for id in [".", "..", " padded "] {
        let (status, _) = request(
            app.clone(),
            "POST",
            "/v1/hubs",
            Some(json!({"id":id,"mode":"internal"})),
            true,
        )
        .await;
        assert_eq!(status, 400, "unaddressable hub id must be rejected: {id}");
    }
    let (_, list) = request(app, "GET", "/v1/hubs", None, true).await;
    assert_eq!(list["hubs"], json!([]));
}

#[tokio::test]
async fn ping_endpoint_is_unauthenticated_and_native_ping_reports_stats() {
    let service = HubService::new(config()).unwrap();
    let (status, value) = request(service.router(), "GET", "/v1/ping", None, false).await;
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["protocol"], "liveengine.hub/v1");
    assert_eq!(value["pong"], true);
    assert!(value["processingMicros"].is_u64());
    assert!(value["serverTimeMs"].is_u64());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let app = service.router();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let stats = liveengine::hub::ping_hub(&format!("http://{address}"), 3, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(stats.samples.len(), 3);
    assert!(stats.avg_ms >= 0.0);
    assert!(stats.min_ms <= stats.avg_ms);
    assert!(stats.max_ms >= stats.avg_ms);
    assert!(stats.server_time_ms.is_some());
}

#[cfg(unix)]
#[tokio::test]
async fn durable_registry_restart_and_invalid_files() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!(
        "liveengine-durable-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let cfg = HubConfig {
        state_file: Some(path.clone()),
        ..config()
    };
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, service.router()).await.unwrap() });
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    let post = |uri: &str, body: Value| {
        client
            .post(format!("{base}{uri}"))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
    };
    assert_eq!(
        post(
            "/v1/schemas",
            json!({"id":"saved","version":"1","schema":{"type":"object"}})
        )
        .await
        .unwrap()
        .status(),
        201
    );
    assert_eq!(
        post("/v1/templates", json!({"name":"saved","mode":"internal"}))
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        post("/v1/hubs", json!({"id":"parent","mode":"central"}))
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(post("/v1/route", json!({"path":["parent"],"op":"register","body":{"id":"child","template":"saved","schemaRef":"saved@1"}})).await.unwrap().status(), 201);
    assert_eq!(post("/v1/route", json!({"path":["parent"],"op":"call","body":{"id":"child","method":"upsertUser","args":[{"id":"transient"}]}})).await.unwrap().status(), 200);
    task.abort();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let first_id: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let restored = HubService::new(cfg.clone()).unwrap();
    restored.initialize_state().await.unwrap();
    let second_id: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(first_id["instance_id"], second_id["instance_id"]);
    let app = restored.router();
    assert_eq!(
        request(app.clone(), "GET", "/v1/schemas/saved@1", None, true)
            .await
            .0,
        200
    );
    assert_eq!(
        request(app.clone(), "GET", "/v1/templates/saved", None, true)
            .await
            .0,
        200
    );
    let (status, info) = request(
        app.clone(),
        "POST",
        "/v1/route",
        Some(json!({"path":["parent"],"op":"get","body":{"id":"child"}})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{info}");
    let (_, result) = request(app, "POST", "/v1/route", Some(json!({"path":["parent"],"op":"call","body":{"id":"child","method":"getUser","args":["transient"]}})), true).await;
    assert!(result["result"].is_null(), "{result}");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(HubService::new(cfg.clone()).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&path, b"{corrupt").unwrap();
    assert!(HubService::new(cfg).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"{corrupt");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn refreshed_import_marks_missing_entries_stale_and_persists_credentials_privately() {
    let remote = HubService::new(HubConfig {
        token: REMOTE_TOKEN.into(),
        ..config()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote_app = remote.router();
    let task = tokio::spawn(async move { axum::serve(listener, remote_app).await.unwrap() });
    let client = reqwest::Client::new();
    let base = format!("http://{addr}");
    assert_eq!(
        client
            .post(format!("{base}/v1/hubs"))
            .bearer_auth(REMOTE_TOKEN)
            .json(&json!({"id":"source","mode":"internal"}))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let dir = std::env::temp_dir().join(format!(
        "liveengine-refresh-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let cfg = HubConfig {
        state_file: Some(path.clone()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..config()
    };
    let importer = HubService::new(cfg.clone()).unwrap();
    let app = importer.router();
    let (status, result) = request(
        app.clone(),
        "POST",
        "/v1/registries/import",
        Some(json!({"address":base,"token":REMOTE_TOKEN,"prefix":"copy-","refreshSeconds":1})),
        true,
    )
    .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(
        client
            .delete(format!("{base}/v1/hubs/source"))
            .bearer_auth(REMOTE_TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    let mut stale = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, value) = request(app.clone(), "GET", "/v1/hubs/copy-source", None, true).await;
        if value["stale"] == true {
            stale = true;
            break;
        }
    }
    assert!(stale);
    for uri in [
        "/v1/hubs",
        "/v1/hubs/copy-source",
        "/v1/tree",
        "/v1/registry/export",
        "/v1/templates",
    ] {
        let (_, value) = request(app.clone(), "GET", uri, None, true).await;
        assert!(!value.to_string().contains(REMOTE_TOKEN));
    }
    drop(app);
    drop(importer);
    let restored = HubService::new(cfg).unwrap();
    let restored_app = restored.router();
    let (_, value) = request(
        restored_app.clone(),
        "GET",
        "/v1/hubs/copy-source",
        None,
        true,
    )
    .await;
    assert_eq!(value["stale"], true);
    assert_eq!(
        client
            .post(format!("{base}/v1/hubs"))
            .bearer_auth(REMOTE_TOKEN)
            .json(&json!({"id":"after-restart","mode":"internal"}))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let mut rediscovered = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (status, _) = request(
            restored_app.clone(),
            "GET",
            "/v1/hubs/copy-after-restart",
            None,
            true,
        )
        .await;
        if status == 200 {
            rediscovered = true;
            break;
        }
    }
    assert!(
        rediscovered,
        "persisted import did not refresh after restart"
    );
    drop(restored_app);
    drop(restored);
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .contains(REMOTE_TOKEN));
    task.abort();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn standalone_state_file_environment_and_startup_errors() {
    use std::{
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        time::Duration,
    };
    let dir = std::env::temp_dir().join(format!(
        "liveengine-env-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let start = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_liveengine-hub"));
        command
            .env("LIVEENGINE_TOKEN", TOKEN)
            .env("LIVEENGINE_STATE_FILE", &path)
            .env("LIVEENGINE_BIND", "127.0.0.1:0")
            .env_remove("LIVEENGINE_INSTANCE_ID")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    };
    let mut child = start().spawn().unwrap();
    for _ in 0..100 {
        if path.exists() || child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let created = path.exists();
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        created,
        "standalone service did not create LIVEENGINE_STATE_FILE"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let first: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let mut child = start().spawn().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(child.try_wait().unwrap().is_none(), "restart failed");
    child.kill().unwrap();
    child.wait().unwrap();
    let second: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(first["instance_id"], second["instance_id"]);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let output = start().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("state file"));
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&path, b"{invalid").unwrap();
    let output = start().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid state file"));
    assert_eq!(std::fs::read(&path).unwrap(), b"{invalid");
    std::fs::remove_dir_all(dir).unwrap();
}
