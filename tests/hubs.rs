#![cfg(feature = "host")]

use std::{net::SocketAddr, time::Duration};

use axum::Router;
use liveengine::hub::{HubConfig, HubHandler, HubMode, HubRegistration, HubService, HubSpec};
use serde_json::{json, Value};
use tower::ServiceExt;

const TOKEN: &str = "test-administrator-token-long-enough";
const REMOTE_TOKEN: &str = "different-remote-administrator-token";

#[test]
fn concurrent_instance_id_child() {
    if std::env::var_os("LIVEENGINE_TEST_INSTANCE_CHILD").is_some() {
        let id = HubConfig::from_env().unwrap().instance_id.unwrap();
        println!("CONCURRENT_INSTANCE_ID={id}");
    }
}

#[test]
fn concurrent_instance_id_file_creators_converge() {
    use std::sync::{Arc, Barrier};

    let dir = std::env::temp_dir().join(format!(
        "liveengine-concurrent-id-{}-{:032x}",
        std::process::id(),
        rand::random::<u128>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = Arc::new(dir.join("instance-id"));
    let barrier = Arc::new(Barrier::new(16));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "concurrent_instance_id_child", "--nocapture"])
                    .env("LIVEENGINE_TEST_INSTANCE_CHILD", "1")
                    .env("LIVEENGINE_TOKEN", TOKEN)
                    .env("LIVEENGINE_INSTANCE_ID_FILE", path.as_ref())
                    .env_remove("LIVEENGINE_INSTANCE_ID")
                    .env_remove("LIVEENGINE_STATE_FILE")
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let stdout = String::from_utf8(output.stdout).unwrap();
                stdout
                    .lines()
                    .find_map(|line| {
                        line.split_once("CONCURRENT_INSTANCE_ID=")
                            .map(|(_, id)| id.to_owned())
                    })
                    .unwrap()
            })
        })
        .collect();
    let ids: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert!(ids.iter().all(|id| id == &ids[0]));
    assert_eq!(std::fs::read_to_string(path.as_ref()).unwrap(), ids[0]);
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn typed_hub_spec_uses_http_registration_rules() {
    let service = HubService::new(config()).unwrap();
    let app = service.router();
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/schemas",
            Some(json!({
                "id":"typed", "version":"1", "schema":{"type":"object"}
            })),
            true
        )
        .await
        .0,
        201
    );
    let spec = HubSpec {
        id: "typed-hub".into(),
        schema_ref: Some("typed@1".into()),
        user_schema: Some(json!({"type":"object"})),
        server_schema: Some(json!({"type":"object"})),
        if_absent: true,
        ..HubSpec::default()
    };
    let info = service.register(spec.clone()).await.unwrap();
    assert_eq!(info.schema_ref.as_deref(), Some("typed@1"));
    assert_eq!(info.user_schema, spec.user_schema);
    assert_eq!(info.server_schema, spec.server_schema);
    assert_eq!(service.register(spec).await.unwrap().id, "typed-hub");
    let (status, fetched) = request(app, "GET", "/v1/hubs/typed-hub", None, true).await;
    assert_eq!(status, 200);
    assert_eq!(fetched["schemaRef"], "typed@1");

    assert_eq!(
        request(
            service.router(),
            "POST",
            "/v1/templates",
            Some(json!({
                "name":"central-template", "mode":"central"
            })),
            true
        )
        .await
        .0,
        201
    );
    let central = service
        .register(HubSpec {
            id: "typed-central".into(),
            template: Some("central-template".into()),
            max_hubs: Some(2),
            ..HubSpec::default()
        })
        .await
        .unwrap();
    assert_eq!(central.mode, HubMode::Central);
    assert_eq!(central.max_hubs, Some(2));
}

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
    // The permit is held by the timed-out worker, so a caller must not queue past the test.
    cfg.call_queue = Duration::from_millis(20);
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
async fn unwritable_state_directory_rejects_initial_snapshot() {
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!(
        "liveengine-unwritable-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let service = HubService::new(HubConfig {
        state_file: Some(dir.join("state.json")),
        ..config()
    })
    .unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = service.initialize_state().await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn failed_state_rename_removes_temporary_file() {
    let dir = std::env::temp_dir().join(format!(
        "liveengine-rename-failure-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let service = HubService::new(HubConfig {
        state_file: Some(path.clone()),
        ..config()
    })
    .unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(service.initialize_state().await.is_err());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(dir).unwrap();
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
    // A refresh may still be writing its snapshot, so cleanup is best-effort.
    let _ = std::fs::remove_dir_all(dir);
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

#[tokio::test]
async fn rust_client_routes_and_exports_over_tcp() {
    let service = HubService::new(config()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            service
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = liveengine::hub::HubClient::new(&format!("http://{address}"), TOKEN).unwrap();
    client
        .route(&[], "register", json!({"id":"region","mode":"central"}))
        .await
        .unwrap();
    client
        .route(
            &["region".into()],
            "register",
            json!({"id":"game","mode":"internal"}),
        )
        .await
        .unwrap();
    let result = client
        .route(
            &["region".into()],
            "call",
            json!({"id":"game","method":"stats","args":[]}),
        )
        .await
        .unwrap();
    assert_eq!(result["result"]["users"], 0);
    assert_eq!(
        client.tree(2).await.unwrap()["hubs"][0]["hub"]["id"],
        "region"
    );
    assert!(client
        .export_registry()
        .await
        .unwrap()
        .get("hubs")
        .is_some());
    task.abort();
}

#[tokio::test]
async fn list_and_export_respect_response_size_limit() {
    let service = HubService::new(HubConfig {
        max_response_bytes: 30,
        ..config()
    })
    .unwrap();
    let app = service.router();
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs",
            Some(json!({"id":"main","mode":"internal"})),
            true
        )
        .await
        .0,
        201
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/templates",
            Some(json!({"name":"test","mode":"internal"})),
            true
        )
        .await
        .0,
        201
    );
    for path in [
        "/v1/schemas",
        "/v1/templates",
        "/v1/hubs",
        "/v1/registry/export",
    ] {
        let (status, body) = request(app.clone(), "GET", path, None, true).await;
        assert_eq!(status, 413, "{path}: {body}");
        assert!(body["error"].as_str().unwrap().contains("response"));
    }
    let (status, _) = request(
        app,
        "POST",
        "/v1/route",
        Some(json!({"path":[],"op":"list"})),
        true,
    )
    .await;
    assert_eq!(status, 413);
}

#[cfg(unix)]
#[tokio::test]
async fn rollback_failed_definition_writes_and_retry() {
    use std::os::unix::fs::PermissionsExt;
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        == b"0\n"
    {
        eprintln!("skipping rollback permission test as root");
        return;
    }
    let dir = std::env::temp_dir().join(format!(
        "liveengine-rollback-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let cfg = HubConfig {
        state_file: Some(dir.join("state.json")),
        ..config()
    };
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let app = service.router();
    let cases = [
        (
            "/v1/hubs",
            "/v1/hubs/rollback-hub",
            json!({"id":"rollback-hub","mode":"internal"}),
        ),
        (
            "/v1/schemas",
            "/v1/schemas/rollback@1",
            json!({"id":"rollback","version":"1","schema":{"type":"object"}}),
        ),
        (
            "/v1/templates",
            "/v1/templates/rollback",
            json!({"name":"rollback","mode":"internal"}),
        ),
    ];
    for (collection, item, body) in cases {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert_eq!(
            request(app.clone(), "POST", collection, Some(body.clone()), true)
                .await
                .0,
            500
        );
        assert_eq!(request(app.clone(), "GET", item, None, true).await.0, 404);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            request(app.clone(), "POST", collection, Some(body), true)
                .await
                .0,
            201
        );
        let restarted = HubService::new(cfg.clone()).unwrap().router();
        assert_eq!(request(restarted, "GET", item, None, true).await.0, 200);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert_eq!(
            request(app.clone(), "DELETE", item, None, true).await.0,
            500
        );
        assert_eq!(request(app.clone(), "GET", item, None, true).await.0, 200);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            request(app.clone(), "DELETE", item, None, true).await.0,
            204
        );
        let restarted = HubService::new(cfg.clone()).unwrap().router();
        assert_eq!(request(restarted, "GET", item, None, true).await.0, 404);
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rollback_nested_route_write() {
    use std::os::unix::fs::PermissionsExt;
    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        == b"0\n"
    {
        eprintln!("skipping rollback permission test as root");
        return;
    }
    let dir = std::env::temp_dir().join(format!(
        "liveengine-rollback-nested-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let cfg = HubConfig {
        state_file: Some(dir.join("state.json")),
        ..config()
    };
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let app = service.router();
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/v1/hubs",
            Some(json!({"id":"parent","mode":"central"})),
            true
        )
        .await
        .0,
        201
    );
    let create = json!({"path":["parent"],"op":"register","body":{"id":"child","mode":"internal"}});
    let get = json!({"path":["parent"],"op":"get","body":{"id":"child"}});
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert_eq!(
        request(app.clone(), "POST", "/v1/route", Some(create.clone()), true)
            .await
            .0,
        500
    );
    assert_eq!(
        request(app.clone(), "POST", "/v1/route", Some(get.clone()), true)
            .await
            .0,
        404
    );
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        request(app.clone(), "POST", "/v1/route", Some(create), true)
            .await
            .0,
        201
    );
    let restarted = HubService::new(cfg).unwrap().router();
    assert_eq!(
        request(restarted, "POST", "/v1/route", Some(get), true)
            .await
            .0,
        200
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rollback_failed_public_internal_registration() {
    use std::os::unix::fs::PermissionsExt;

    if std::process::Command::new("id")
        .arg("-u")
        .output()
        .unwrap()
        .stdout
        == b"0\n"
    {
        eprintln!("skipping rollback permission test as root");
        return;
    }
    let dir = std::env::temp_dir().join(format!(
        "liveengine-rollback-public-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let cfg = HubConfig {
        state_file: Some(dir.join("state.json")),
        ..config()
    };
    let service = HubService::new(cfg.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let registration = HubRegistration {
        id: "public-rollback".into(),
        mode: HubMode::Internal,
        address: None,
        remote_token: None,
        remote_hub_id: None,
        scopes: json!({}),
        scope_schema: None,
        ttl_seconds: None,
    };
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(service
        .register_internal(registration.clone(), EchoHandler)
        .await
        .is_err());
    assert_eq!(
        request(
            service.router(),
            "GET",
            "/v1/hubs/public-rollback",
            None,
            true
        )
        .await
        .0,
        404
    );
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    service
        .register_internal(registration, EchoHandler)
        .await
        .unwrap();
    let restarted = HubService::new(cfg).unwrap().router();
    assert_eq!(
        request(restarted, "GET", "/v1/hubs/public-rollback", None, true)
            .await
            .0,
        200
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn concurrent_calls_to_one_hub_queue_instead_of_failing() {
    // Default limits: 16 workers, and 64 callers at once on a single hub.
    let service = HubService::new(config()).unwrap();
    let app = service.router();
    let (status, body) = request(
        app.clone(),
        "POST",
        "/v1/hubs",
        Some(json!({"id":"busy","mode":"internal"})),
        true,
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let calls: Vec<_> = (0..64)
        .map(|n| {
            tokio::spawn(request(
                app.clone(),
                "POST",
                "/v1/hubs/busy/call",
                Some(json!({"method":"upsertUser","args":[{"id":format!("u{n}"),"scopes":{}}]})),
                true,
            ))
        })
        .collect();
    for call in calls {
        let (status, body) = call.await.unwrap();
        assert_eq!(status, 200, "{body}");
    }
    let (_, stats) = request(
        app,
        "POST",
        "/v1/hubs/busy/call",
        Some(json!({"method":"listUsers","args":[]})),
        true,
    )
    .await;
    assert_eq!(stats["result"].as_array().unwrap().len(), 64);
}

#[tokio::test]
async fn a_stuck_handler_refuses_callers_at_the_queue_deadline() {
    let mut cfg = config();
    cfg.call_queue = Duration::from_millis(300);
    let service = HubService::new(cfg).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let registration = HubRegistration {
        id: "stuck".into(),
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
    let call = || {
        request(
            app.clone(),
            "POST",
            "/v1/hubs/stuck/call",
            Some(json!({"method":"block","args":[]})),
            true,
        )
    };
    let first = tokio::spawn(call());
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let (status, body) = call().await;
    let waited = started.elapsed();
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "hub handler is busy");
    assert!(waited >= Duration::from_millis(250), "{waited:?}");
    assert!(waited < Duration::from_millis(1500), "{waited:?}");
    // Other hubs and the service keep answering while one handler is stuck.
    assert_eq!(
        request(app.clone(), "GET", "/health", None, false).await.0,
        200
    );
    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap().0, 200);
}

#[test]
fn malformed_live_limits_fail_at_startup() {
    for name in [
        "LIVEENGINE_LIVE_MAX_CONNECTIONS",
        "LIVEENGINE_LIVE_MAX_CONNECTIONS_PER_ROOM",
        "LIVEENGINE_LIVE_MAX_FRAME_BYTES",
        "LIVEENGINE_LIVE_SEND_QUEUE",
        "LIVEENGINE_LIVE_IDLE_SECONDS",
        "LIVEENGINE_LIVE_PING_SECONDS",
        "LIVEENGINE_LIVE_MESSAGES_PER_SECOND",
        "LIVEENGINE_LIVE_BYTES_PER_SECOND",
        "LIVEENGINE_LIVE_RESUME_MILLIS",
        "LIVEENGINE_SIGNAL_MAX_BYTES",
        "LIVEENGINE_LINK_MAX_MESSAGES_PER_SECOND",
        "LIVEENGINE_LINK_MAX_BYTES_PER_SECOND",
        "LIVEENGINE_LIVE_REGISTRY_MAX_SUBSCRIBERS",
        "LIVEENGINE_LIVE_TREE_DEPTH",
    ] {
        for value in ["0", "bad"] {
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_liveengine-hub"))
                .env("LIVEENGINE_TOKEN", TOKEN)
                .env("LIVEENGINE_INSTANCE_ID", "test-instance")
                .env("LIVEENGINE_BIND", "127.0.0.1:0")
                .env(name, value)
                .output()
                .unwrap();
            assert!(!output.status.success(), "{name}={value}");
            assert!(!output.stderr.is_empty(), "{name}={value}");
        }
    }
}
