#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::task::JoinHandle;
const TOKEN: &str = "nested-integration-secret-123456789";
async fn server(id: &str, depth: usize) -> (String, JoinHandle<()>) {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        instance_id: Some(id.into()),
        max_depth: depth,
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
    (base, task)
}
async fn post(base: &str, path: &str, body: Value) -> (u16, Value) {
    let response = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}
async fn get(base: &str, path: &str) -> (u16, Value) {
    let response = reqwest::Client::new()
        .get(format!("{base}{path}"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}
#[tokio::test]
async fn three_service_nest_loop_depth_tree_and_credentials() {
    let (a, ta) = server("nested-a", 3).await;
    let (b, tb) = server("nested-b", 3).await;
    let (c, tc) = server("nested-c", 3).await;
    assert_eq!(
        post(
            &a,
            "/v1/hubs",
            json!({"id":"b","mode":"central","address":b,"remoteToken":TOKEN})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &b,
            "/v1/hubs",
            json!({"id":"c","mode":"central","address":c,"remoteToken":TOKEN})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &c,
            "/v1/hubs",
            json!({"id":"a","mode":"central","address":a,"remoteToken":TOKEN})
        )
        .await
        .0,
        201
    );
    let route = |path: Value, op: &str, body: Value| json!({"path":path,"op":op,"body":body});
    assert_eq!(
        post(
            &a,
            "/v1/route",
            route(
                json!(["b", "c"]),
                "register",
                json!({"id":"game","mode":"internal"})
            )
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &a,
            "/v1/route",
            route(
                json!(["b", "c"]),
                "call",
                json!({"id":"game","method":"stats"})
            )
        )
        .await
        .0,
        200
    );
    assert_eq!(
        post(
            &a,
            "/v1/route",
            route(json!(["b", "c", "a"]), "list", json!({}))
        )
        .await
        .0,
        508
    );
    assert_eq!(
        post(
            &a,
            "/v1/route",
            route(json!(["b", "c", "a", "x"]), "list", json!({}))
        )
        .await
        .0,
        508
    );
    let (_, tree) = get(&a, "/v1/tree?depth=3").await;
    assert!(!tree.to_string().contains(TOKEN));
    assert_eq!(tree["hubs"][0]["hubs"][0]["hubs"][1]["hub"]["id"], "game");
    assert!(!get(&a, "/v1/hubs").await.1.to_string().contains(TOKEN));
    assert!(!get(&a, "/v1/hubs/b").await.1.to_string().contains(TOKEN));
    assert!(!get(&a, "/v1/registry/export")
        .await
        .1
        .to_string()
        .contains(TOKEN));
    assert!(
        !post(&a, "/v1/route", route(json!(["b"]), "list", json!({})))
            .await
            .1
            .to_string()
            .contains(TOKEN)
    );
    assert!(!post(
        &a,
        "/v1/route",
        route(json!(["b"]), "get", json!({"id":"c"}))
    )
    .await
    .1
    .to_string()
    .contains(TOKEN));
    tc.abort();
    let (_, tree) = get(&a, "/v1/tree?depth=3").await;
    assert_eq!(tree["hubs"][0]["hubs"][0]["status"], "error");
    ta.abort();
    tb.abort();
}
#[tokio::test]
async fn inherit_schema_across_remote_centrals_and_override() {
    let (a, ta) = server("inherit-a", 8).await;
    let (b, tb) = server("inherit-b", 8).await;
    let schema = json!({"type":"object","required":["rank"]});
    assert_eq!(
        post(
            &a,
            "/v1/hubs",
            json!({"id":"b","mode":"central","address":b,"remoteToken":TOKEN,"userSchema":schema})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(&b, "/v1/hubs", json!({"id":"game","mode":"internal"}))
            .await
            .0,
        201
    );
    assert_eq!(post(&b,"/v1/hubs",json!({"id":"override","mode":"internal","userSchema":{"type":"object","required":["region"]}})).await.0,201);
    let call = |id: &str, scopes: Value| json!({"path":["b"],"op":"call","body":{"id":id,"method":"upsertUser","args":[{"id":"u","scopes":scopes}]}});
    assert_eq!(post(&a, "/v1/route", call("game", json!({}))).await.0, 400);
    assert_eq!(
        post(&a, "/v1/route", call("game", json!({"rank":1})))
            .await
            .0,
        200
    );
    assert_eq!(
        post(&a, "/v1/route", call("override", json!({"region":"eu"})))
            .await
            .0,
        200
    );
    assert_eq!(
        post(&a, "/v1/route", call("override", json!({"rank":1})))
            .await
            .0,
        400
    );
    ta.abort();
    tb.abort();
}

#[tokio::test]
async fn local_depth_trace_and_remote_allowlist_are_bounded() {
    let (base, task) = server("depth-test", 2).await;
    let route = |path: Value, op: &str, body: Value| json!({"path":path,"op":op,"body":body});
    assert_eq!(
        post(&base, "/v1/hubs", json!({"id":"one","mode":"central"}))
            .await
            .0,
        201
    );
    assert_eq!(
        post(
            &base,
            "/v1/route",
            route(
                json!(["one"]),
                "register",
                json!({"id":"two","mode":"central"})
            )
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &base,
            "/v1/route",
            route(
                json!(["one", "two"]),
                "register",
                json!({"id":"three","mode":"central"})
            )
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &base,
            "/v1/route",
            route(json!(["one", "two", "three"]), "list", json!({}))
        )
        .await
        .0,
        508
    );
    assert_eq!(get(&base, "/v1/tree?depth=3").await.0, 400);
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{base}/v1/tree"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/tree"))
            .bearer_auth(TOKEN)
            .header("x-liveengine-trace", "depth-test")
            .send()
            .await
            .unwrap()
            .status(),
        508
    );
    assert_eq!(
        client
            .get(format!("{base}/v1/tree"))
            .bearer_auth(TOKEN)
            .header("x-liveengine-trace", "bad id")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        post(
            &base,
            "/v1/hubs",
            json!({"id":"bad","mode":"central","address":"http://example.com","remoteToken":TOKEN})
        )
        .await
        .0,
        400
    );
    task.abort();
}

#[tokio::test]
async fn nested_schema_references_share_registry_and_cannot_be_deleted_in_use() {
    let (base, task) = server("schema-tree", 8).await;
    assert_eq!(
        post(
            &base,
            "/v1/schemas",
            json!({"id":"nested","version":"1","schema":{"type":"object"}})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(&base, "/v1/hubs", json!({"id":"local","mode":"central"}))
            .await
            .0,
        201
    );
    assert_eq!(post(&base, "/v1/route", json!({"path":["local"],"op":"register","body":{"id":"game","mode":"internal","schemaRef":"nested@1"}})).await.0, 201);
    let response = reqwest::Client::new()
        .delete(format!("{base}/v1/schemas/nested@1"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    task.abort();
}

#[tokio::test]
async fn inherit_scope_schema_when_registering_through_remote() {
    let (a, ta) = server("registration-a", 8).await;
    let (b, tb) = server("registration-b", 8).await;
    assert_eq!(post(&a, "/v1/hubs", json!({"id":"b","mode":"central","address":b,"remoteToken":TOKEN,"scopeSchema":{"type":"object","required":["region"]},"scopes":{"region":"eu"}})).await.0, 201);
    let register = |body| json!({"path":["b"],"op":"register","body":body});
    assert_eq!(
        post(
            &a,
            "/v1/route",
            register(json!({"id":"invalid","mode":"internal"}))
        )
        .await
        .0,
        400
    );
    assert_eq!(
        post(
            &a,
            "/v1/route",
            register(json!({"id":"valid","mode":"internal","scopes":{"region":"eu"}}))
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &a,
            "/v1/route",
            register(json!({"id":"override","mode":"internal","scopeSchema":{"type":"object"}}))
        )
        .await
        .0,
        201
    );
    ta.abort();
    tb.abort();
}

#[test]
fn standalone_instance_identity_survives_restart() {
    use std::time::{Duration, Instant};
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("nested-instance-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_liveengine-hub"))
            .env("LIVEENGINE_TOKEN", TOKEN)
            .env("LIVEENGINE_BIND", "127.0.0.1:0")
            .env("LIVEENGINE_INSTANCE_ID_FILE", &path)
            .env_remove("LIVEENGINE_INSTANCE_ID")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    };
    let mut first = run();
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::read_to_string(&path).map_or(true, |id| id.len() != 32)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let original = std::fs::read_to_string(&path).unwrap();
    first.kill().unwrap();
    first.wait().unwrap();
    let mut second = run();
    std::thread::sleep(Duration::from_millis(100));
    assert!(second.try_wait().unwrap().is_none());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    second.kill().unwrap();
    second.wait().unwrap();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn tree_reports_a_timed_out_remote_without_failing_siblings() {
    use axum::{
        routing::{get as axum_get, post as axum_post},
        Json, Router,
    };
    let remote = Router::new()
        .route(
            "/health",
            axum_get(|| async { Json(json!({"protocol":"liveengine.hub/v1"})) }),
        )
        .route(
            "/v1/contract",
            axum_get(|| async { Json(json!({"protocol":"liveengine.hub/v1"})) }),
        )
        .route(
            "/v1/route",
            axum_post(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                Json(json!({"hubs":[]}))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let remote_task = tokio::spawn(async move {
        axum::serve(listener, remote).await.unwrap();
    });
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        request_timeout: std::time::Duration::from_millis(400),
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, service.router()).await.unwrap();
    });
    assert_eq!(
        post(
            &base,
            "/v1/hubs",
            json!({"id":"slow","mode":"central","address":address,"remoteToken":TOKEN})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(&base, "/v1/hubs", json!({"id":"healthy","mode":"internal"}))
            .await
            .0,
        201
    );
    let (status, tree) = get(&base, "/v1/tree?depth=1").await;
    assert_eq!(status, 200);
    assert_eq!(tree["hubs"][0]["status"], "ok");
    assert_eq!(tree["hubs"][1]["status"], "error");
    task.abort();
    remote_task.abort();
}
