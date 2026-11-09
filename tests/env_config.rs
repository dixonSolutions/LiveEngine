#![cfg(feature = "host")]
use futures_util::{SinkExt, StreamExt};
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "environment-test-admin-token-123456789";
const CREDENTIAL: &str = "environment-test-user-token-123456789";

#[tokio::test]
async fn environment_configuration_and_cli() {
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("LIVEENGINE_") {
            std::env::remove_var(name);
        }
    }
    std::env::set_var("LIVEENGINE_TOKEN", TOKEN);
    std::env::set_var("LIVEENGINE_INSTANCE_ID", "env-test-instance");
    type ConfigCase = (&'static str, u64, fn(&HubConfig) -> u64, bool);
    let cases: &[ConfigCase] = &[
        (
            "LIVEENGINE_CALL_QUEUE_MS",
            37,
            |c| c.call_queue.as_millis() as u64,
            true,
        ),
        ("LIVEENGINE_MAX_ARGS", 37, |c| c.max_args as u64, false),
        (
            "LIVEENGINE_MAX_BODY_BYTES",
            2048,
            |c| c.max_request_bytes as u64,
            false,
        ),
        ("LIVEENGINE_MAX_DEPTH", 5, |c| c.max_depth as u64, false),
        (
            "LIVEENGINE_MAX_ENTITIES",
            37,
            |c| c.max_entities as u64,
            false,
        ),
        ("LIVEENGINE_MAX_EVENTS", 37, |c| c.max_events as u64, false),
        (
            "LIVEENGINE_MAX_GENERATE",
            37,
            |c| c.max_generates_per_call as u64,
            false,
        ),
        ("LIVEENGINE_MAX_HUBS", 37, |c| c.max_hubs as u64, false),
        (
            "LIVEENGINE_MAX_INFLIGHT_CALLS",
            37,
            |c| c.max_inflight_calls as u64,
            false,
        ),
        (
            "LIVEENGINE_MAX_PAIR_WAIT_MS",
            37,
            |c| c.max_pair_wait.as_millis() as u64,
            true,
        ),
        (
            "LIVEENGINE_MAX_PENDING_PAIRS",
            37,
            |c| c.max_pending_pairs as u64,
            false,
        ),
        (
            "LIVEENGINE_MAX_QUEUED_CALLS",
            37,
            |c| c.max_queued_calls as u64,
            false,
        ),
        (
            "LIVEENGINE_MAX_RESPONSE_BYTES",
            2048,
            |c| c.max_response_bytes as u64,
            false,
        ),
        (
            "LIVEENGINE_MAX_ROOM_CAPACITY",
            37,
            |c| c.max_room_capacity as u64,
            false,
        ),
        (
            "LIVEENGINE_RATE_LIMIT_PEERS",
            37,
            |c| c.rate_limit_peers as u64,
            false,
        ),
        (
            "LIVEENGINE_RATE_LIMIT_WINDOW_SECONDS",
            37,
            |c| c.rate_limit_window.as_secs(),
            false,
        ),
        ("LIVEENGINE_SEED", 37, |c| c.seed, true),
        (
            "LIVEENGINE_TICKET_TTL_SECONDS",
            37,
            |c| c.ticket_ttl.as_secs(),
            false,
        ),
    ];
    for &(name, value, read, zero_valid) in cases {
        std::env::set_var(name, value.to_string());
        assert_eq!(read(&HubConfig::from_env().unwrap()), value, "{name}");
        std::env::set_var(name, "invalid");
        assert!(HubConfig::from_env().is_err(), "{name} accepted invalid");
        std::env::set_var(name, "0");
        assert_eq!(HubConfig::from_env().is_ok(), zero_valid, "{name} zero");
        std::env::remove_var(name);
    }
    std::env::set_var("LIVEENGINE_MAX_DEPTH", "17");
    assert!(HubConfig::from_env().is_err());
    std::env::remove_var("LIVEENGINE_MAX_DEPTH");
    for name in [
        "LIVEENGINE_SESSION_TTL_SECONDS",
        "LIVEENGINE_LOGIN_MAX_FAILURES",
    ] {
        for invalid in ["invalid", "0"] {
            std::env::set_var(name, invalid);
            assert!(
                HubService::new(HubConfig::from_env().unwrap()).is_err(),
                "{name}={invalid}"
            );
        }
        std::env::remove_var(name);
    }
    std::env::set_var("LIVEENGINE_SESSION_TTL_SECONDS", "7");
    std::env::set_var("LIVEENGINE_LOGIN_MAX_FAILURES", "1");
    std::env::set_var("LIVEENGINE_PASSWORD_ITERATIONS", "100000");
    std::env::set_var("LIVEENGINE_PUBLIC_URL", "ws://example.test/v1/live");
    let service = HubService::new(HubConfig::from_env().unwrap()).unwrap();
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
        .unwrap()
    });
    let http = reqwest::Client::new();
    let post = |path: &str, body: Value| {
        http.post(format!("{base}{path}"))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
    };
    assert_eq!(
        post(
            "/v1/accounts",
            json!({"username":"alice","password":"long-password-123","role":"user"})
        )
        .await
        .unwrap()
        .status(),
        201
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let login: Value = post(
        "/v1/sessions",
        json!({"username":"alice","password":"long-password-123"}),
    )
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert!((now + 7..=now + 9).contains(&login["expiresAt"].as_u64().unwrap()));
    assert_eq!(
        post(
            "/v1/sessions",
            json!({"username":"alice","password":"wrong-password"})
        )
        .await
        .unwrap()
        .status(),
        401
    );
    assert_eq!(
        post(
            "/v1/sessions",
            json!({"username":"alice","password":"long-password-123"})
        )
        .await
        .unwrap()
        .status(),
        401
    );
    assert_eq!(post("/v1/hubs", json!({"id":"game","mode":"internal","auth":{"kind":"token","tokens":[{"token":CREDENTIAL,"identity":"alice"}]}})).await.unwrap().status(), 201);
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        assert!(
            post("/v1/hubs/game/call", json!({"method":method,"args":args}))
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }
    let binary = env!("CARGO_BIN_EXE_liveengine-hub");
    let run = |args: Vec<String>, credential: Option<&str>, token: Option<&str>| {
        let mut cmd = tokio::process::Command::new(binary);
        cmd.args(args)
            .env_remove("LIVEENGINE_CREDENTIAL")
            .env_remove("LIVEENGINE_TOKEN");
        if let Some(v) = credential {
            cmd.env("LIVEENGINE_CREDENTIAL", v);
        }
        if let Some(v) = token {
            cmd.env("LIVEENGINE_TOKEN", v);
        }
        cmd
    };
    let args = vec![
        "ticket".into(),
        base.clone(),
        "game".into(),
        "alice".into(),
        "room".into(),
    ];
    assert!(!run(args.clone(), None, Some(TOKEN))
        .output()
        .await
        .unwrap()
        .status
        .success());
    let output = run(args, Some(CREDENTIAL), Some(TOKEN))
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["ticket"]
            .as_str()
            .is_some()
    );
    assert_eq!(
        post("/v1/hubs", json!({"id":"central","mode":"central"}))
            .await
            .unwrap()
            .status(),
        201
    );
    let args = vec![
        "watch-ticket".into(),
        base.clone(),
        "--path".into(),
        "central".into(),
        "--depth".into(),
        "1".into(),
    ];
    assert!(!run(args.clone(), None, None)
        .output()
        .await
        .unwrap()
        .status
        .success());
    let output = run(args, None, Some(TOKEN)).output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ticket = serde_json::from_slice::<Value>(&output.stdout).unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        http.post(format!("{base}/v1/hubs"))
            .bearer_auth(&ticket)
            .json(&json!({"id":"forbidden","mode":"internal"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let ws_url = base.replace("http://", "ws://") + "/v1/live/registry";
    for (path, accepted) in [(json!(["central"]), true), (json!([]), false)] {
        let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
        ws.send(Message::Text(
            json!({"type":"auth","ticket":ticket,"path":path,"depth":1})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap();
        if accepted {
            let Message::Text(text) = frame.unwrap().unwrap() else {
                panic!("expected snapshot")
            };
            assert_eq!(
                serde_json::from_str::<Value>(&text).unwrap()["type"],
                "snapshot"
            );
        } else {
            assert!(matches!(
                frame,
                None | Some(Ok(Message::Close(_))) | Some(Err(_))
            ));
        }
    }
    task.abort();
}
