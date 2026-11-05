#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "discovery-administrator-token-123456";
const REMOTE: &str = "remote-discovery-token-123456789";

async fn server(token: &str) -> (String, tokio::task::JoinHandle<()>) {
    let service = HubService::new(HubConfig {
        token: token.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        public_url: Some("ws://live.example.test/v1/live".into()),
        ..HubConfig::default()
    })
    .unwrap();
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
        .unwrap()
    });
    (format!("http://{address}"), task)
}

async fn post(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    path: &str,
    body: Value,
) -> (u16, Value) {
    let response = client
        .post(format!("{base}{path}"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap())
}

#[tokio::test]
async fn discovery_across_remote_nest_is_bounded_and_redacted() {
    let (root, root_task) = server(TOKEN).await;
    let (remote, remote_task) = server(REMOTE).await;
    let client = reqwest::Client::new();
    let (status, body) = post(
        &client,
        &remote,
        REMOTE,
        "/v1/hubs",
        json!({"id":"game","mode":"internal","scopes":{"region":"south","private":"192.168.1.9"}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = post(
        &client,
        &remote,
        REMOTE,
        "/v1/hubs/game/call",
        json!({"method":"upsertUser","args":[{"id":"alice","scopes":{"rank":7},"requirements":{"skill":2}}]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        &client,
        &root,
        TOKEN,
        "/v1/hubs",
        json!({"id":"remote","mode":"central","address":remote,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = post(
        &client,
        &root,
        TOKEN,
        "/v1/discover",
        json!({"kind":"any","depth":2}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let candidates = body["candidates"].as_array().unwrap();
    assert!(candidates.iter().any(|c| c["kind"] == "hub"
        && c["id"] == "game"
        && c["path"] == json!(["remote", "game"])));
    assert!(candidates
        .iter()
        .any(|c| c["kind"] == "peer" && c["id"] == "alice"));
    assert!(!body.to_string().contains(REMOTE));
    assert!(!body.to_string().contains("127.0.0.1"));
    assert!(!body.to_string().contains("192.168.1.9"));
    let (_, body) = post(
        &client,
        &root,
        TOKEN,
        "/v1/discover",
        json!({"kind":"peer","depth":1}),
    )
    .await;
    assert_eq!(body["candidates"], json!([]));
    let (_, body) = post(&client, &root, TOKEN, "/v1/route", json!({"path":["remote"],"op":"discover","body":{"kind":"peer","requirements":{"rank":{"gte":7}}}})).await;
    assert_eq!(body["candidates"][0]["id"], "alice");
    let (status, _) = post(
        &client,
        &root,
        TOKEN,
        "/v1/discover",
        json!({"kind":"any","depth":9}),
    )
    .await;
    assert_eq!(status, 400);
    let response = client
        .post(format!("{root}/v1/discover"))
        .json(&json!({"kind":"any"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    root_task.abort();
    remote_task.abort();
}

#[tokio::test]
async fn pairing_returns_each_link_kind_through_a_remote_nest() {
    let (root, root_task) = server(TOKEN).await;
    let (remote, remote_task) = server(REMOTE).await;
    let c = reqwest::Client::new();
    let (status, body) = post(
        &c,
        &remote,
        REMOTE,
        "/v1/hubs",
        json!({"id":"game","mode":"internal","scopes":{"region":"south"}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = post(
        &c,
        &remote,
        REMOTE,
        "/v1/hubs/game/call",
        json!({"method":"upsertUser","args":[{"id":"alice","scopes":{"rank":7},"requirements":{"skill":2}}]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        &c,
        &root,
        TOKEN,
        "/v1/hubs",
        json!({"id":"nest","mode":"central","address":remote,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    for (subject, target, link) in [
        (
            json!({"kind":"peer","id":"bob","scopes":{"rank":7,"skill":2}}),
            json!({"kind":"peer","requirements":{"rank":7}}),
            "peer-peer",
        ),
        (
            json!({"kind":"peer","id":"carol","scopes":{}}),
            json!({"kind":"hub","requirements":{"region":"south"}}),
            "peer-hub",
        ),
        (
            json!({"kind":"hub","id":"service","scopes":{}}),
            json!({"kind":"hub","requirements":{"region":"south"}}),
            "hub-hub",
        ),
    ] {
        let (status, body) = post(
            &c,
            &root,
            TOKEN,
            "/v1/pair",
            json!({"path":["nest"],"subject":subject,"target":target}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["link"], link);
        assert!(body["ticket"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(body["url"], "ws://live.example.test/v1/live");
        assert_eq!(body["transports"], json!(["webrtc", "websocket"]));
        assert!(!body.to_string().contains(REMOTE));
    }
    let (status, body) = post(&c, &root, TOKEN, "/v1/route", json!({"path":["nest"],"op":"pair","body":{"subject":{"kind":"peer","id":"dave","scopes":{"rank":7,"skill":2}},"target":{"kind":"peer","requirements":{"rank":7}}}})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["link"], "peer-peer");
    let (status, body) = post(
        &c,
        &remote,
        REMOTE,
        "/v1/hubs/game/call",
        json!({"method":"getServer","args":[body["room"]]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"]["members"].as_array().unwrap().len(), 3);
    root_task.abort();
    remote_task.abort();
}
