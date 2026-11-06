#![cfg(feature = "host")]
//! The Rust `HubClient` methods for live tickets and the registry watch.
use liveengine::hub::{HubClient, HubConfig, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "rust-client-admin-token-123456789012";

async fn hub() -> (String, tokio::task::JoinHandle<()>) {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        public_url: Some("ws://live.example.test/v1/live".into()),
        live_tree_depth: 3,
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
        .unwrap()
    });
    (base, task)
}

async fn paired(client: &HubClient) {
    client
        .request(
            reqwest::Method::POST,
            "/v1/hubs",
            Some(json!({"id":"game","mode":"internal"})),
        )
        .await
        .unwrap();
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        client
            .request(
                reqwest::Method::POST,
                "/v1/hubs/game/call",
                Some(json!({"method":method,"args":args})),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn tickets_are_issued_directly_and_through_a_route() {
    let (base, task) = hub().await;
    let client = HubClient::new(&base, TOKEN).unwrap();
    paired(&client).await;
    let contract = client
        .request(reqwest::Method::GET, "/v1/contract", None)
        .await
        .unwrap();
    for route in ["/v1/discover", "/v1/pair", "/v1/live"] {
        assert!(contract["endpoints"]
            .as_array()
            .unwrap()
            .contains(&json!(route)));
    }
    let direct = client
        .ticket(&[], "game", "alice", "room", "peer", None)
        .await
        .unwrap();
    assert!(direct["ticket"].as_str().is_some_and(|t| !t.is_empty()));
    assert_eq!(direct["url"], "ws://live.example.test/v1/live");
    let service = client
        .ticket(&[], "game", "alice", "room", "service", None)
        .await
        .unwrap();
    assert_ne!(service["ticket"], direct["ticket"]);
    assert!(client
        .ticket(&[], "game", "mallory", "room", "peer", None)
        .await
        .is_err());
    assert!(client
        .ticket(&[], "game", "alice", "room", "admin", None)
        .await
        .is_err());
    task.abort();
}

#[tokio::test]
async fn watch_tickets_are_scoped_and_the_watch_streams_a_snapshot_then_diffs() {
    let (base, task) = hub().await;
    let client = HubClient::new(&base, TOKEN).unwrap();
    let ticket = client.watch_ticket(&[], 2).await.unwrap();
    assert!(ticket["ticket"].as_str().is_some_and(|t| !t.is_empty()));
    assert!(client
        .watch_ticket(&["missing".to_string()], 1)
        .await
        .is_err());
    let adder = client.clone();
    let add = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        adder
            .request(
                reqwest::Method::POST,
                "/v1/hubs",
                Some(json!({"id":"late","mode":"internal"})),
            )
            .await
            .unwrap();
    });
    let mut messages: Vec<Value> = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.watch(&[], 1, |message| {
            messages.push(message);
            messages.len() < 2
        }),
    )
    .await
    .expect("watch timed out")
    .unwrap();
    add.await.unwrap();
    assert_eq!(messages[0]["type"], "snapshot");
    assert_eq!(messages[0]["seq"], 0);
    assert_eq!(messages[1]["type"], "added");
    assert_eq!(messages[1]["seq"], 1);
    assert_eq!(messages[1]["path"], json!(["late"]));
    task.abort();
}

#[tokio::test]
async fn discover_and_pair_allow_the_requested_wait() {
    use axum::{routing::post, Json, Router};
    async fn discover(Json(body): Json<Value>) -> Json<Value> {
        Json(body)
    }
    async fn pair(Json(body): Json<Value>) -> Json<Value> {
        tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
        Json(body)
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/discover", post(discover))
                .route("/v1/pair", post(pair))
                .route("/v1/route", post(pair)),
        )
        .await
        .unwrap()
    });
    let client = HubClient::new(&base, TOKEN).unwrap();
    assert_eq!(
        client.discover(json!({"kind":"peer"})).await.unwrap()["kind"],
        "peer"
    );
    assert_eq!(
        client.pair(json!({"waitMs":1000})).await.unwrap()["waitMs"],
        1000
    );
    assert_eq!(
        client
            .route(&[], "pair", json!({"waitMs":1000}))
            .await
            .unwrap()["op"],
        "pair"
    );
    task.abort();
}
