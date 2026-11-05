#![cfg(feature = "host")]
use futures_util::{SinkExt, StreamExt};
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
const TOKEN: &str = "registry-admin-token-at-least-24-characters";
struct Fixture {
    base: String,
    ws: String,
    client: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn setup() -> Fixture {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        live_tree_depth: 2,
        ..Default::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, service.router().into_make_service())
            .await
            .unwrap();
    });
    Fixture {
        base: format!("http://{addr}"),
        ws: format!("ws://{addr}/v1/live/registry"),
        client: reqwest::Client::new(),
        task,
    }
}
async fn post(f: &Fixture, route: &str, body: Value) -> Value {
    let response = f
        .client
        .post(format!("{}{}", f.base, route))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{}", response.status());
    response.json().await.unwrap_or(Value::Null)
}
async fn route(f: &Fixture, path: Value, op: &str, body: Value) -> Value {
    post(f, "/v1/route", json!({"path":path,"op":op,"body":body})).await
}
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn connect(f: &Fixture, auth: Value, path: Value, depth: usize) -> Socket {
    let (mut ws, _) = connect_async(&f.ws).await.unwrap();
    ws.send(Message::Text(auth.to_string().into()))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"path":path,"depth":depth}).to_string().into(),
    ))
    .await
    .unwrap();
    ws
}
async fn read(ws: &mut Socket) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(4), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}
#[tokio::test]
async fn local_tree_snapshots_diffs_depth_scope_and_resnapshot() {
    let f = setup().await;
    post(
        &f,
        "/v1/hubs",
        json!({"id":"a","mode":"central","liveDepth":3}),
    )
    .await;
    route(
        &f,
        json!(["a"]),
        "register",
        json!({"id":"b","mode":"central"}),
    )
    .await;
    route(
        &f,
        json!(["a", "b"]),
        "register",
        json!({"id":"game","mode":"internal"}),
    )
    .await;
    let mut root = connect(&f, json!({"type":"auth","token":TOKEN}), json!([]), 99).await;
    let snap = read(&mut root).await;
    assert_eq!(snap["effectiveDepth"], 2);
    assert_eq!(snap["hubs"][0]["hubs"][0]["childrenLive"], false);
    let mut deep = connect(&f, json!({"type":"auth","token":TOKEN}), json!(["a"]), 99).await;
    let snap = read(&mut deep).await;
    assert_eq!(snap["effectiveDepth"], 3);
    assert_eq!(snap["hubs"][0]["hubs"][0]["id"], "game");
    for (method, args) in [
        ("upsertServer", json!([{"id":"room"}])),
        ("upsertUser", json!([{"id":"user"}])),
        ("join", json!(["user", "room"])),
    ] {
        route(
            &f,
            json!(["a", "b"]),
            "call",
            json!({"id":"game","method":method,"args":args}),
        )
        .await;
    }
    let diff = read(&mut deep).await;
    assert_eq!(diff["type"], "updated");
    assert_eq!(diff["seq"], 1);
    assert_eq!(diff["hub"]["roomCount"], 1);
    assert_eq!(diff["hub"]["memberCount"], 1);
    deep.send(Message::Text(
        json!({"type":"resnapshot"}).to_string().into(),
    ))
    .await
    .unwrap();
    let fresh = read(&mut deep).await;
    assert_eq!(fresh["type"], "snapshot");
    assert_eq!(fresh["seq"], 1);
    assert!(!fresh.to_string().contains(TOKEN));
    let ticket = post(&f, "/v1/tickets/watch", json!({"path":["a"],"depth":2})).await["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let mut watched = connect(&f, json!({"type":"auth","ticket":ticket}), json!(["a"]), 2).await;
    let frame = read(&mut watched).await;
    assert_eq!(frame["type"], "snapshot");
    assert!(!frame.to_string().contains(TOKEN));
    assert!(!frame.to_string().contains("address"));
    assert!(!frame.to_string().contains("ticket"));
    let ticket = route(
        &f,
        json!(["a"]),
        "watchTicket",
        json!({"path":[],"depth":1}),
    )
    .await["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let mut denied = connect(&f, json!({"type":"auth","ticket":ticket}), json!([]), 1).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), denied.next())
            .await
            .unwrap(),
        None | Some(Ok(Message::Close(_))) | Some(Err(_))
    ));
    route(
        &f,
        json!(["a", "b"]),
        "register",
        json!({"id":"new","mode":"internal"}),
    )
    .await;
    let added = read(&mut deep).await;
    assert_eq!(added["type"], "added");
    assert_eq!(added["seq"], 2);
    route(&f, json!(["a", "b"]), "delete", json!({"id":"new"})).await;
    let removed = read(&mut deep).await;
    assert_eq!(removed["type"], "removed");
    assert_eq!(removed["seq"], 3);
}
