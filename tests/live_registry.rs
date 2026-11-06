#![cfg(feature = "host")]
use futures_util::{SinkExt, StreamExt};
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
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

async fn remote_service(addr: Option<std::net::SocketAddr>, count: Arc<AtomicUsize>) -> Fixture {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        live_tree_depth: 8,
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..Default::default()
    })
    .unwrap();
    let listener =
        tokio::net::TcpListener::bind(addr.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
    let addr = listener.local_addr().unwrap();
    let router = service.router().layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let count = count.clone();
            async move {
                if req.uri().path() == "/v1/live/registry" {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                next.run(req).await
            }
        },
    ));
    let task = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
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

async fn wait_for(ws: &mut Socket, predicate: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = read(ws).await;
            if predicate(&frame) {
                return frame;
            }
        }
    })
    .await
    .unwrap()
}
async fn proxy(
    addr: Option<std::net::SocketAddr>,
    target: std::net::SocketAddr,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener =
        tokio::net::TcpListener::bind(addr.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((mut incoming, _)) = accepted else { break };
                    connections.spawn(async move {
                        if let Ok(mut outgoing) = tokio::net::TcpStream::connect(target).await {
                            let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                        }
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (addr, task)
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

#[tokio::test]
async fn remote_tree_updates_recovers_and_shares_upstream() {
    let count = Arc::new(AtomicUsize::new(0));
    let leaf = remote_service(None, count.clone()).await;
    let target = leaf.base.trim_start_matches("http://").parse().unwrap();
    let (addr, proxy_task) = proxy(None, target).await;
    let middle = remote_service(None, Arc::new(AtomicUsize::new(0))).await;
    let root = remote_service(None, Arc::new(AtomicUsize::new(0))).await;
    post(&middle, "/v1/hubs", json!({"id":"leaf","mode":"central","address":format!("http://{addr}"),"remoteToken":TOKEN})).await;
    post(
        &root,
        "/v1/hubs",
        json!({"id":"middle","mode":"central","address":middle.base,"remoteToken":TOKEN}),
    )
    .await;
    let mut a = connect(&root, json!({"type":"auth","token":TOKEN}), json!([]), 4).await;
    let mut b = connect(&root, json!({"type":"auth","token":TOKEN}), json!([]), 4).await;
    read(&mut a).await;
    read(&mut b).await;
    wait_for(&mut a, |v| {
        v["path"] == json!(["middle", "leaf"]) && v["hub"]["status"] == "ok"
    })
    .await;
    post(&leaf, "/v1/hubs", json!({"id":"game","mode":"internal"})).await;
    let added = wait_for(&mut a, |v| {
        v["path"] == json!(["middle", "leaf", "game"]) && v["type"] == "added"
    })
    .await;
    assert!(!added.to_string().contains(TOKEN));
    wait_for(&mut b, |v| {
        v["path"] == json!(["middle", "leaf", "game"]) && v["type"] == "added"
    })
    .await;
    let ticket = post(
        &root,
        "/v1/tickets/watch",
        json!({"path":["middle","leaf"],"depth":1}),
    )
    .await["ticket"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut scoped = connect(
        &root,
        json!({"type":"auth","ticket":ticket}),
        json!(["middle", "leaf"]),
        1,
    )
    .await;
    let first = read(&mut scoped).await;
    if first["hubs"].as_array().unwrap().is_empty() {
        wait_for(&mut scoped, |v| {
            v["path"] == json!(["game"]) && v["type"] == "added"
        })
        .await;
    } else {
        assert_eq!(first["hubs"][0]["id"], "game");
    }
    scoped.close(None).await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    proxy_task.abort();
    let _ = proxy_task.await;
    wait_for(&mut a, |v| {
        v["path"] == json!(["middle", "leaf"]) && v["hub"]["status"] == "unreachable"
    })
    .await;
    post(&root, "/v1/hubs", json!({"id":"local","mode":"internal"})).await;
    wait_for(&mut a, |v| {
        v["path"] == json!(["local"]) && v["type"] == "added"
    })
    .await;
    let (_, proxy_task) = proxy(Some(addr), target).await;
    post(&leaf, "/v1/hubs", json!({"id":"back","mode":"internal"})).await;
    wait_for(&mut a, |v| {
        v["path"] == json!(["middle", "leaf", "back"]) && v["type"] == "added"
    })
    .await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    a.close(None).await.unwrap();
    b.close(None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut c = connect(&root, json!({"type":"auth","token":TOKEN}), json!([]), 4).await;
    read(&mut c).await;
    wait_for(&mut c, |v| {
        v["path"] == json!(["middle", "leaf", "back"]) && v["type"] == "added"
    })
    .await;
    assert_eq!(count.load(Ordering::SeqCst), 3);
    proxy_task.abort();
}

#[tokio::test]
async fn remote_registry_refuses_trace_loops() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let f = remote_service(None, Arc::new(AtomicUsize::new(0))).await;
    let mut request = f.ws.as_str().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-liveengine-trace", "repeated,repeated".parse().unwrap());
    let error = connect_async(request).await.unwrap_err();
    assert!(
        matches!(error, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 508)
    );
    post(
        &f,
        "/v1/hubs",
        json!({"id":"self","mode":"central","address":f.base,"remoteToken":TOKEN}),
    )
    .await;
    let mut ws = connect(&f, json!({"type":"auth","token":TOKEN}), json!([]), 4).await;
    let snapshot = read(&mut ws).await;
    assert_eq!(snapshot["hubs"][0]["status"], "unreachable");
    tokio::time::sleep(Duration::from_millis(600)).await;
    ws.send(Message::Text(
        json!({"type":"resnapshot"}).to_string().into(),
    ))
    .await
    .unwrap();
    let snapshot = read(&mut ws).await;
    assert_eq!(snapshot["hubs"][0]["status"], "unreachable");
}

#[tokio::test]
async fn subscriber_capacity_is_configurable_per_service() {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        live_registry_max_subscribers: 1,
        ..Default::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/v1/live/registry", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, service.router().into_make_service())
            .await
            .unwrap()
    });
    let (mut first, _) = connect_async(&url).await.unwrap();
    first
        .send(Message::Text(
            json!({"type":"auth","token":TOKEN,"path":[],"depth":1})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(read(&mut first).await["type"], "snapshot");
    assert!(connect_async(&url).await.is_err());
    first.close(None).await.unwrap();
    let mut connected = false;
    for _ in 0..50 {
        if let Ok((mut socket, _)) = connect_async(&url).await {
            socket.close(None).await.unwrap();
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(connected);
    task.abort();
}
