#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};
const A: &str = "local-administrator-token-123456789";
const B: &str = "remote-administrator-token-123456789";
async fn serve(service: HubService) -> (String, tokio::task::JoinHandle<()>) {
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
async fn post(base: &str, token: &str, path: &str, body: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap()
}
async fn proxy(
    remote: String,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) {
    use axum::{
        extract::{
            ws::{Message, WebSocketUpgrade},
            State,
        },
        routing::{get, post},
        Json, Router,
    };
    use futures_util::{SinkExt, StreamExt};
    let frames = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let state = (remote, frames.clone());
    let app = Router::new()
        .route("/v1/hubs/game/tickets", post(|State((remote, _)): State<(String, std::sync::Arc<std::sync::Mutex<Vec<String>>>)>, headers: axum::http::HeaderMap, Json(body): Json<Value>| async move {
            let response = reqwest::Client::new().post(format!("{remote}/v1/hubs/game/tickets")).header("authorization", headers.get("authorization").unwrap()).json(&body).send().await.unwrap();
            let status = response.status(); let value: Value = response.json().await.unwrap(); (status, Json(value))
        }))
        .route("/v1/live", get(|State((remote, frames)): State<(String, std::sync::Arc<std::sync::Mutex<Vec<String>>>)>, ws: WebSocketUpgrade| async move {
            ws.on_upgrade(move |mut inbound| async move {
                let url = remote.replacen("http://", "ws://", 1) + "/v1/live";
                let (mut outbound, _) = tokio_tungstenite::connect_async(url).await.unwrap();
                loop { tokio::select! {
                    m = inbound.recv() => match m { Some(Ok(Message::Text(t))) => { frames.lock().unwrap().push(t.to_string()); if outbound.send(tokio_tungstenite::tungstenite::Message::Text(t.to_string().into())).await.is_err() { break; } }, Some(Ok(Message::Pong(p))) => { if outbound.send(tokio_tungstenite::tungstenite::Message::Pong(p.to_vec().into())).await.is_err() { break; } }, _ => break },
                    m = outbound.next() => match m { Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => { frames.lock().unwrap().push(t.to_string()); if inbound.send(Message::Text(t.to_string().into())).await.is_err() { break; } }, Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(p))) => { if inbound.send(Message::Ping(p.to_vec().into())).await.is_err() { break; } }, _ => break }
                }}
            })
        })).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base, task, frames)
}
async fn status(base: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let r: Value = reqwest::Client::new()
                .get(format!("{base}/v1/links/link"))
                .bearer_auth(A)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert!(!r.to_string().contains(B));
            if r["status"] == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn persistent_link_connects_and_recovers_over_tcp() {
    let path = std::env::temp_dir().join(format!(
        "liveengine-links-{:032x}.json",
        rand::random::<u128>()
    ));
    let remote = HubService::new(HubConfig {
        token: B.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        ..Default::default()
    })
    .unwrap();
    let (remote_base, remote_task) = serve(remote).await;
    assert_eq!(
        post(
            &remote_base,
            B,
            "/v1/hubs",
            json!({"id":"game","mode":"internal"})
        )
        .await
        .status(),
        201
    );
    for (method, args) in [
        ("upsertUser", json!([{"id":"service"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["service", "room"])),
    ] {
        assert_eq!(
            post(
                &remote_base,
                B,
                "/v1/hubs/game/call",
                json!({"method":method,"args":args})
            )
            .await
            .status(),
            200
        );
    }
    let (proxy_base, proxy_task, frames) = proxy(remote_base.clone()).await;
    let config = HubConfig {
        token: A.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        state_file: Some(path.clone()),
        ..Default::default()
    };
    let local = HubService::new(config.clone()).unwrap();
    local.initialize_state().await.unwrap();
    let (base, task) = serve(local).await;
    assert_eq!(
        reqwest::Client::new()
            .get(format!("{base}/v1/links"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(post(&base, A, "/v1/links", json!({"id":"denied","address":"http://disallowed.example","remoteToken":B,"hubId":"game","userId":"service","serverId":"room"})).await.status(), 400);
    let credential: Value = post(&remote_base, B, "/v1/hubs/game/link-credentials", json!({}))
        .await
        .json()
        .await
        .unwrap();
    let credential = credential["token"].as_str().unwrap().to_string();
    assert_eq!(post(&base,A,"/v1/links",json!({"id":"link","address":proxy_base,"remoteToken":credential,"hubId":"game","userId":"service","serverId":"room"})).await.status(),201);
    status(&base, "connected").await;
    assert_eq!(
        post(
            &remote_base,
            B,
            "/v1/hubs/game/call",
            json!({"method":"leave","args":["service","room"]})
        )
        .await
        .status(),
        200
    );
    status(&base, "unreachable").await;
    assert_eq!(
        post(
            &remote_base,
            B,
            "/v1/hubs/game/call",
            json!({"method":"join","args":["service","room"]})
        )
        .await
        .status(),
        200
    );
    status(&base, "connected").await;
    task.abort();
    let local2 = HubService::new(config).unwrap();
    let (base2, task2) = serve(local2).await;
    status(&base2, "connected").await;
    let list: Value = reqwest::Client::new()
        .get(format!("{base2}/v1/links"))
        .bearer_auth(A)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!list.to_string().contains(&credential));
    assert_eq!(
        reqwest::Client::new()
            .delete(format!("{base2}/v1/links/link"))
            .bearer_auth(A)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(frames
        .lock()
        .unwrap()
        .iter()
        .any(|frame| frame.contains("\"type\":\"auth\"")));
    assert!(frames
        .lock()
        .unwrap()
        .iter()
        .all(|frame| !frame.contains(A) && !frame.contains(B) && !frame.contains(&credential)));
    task2.abort();
    proxy_task.abort();
    remote_task.abort();
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn remote_central_ticket_preserves_inherited_channel_schema() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let remote = HubService::new(HubConfig {
        token: B.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        ..Default::default()
    })
    .unwrap();
    let (remote_base, remote_task) = serve(remote).await;
    let local = HubService::new(HubConfig {
        token: A.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..Default::default()
    })
    .unwrap();
    let (base, task) = serve(local).await;
    assert_eq!(
        post(
            &base,
            A,
            "/v1/schemas",
            json!({"id":"message","version":"1","schema":{"type":"integer"}})
        )
        .await
        .status(),
        201
    );
    assert_eq!(post(&base, A, "/v1/hubs", json!({"id":"remote","mode":"central","address":remote_base,"remoteToken":B,"channels":{"updates":{"delivery":"reliable","schemaRef":"message@1"}}})).await.status(), 201);
    assert_eq!(
        post(
            &remote_base,
            B,
            "/v1/hubs",
            json!({"id":"game","mode":"internal"})
        )
        .await
        .status(),
        201
    );
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertUser", json!([{"id":"bob"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
        ("join", json!(["bob", "room"])),
    ] {
        assert_eq!(
            post(
                &remote_base,
                B,
                "/v1/hubs/game/call",
                json!({"method":method,"args":args})
            )
            .await
            .status(),
            200
        );
    }
    async fn connect(
        base: &str,
        remote: &str,
        user: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let issued: Value = post(base, A, "/v1/route", json!({"path":["remote"],"op":"ticket","body":{"id":"game","userId":user,"serverId":"room"}})).await.json().await.unwrap();
        let (mut socket, _) = connect_async(format!(
            "{}/v1/live",
            remote.replacen("http://", "ws://", 1)
        ))
        .await
        .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"auth","ticket":issued["ticket"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let welcome = socket.next().await.unwrap().unwrap();
        assert!(welcome.to_string().contains("welcome"));
        socket
    }
    let mut alice = connect(&base, &remote_base, "alice").await;
    let mut bob = connect(&base, &remote_base, "bob").await;
    assert!(alice
        .next()
        .await
        .unwrap()
        .unwrap()
        .to_string()
        .contains("presence"));
    alice
        .send(Message::Text(
            json!({"v":1,"type":"message","channel":"updates","seq":1,"payload":42})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), bob.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .to_string()
        .contains("42"));
    alice
        .send(Message::Text(
            json!({"v":1,"type":"message","channel":"updates","seq":2,"payload":"invalid"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), alice.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let error: Value = serde_json::from_str(&frame.into_text().unwrap()).unwrap();
    assert_eq!(error, json!({"type":"error","code":"schema"}));
    task.abort();
    remote_task.abort();
}

async fn credential_remote(
    state_file: Option<std::path::PathBuf>,
) -> (String, tokio::task::JoinHandle<()>, String) {
    let remote = HubService::new(HubConfig {
        token: B.into(),
        public_url: None,
        state_file,
        ..Default::default()
    })
    .unwrap();
    remote.initialize_state().await.unwrap();
    let (base, task) = serve(remote).await;
    for id in ["game", "other"] {
        assert_eq!(
            post(&base, B, "/v1/hubs", json!({"id":id,"mode":"internal"}))
                .await
                .status(),
            201
        );
        for (method, args) in [
            ("upsertUser", json!([{"id":"service"}])),
            ("upsertServer", json!([{"id":"room","capacity":2}])),
            ("join", json!(["service", "room"])),
        ] {
            assert_eq!(
                post(
                    &base,
                    B,
                    &format!("/v1/hubs/{id}/call"),
                    json!({"method":method,"args":args})
                )
                .await
                .status(),
                200
            );
        }
    }
    let created = post(&base, B, "/v1/hubs/game/link-credentials", json!({})).await;
    assert_eq!(created.status(), 201);
    let token = created.json::<Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    (base, task, token)
}
fn service_ticket() -> Value {
    json!({"userId":"service","serverId":"room","role":"service"})
}

#[tokio::test]
async fn link_credential_gets_a_service_ticket_only() {
    let (base, task, token) = credential_remote(None).await;
    let response = post(&base, &token, "/v1/hubs/game/tickets", service_ticket()).await;
    assert_eq!(response.status(), 200);
    assert!(response.json::<Value>().await.unwrap()["ticket"].is_string());
    // Refused for role peer.
    let peer = post(
        &base,
        &token,
        "/v1/hubs/game/tickets",
        json!({"userId":"service","serverId":"room","role":"peer"}),
    )
    .await;
    assert_eq!(peer.status(), 403);
    // Refused for another hub.
    assert_eq!(
        post(&base, &token, "/v1/hubs/other/tickets", service_ticket())
            .await
            .status(),
        401
    );
    // Refused on every admin route.
    let client = reqwest::Client::new();
    let get = |path: &'static str| {
        client
            .get(format!("{base}{path}"))
            .bearer_auth(&token)
            .send()
    };
    assert_eq!(get("/v1/hubs").await.unwrap().status(), 401);
    assert_eq!(get("/v1/registry/export").await.unwrap().status(), 401);
    assert_eq!(get("/v1/links").await.unwrap().status(), 401);
    assert_eq!(
        post(
            &base,
            &token,
            "/v1/hubs",
            json!({"id":"x","mode":"internal"})
        )
        .await
        .status(),
        401
    );
    assert_eq!(
        post(
            &base,
            &token,
            "/v1/hubs/game/call",
            json!({"method":"getUser","args":["service"]})
        )
        .await
        .status(),
        401
    );
    assert_eq!(
        post(&base, &token, "/v1/hubs/game/link-credentials", json!({}))
            .await
            .status(),
        401
    );
    assert_eq!(
        client
            .delete(format!("{base}/v1/hubs/game"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .delete(format!("{base}/v1/hubs/game/link-credentials"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    // The hub is intact and the admin token still works.
    assert_eq!(
        client
            .get(format!("{base}/v1/hubs/game"))
            .bearer_auth(B)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // A made-up token is refused.
    assert_eq!(
        post(
            &base,
            "not-a-real-credential-0123456789",
            "/v1/hubs/game/tickets",
            service_ticket()
        )
        .await
        .status(),
        401
    );
    task.abort();
}

#[tokio::test]
async fn link_credential_revoke_stops_it() {
    let (base, task, token) = credential_remote(None).await;
    assert_eq!(
        post(&base, &token, "/v1/hubs/game/tickets", service_ticket())
            .await
            .status(),
        200
    );
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .delete(format!("{base}/v1/hubs/game/link-credentials"))
            .bearer_auth(B)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        post(&base, &token, "/v1/hubs/game/tickets", service_ticket())
            .await
            .status(),
        401
    );
    assert_eq!(
        client
            .delete(format!("{base}/v1/hubs/missing/link-credentials"))
            .bearer_auth(B)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    task.abort();
}

#[tokio::test]
async fn link_credential_raw_token_is_never_stored_or_echoed() {
    let path = std::env::temp_dir().join(format!("liveengine-credential-{}.json", rand_suffix()));
    let (base, task, token) = credential_remote(Some(path.clone())).await;
    let client = reqwest::Client::new();
    let mut bodies = vec![];
    for path in [
        "/v1/hubs",
        "/v1/hubs/game",
        "/v1/registry/export",
        "/v1/tree",
    ] {
        bodies.push(
            client
                .get(format!("{base}{path}"))
                .bearer_auth(B)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        );
    }
    bodies.push(
        post(&base, &token, "/v1/hubs/game/tickets", service_ticket())
            .await
            .text()
            .await
            .unwrap(),
    );
    assert!(bodies.iter().all(|body| !body.contains(&token)));
    let state = std::fs::read_to_string(&path).unwrap();
    assert!(!state.contains(&token));
    assert!(state.contains("link_credentials"));
    task.abort();
    // The stored hash survives a restart.
    let restarted = HubService::new(HubConfig {
        token: B.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        state_file: Some(path.clone()),
        ..Default::default()
    })
    .unwrap();
    restarted.initialize_state().await.unwrap();
    let (base2, task2) = serve(restarted).await;
    // Engine state is not persisted, so the user is no longer paired: 403 proves
    // the credential itself still authenticates, where an unknown one gets 401.
    assert_eq!(
        post(&base2, &token, "/v1/hubs/game/tickets", service_ticket())
            .await
            .status(),
        403
    );
    assert_eq!(
        post(
            &base2,
            "not-a-real-credential-0123456789",
            "/v1/hubs/game/tickets",
            service_ticket()
        )
        .await
        .status(),
        401
    );
    task2.abort();
    std::fs::remove_file(path).unwrap();
}
fn rand_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

// Rooms that span linked hubs.
type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct Node {
    name: String,
    base: String,
    token: String,
    task: tokio::task::JoinHandle<()>,
}
async fn node(name: &str, users: &[&str]) -> Node {
    node_with(name, users, None, None).await
}
async fn node_with(
    name: &str,
    users: &[&str],
    channels: Option<Value>,
    schema: Option<Value>,
) -> Node {
    let token = format!("node-{name}-administrator-token-123456789");
    let service = HubService::new(HubConfig {
        token: token.clone(),
        public_url: Some("ws://localhost/v1/live".into()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..Default::default()
    })
    .unwrap();
    let (base, task) = serve(service).await;
    let mut hub = json!({"id":"game","mode":"internal"});
    if let Some(schema) = schema {
        assert_eq!(
            post(
                &base,
                &token,
                "/v1/schemas",
                json!({"id":"message","version":"1","schema":schema})
            )
            .await
            .status(),
            201
        );
    }
    if let Some(channels) = channels {
        hub["channels"] = channels;
    }
    assert_eq!(post(&base, &token, "/v1/hubs", hub).await.status(), 201);
    let mut calls = vec![("upsertServer", json!([{"id":"room","capacity":16}]))];
    for user in users {
        calls.push(("upsertUser", json!([{"id":user}])));
        calls.push(("join", json!([user, "room"])));
    }
    for (method, args) in calls {
        assert_eq!(
            post(
                &base,
                &token,
                "/v1/hubs/game/call",
                json!({"method":method,"args":args})
            )
            .await
            .status(),
            200
        );
    }
    Node {
        name: name.into(),
        base,
        token,
        task,
    }
}
/// Dial `to` from `from` as service `svc-<from>`, bridging room `room` of hub `game` on both sides.
async fn link(from: &Node, to: &Node, id: &str) {
    let credential: Value = post(
        &to.base,
        &to.token,
        "/v1/hubs/game/link-credentials",
        json!({}),
    )
    .await
    .json()
    .await
    .unwrap();
    let body = json!({"id":id,"address":to.base,"remoteToken":credential["token"],"hubId":"game","userId":format!("svc-{}",from.name),"serverId":"room","localHubId":"game","localRoom":"room"});
    assert_eq!(
        post(&from.base, &from.token, "/v1/links", body)
            .await
            .status(),
        201
    );
}
async fn unlink(from: &Node, id: &str) {
    let response = reqwest::Client::new()
        .delete(format!("{}/v1/links/{id}", from.base))
        .bearer_auth(&from.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
}
async fn link_status(from: &Node, id: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let r: Value = reqwest::Client::new()
                .get(format!("{}/v1/links/{id}", from.base))
                .bearer_auth(&from.token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if r["status"] == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
}
async fn counters(from: &Node) -> (u64, u64) {
    let r: Value = reqwest::Client::new()
        .get(format!("{}/v1/links", from.base))
        .bearer_auth(&from.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    (
        r["frames"]["dropped"].as_u64().unwrap(),
        r["frames"]["rejected"].as_u64().unwrap(),
    )
}
async fn peer(node: &Node, user: &str) -> (Ws, Vec<String>) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let issued: Value = post(
        &node.base,
        &node.token,
        "/v1/hubs/game/tickets",
        json!({"userId":user,"serverId":"room"}),
    )
    .await
    .json()
    .await
    .unwrap();
    let (mut socket, _) = connect_async(format!(
        "{}/v1/live",
        node.base.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    socket
        .send(Message::Text(
            json!({"type":"auth","ticket":issued["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let welcome: Value =
        serde_json::from_str(&socket.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(welcome["type"], "welcome");
    let peers = welcome["peers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap().to_string())
        .collect();
    (socket, peers)
}
/// The next text frame matching `pred` within `ms`, discarding the others.
async fn wait_for(ws: &mut Ws, ms: u64, pred: impl Fn(&Value) -> bool) -> Option<Value> {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if pred(&value) {
                    return Some(value);
                }
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            _ => return None,
        }
    }
}
/// Wait until every one of `names` is known in the room, from the welcome list or a presence
/// join. Joins from different links arrive in no particular order, so one wait covers them all.
async fn sees(ws: &mut Ws, welcome: &[String], names: &[&str]) {
    let mut missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !welcome.iter().any(|p| p == name))
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !missing.is_empty() {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let joined = wait_for(ws, left.as_millis() as u64, |v| {
            v["type"] == "presence"
                && v["event"] == "join"
                && missing.iter().any(|n| v["peer"] == *n)
        })
        .await;
        let Some(joined) = joined else {
            panic!("{missing:?} never appeared");
        };
        missing.retain(|n| joined["peer"] != *n);
    }
}
fn frame(seq: u64, payload: Value) -> Message {
    Message::Text(
        json!({"v":1,"type":"message","channel":"updates","seq":seq,"payload":payload})
            .to_string()
            .into(),
    )
}
use tokio_tungstenite::tungstenite::Message;
async fn say(ws: &mut Ws, message: Message) {
    use futures_util::SinkExt;
    ws.send(message).await.unwrap();
}
fn is_frame(v: &Value) -> bool {
    v["type"] == "message"
}
/// Every non-presence frame that arrives within `ms`.
async fn frames_within(ws: &mut Ws, ms: u64) -> Vec<Value> {
    let mut out = vec![];
    while let Some(v) = wait_for(ws, ms, is_frame).await {
        out.push(v);
    }
    out
}
async fn pair() -> (Node, Node) {
    let a = node("a", &["alice", "carol", "svc-b"]).await;
    let b = node("b", &["bob", "dave", "svc-a"]).await;
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    (a, b)
}

#[tokio::test]
async fn frames_cross_a_linked_room_in_both_directions() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    say(&mut alice, frame(1, json!("from a"))).await;
    let got = wait_for(&mut bob, 5000, is_frame).await.unwrap();
    assert_eq!(
        (got["from"].as_str(), &got["payload"]),
        (Some("alice"), &json!("from a"))
    );
    say(&mut bob, frame(1, json!("from b"))).await;
    let got = wait_for(&mut alice, 5000, is_frame).await.unwrap();
    assert_eq!(
        (got["from"].as_str(), &got["payload"]),
        (Some("bob"), &json!("from b"))
    );
    // Nobody hears an echo of their own frame, and there are no duplicates.
    assert!(frames_within(&mut alice, 500).await.is_empty());
    assert!(frames_within(&mut bob, 500).await.is_empty());
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn targeted_and_signal_frames_cross_the_link_to_the_named_peer() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut carol, w3) = peer(&a, "carol").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut carol, &w3, &["bob"]).await;
    sees(&mut bob, &w2, &["alice", "carol"]).await;
    say(
        &mut alice,
        Message::Text(
            json!({"v":1,"type":"message","channel":"updates","seq":1,"to":"bob","payload":"hi"})
                .to_string()
                .into(),
        ),
    )
    .await;
    let got = wait_for(&mut bob, 5000, is_frame).await.unwrap();
    assert_eq!(
        (got["from"].as_str(), &got["payload"]),
        (Some("alice"), &json!("hi"))
    );
    assert!(frames_within(&mut carol, 500).await.is_empty());
    say(&mut bob, Message::Text(json!({"v":1,"type":"signal.offer","channel":"rtc","seq":1,"to":"carol","payload":{"sdp":"x"}}).to_string().into())).await;
    let got = wait_for(&mut carol, 5000, |v| v["type"] == "signal.offer")
        .await
        .unwrap();
    assert_eq!(
        (got["from"].as_str(), &got["payload"]["sdp"]),
        (Some("bob"), &json!("x"))
    );
    assert!(wait_for(&mut alice, 500, |v| v["type"] == "signal.offer")
        .await
        .is_none());
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn presence_is_merged_across_the_link_in_both_directions() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    // A peer joining later is announced on the other side, and its leave follows.
    let (mut carol, _) = peer(&a, "carol").await;
    assert!(wait_for(&mut bob, 5000, |v| v["type"] == "presence"
        && v["event"] == "join"
        && v["peer"] == "carol")
    .await
    .is_some());
    carol.close(None).await.unwrap();
    assert!(wait_for(&mut bob, 5000, |v| v["type"] == "presence"
        && v["event"] == "leave"
        && v["peer"] == "carol")
    .await
    .is_some());
    // A peer that connects after the link is up sees the other side in its welcome list.
    let (_dave, welcome) = peer(&b, "dave").await;
    assert!(welcome.iter().any(|p| p == "alice"), "{welcome:?}");
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn three_services_in_a_ring_deliver_once_and_do_not_loop() {
    let a = node("a", &["alice", "svc-c"]).await;
    let b = node("b", &["bob", "svc-a"]).await;
    let c = node("c", &["carol", "svc-b"]).await;
    link(&a, &b, "ab").await;
    link(&b, &c, "bc").await;
    link(&c, &a, "ca").await;
    for (n, id) in [(&a, "ab"), (&b, "bc"), (&c, "ca")] {
        link_status(n, id, "connected").await;
    }
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    let (mut carol, w3) = peer(&c, "carol").await;
    for (ws, w, names) in [
        (&mut alice, &w1, ["bob", "carol"]),
        (&mut bob, &w2, ["alice", "carol"]),
        (&mut carol, &w3, ["alice", "bob"]),
    ] {
        sees(ws, w, &names).await;
    }
    say(&mut alice, frame(1, json!("around"))).await;
    for ws in [&mut bob, &mut carol] {
        let got = wait_for(ws, 5000, is_frame).await.unwrap();
        assert_eq!(
            (got["from"].as_str(), &got["payload"]),
            (Some("alice"), &json!("around"))
        );
    }
    // The frame circles the ring but is delivered exactly once per peer and never returns.
    assert!(frames_within(&mut alice, 1500).await.is_empty());
    assert!(frames_within(&mut bob, 100).await.is_empty());
    assert!(frames_within(&mut carol, 100).await.is_empty());
    // A looping copy is absorbed as a duplicate, or as a sender not announced on that
    // link, depending on which path it took. Either way it never reaches a peer.
    let mut absorbed = 0;
    for n in [&a, &b, &c] {
        let (dropped, rejected) = counters(n).await;
        absorbed += dropped + rejected;
    }
    assert!(absorbed > 0, "the ring must have absorbed looping copies");
    // A second frame from the far side behaves the same.
    say(&mut carol, frame(1, json!("back"))).await;
    for ws in [&mut alice, &mut bob] {
        assert!(wait_for(ws, 5000, is_frame).await.is_some());
    }
    assert!(frames_within(&mut alice, 500).await.is_empty());
    assert!(frames_within(&mut bob, 500).await.is_empty());
    for n in [a, b, c] {
        n.task.abort();
    }
}

/// Whether the socket is closed by the hub within a few seconds.
async fn closed(ws: &mut Ws) -> bool {
    use futures_util::StreamExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                _ => {}
            }
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn a_peer_sending_a_link_frame_is_refused() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut carol, w3) = peer(&a, "carol").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut carol, &w3, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    let forged_frame = json!({"type":"link.frame","trace":["x"],"frame":{"from":"alice","v":1,"type":"message","channel":"updates","seq":1,"payload":"forged"}});
    let forged_presence =
        json!({"type":"link.presence","trace":["x"],"event":"join","peer":"ghost"});
    say(&mut alice, Message::Text(forged_frame.to_string().into())).await;
    assert!(
        closed(&mut alice).await,
        "a peer sending link.frame must be refused"
    );
    say(
        &mut carol,
        Message::Text(forged_presence.to_string().into()),
    )
    .await;
    assert!(
        closed(&mut carol).await,
        "a peer sending link.presence must be refused"
    );
    // Nothing leaked to the far side and no ghost peer appeared there.
    assert!(
        wait_for(&mut bob, 800, |v| is_frame(v) || v["peer"] == "ghost")
            .await
            .is_none()
    );
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn invalid_payloads_are_refused_at_ingress_and_again_on_a_stricter_far_side() {
    let channels = json!({"updates":{"delivery":"reliable","schemaRef":"message@1"}});
    let a = node_with(
        "a",
        &["alice", "svc-b"],
        Some(channels.clone()),
        Some(json!({"type":"integer"})),
    )
    .await;
    let b = node_with(
        "b",
        &["bob", "svc-a"],
        Some(channels),
        Some(json!({"type":"integer","maximum":10})),
    )
    .await;
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    // Valid on both sides.
    say(&mut alice, frame(1, json!(5))).await;
    assert_eq!(
        wait_for(&mut bob, 5000, is_frame).await.unwrap()["payload"],
        5
    );
    // Valid here but over the far side's maximum: refused there, and the link stays up.
    say(&mut alice, frame(2, json!(42))).await;
    say(&mut alice, frame(3, json!(7))).await;
    let got = frames_within(&mut bob, 1500).await;
    assert_eq!(
        got.iter().map(|f| f["payload"].clone()).collect::<Vec<_>>(),
        vec![json!(7)]
    );
    assert!(counters(&b).await.1 >= 1);
    // Invalid at ingress: the sender is refused and nothing crosses.
    say(&mut alice, frame(4, json!("bad"))).await;
    assert!(closed(&mut alice).await);
    assert!(frames_within(&mut bob, 500).await.is_empty());
    a.task.abort();
    b.task.abort();
}

/// Wait for a presence `event` about each of `names`, in any order.
async fn presence_set(ws: &mut Ws, event: &str, names: &[&str]) {
    let mut missing: Vec<&str> = names.to_vec();
    while !missing.is_empty() {
        let got = wait_for(ws, 10_000, |v| {
            v["type"] == "presence" && v["event"] == event
        })
        .await
        .unwrap_or_else(|| panic!("no {event} for {missing:?}"));
        missing.retain(|n| got["peer"] != *n);
    }
}

async fn killed_link() -> (Node, Node, Ws, Ws, Ws) {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut carol, w3) = peer(&a, "carol").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut carol, &w3, &["bob"]).await;
    sees(&mut bob, &w2, &["alice", "carol"]).await;
    unlink(&a, "ab").await;
    for ws in [&mut alice, &mut carol] {
        assert!(wait_for(ws, 10_000, |v| v["type"] == "presence"
            && v["event"] == "unreachable"
            && v["peer"] == "bob")
        .await
        .is_some());
    }
    presence_set(&mut bob, "unreachable", &["alice", "carol"]).await;
    (a, b, alice, carol, bob)
}

#[tokio::test]
async fn a_link_killed_mid_room_reports_unreachable_and_local_peers_keep_working() {
    let (a, b, mut alice, mut carol, mut bob) = killed_link().await;
    say(&mut alice, frame(1, json!("still here"))).await;
    let got = wait_for(&mut carol, 5000, is_frame).await.unwrap();
    assert_eq!(got["payload"], "still here");
    say(&mut carol, frame(1, json!("yes"))).await;
    assert_eq!(
        wait_for(&mut alice, 5000, is_frame).await.unwrap()["payload"],
        "yes"
    );
    // The far side hears nothing from this side any more.
    assert!(frames_within(&mut bob, 500).await.is_empty());
    let (_dave, welcome) = peer(&b, "dave").await;
    assert!(
        !welcome.iter().any(|p| p == "alice" || p == "carol"),
        "{welcome:?}"
    );
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn recovery_restores_presence_and_frames() {
    let (a, b, mut alice, mut carol, mut bob) = killed_link().await;
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    for ws in [&mut alice, &mut carol] {
        assert!(wait_for(ws, 10_000, |v| v["type"] == "presence"
            && v["event"] == "join"
            && v["peer"] == "bob")
        .await
        .is_some());
    }
    presence_set(&mut bob, "join", &["alice", "carol"]).await;
    say(&mut alice, frame(1, json!("again"))).await;
    assert_eq!(
        wait_for(&mut bob, 5000, is_frame).await.unwrap()["payload"],
        "again"
    );
    say(&mut bob, frame(1, json!("welcome back"))).await;
    for ws in [&mut alice, &mut carol] {
        assert!(wait_for(ws, 5000, |v| v["payload"] == "welcome back")
            .await
            .is_some());
    }
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn identical_frames_from_one_peer_both_cross_the_link() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    // seq is client-supplied, so a repeated byte-identical frame is still a new frame.
    let text =
        json!({"v":1,"type":"message","channel":"updates","seq":1,"payload":"same"}).to_string();
    say(&mut alice, Message::Text(text.clone().into())).await;
    say(&mut alice, Message::Text(text.into())).await;
    assert_eq!(frames_within(&mut bob, 1000).await.len(), 2);
    a.task.abort();
    b.task.abort();
}

async fn busy_nodes(channels: Option<Value>) -> (Node, Node, Vec<Ws>, Ws) {
    let a = node_with(
        "a",
        &["p1", "p2", "p3", "p4", "svc-b"],
        channels.clone(),
        None,
    )
    .await;
    let b = node_with("b", &["bob", "svc-a"], channels, None).await;
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    let (mut bob, bob_welcome) = peer(&b, "bob").await;
    let mut senders = vec![];
    for name in ["p1", "p2", "p3", "p4"] {
        let (mut ws, welcome) = peer(&a, name).await;
        sees(&mut ws, &welcome, &["bob"]).await;
        senders.push(ws);
    }
    sees(&mut bob, &bob_welcome, &["p4"]).await;
    (a, b, senders, bob)
}
/// Frames received by `ws` until it is quiet for `quiet_ms`, and whether the link was lost.
async fn drain(ws: &mut Ws, quiet_ms: u64) -> (usize, bool) {
    let (mut frames, mut lost) = (0, false);
    while let Some(v) = wait_for(ws, quiet_ms, |v| {
        is_frame(v) || (v["type"] == "presence" && v["event"] == "unreachable")
    })
    .await
    {
        if is_frame(&v) {
            frames += 1;
        } else {
            lost = true;
        }
    }
    (frames, lost)
}

#[tokio::test]
async fn a_busy_room_over_one_hundred_frames_a_second_keeps_the_link_up() {
    let (a, b, senders, mut bob) = busy_nodes(None).await;
    let mut tasks = vec![];
    for (n, mut ws) in senders.into_iter().enumerate() {
        tasks.push(tokio::spawn(async move {
            // 4 peers x 90 frames over about a second: 360 a second across the link.
            for seq in 1..=90u64 {
                say(&mut ws, frame(seq, json!(n))).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ws
        }));
    }
    let (frames, lost) = drain(&mut bob, 1500).await;
    assert!(!lost, "the link was dropped");
    assert_eq!(frames, 360);
    link_status(&a, "ab", "connected").await;
    let mut sockets = vec![];
    for task in tasks {
        sockets.push(task.await.unwrap());
    }
    say(&mut sockets[0], frame(1000, json!("after"))).await;
    assert!(wait_for(&mut bob, 5000, is_frame).await.is_some());
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn over_the_link_budget_lossy_frames_are_dropped_and_the_link_stays_up() {
    // A lower budget must not reach the other tests in this process.
    if std::env::var("LIVEENGINE_TEST_LINK_BUDGET").is_err() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "over_the_link_budget_lossy_frames_are_dropped_and_the_link_stays_up",
            ])
            .env("LIVEENGINE_TEST_LINK_BUDGET", "1")
            .env("LIVEENGINE_LINK_MAX_MESSAGES_PER_SECOND", "50")
            .status()
            .unwrap();
        assert!(status.success(), "link budget test failed");
        return;
    }
    let channels = json!({"updates":{"delivery":"lossy"}});
    let (a, b, senders, mut bob) = busy_nodes(Some(channels)).await;
    let mut tasks = vec![];
    for (n, mut ws) in senders.into_iter().enumerate() {
        tasks.push(tokio::spawn(async move {
            for seq in 1..=60u64 {
                say(&mut ws, frame(seq, json!(n))).await;
            }
            ws
        }));
    }
    let (frames, lost) = drain(&mut bob, 1500).await;
    assert!(!lost, "the link was dropped");
    assert!(frames > 0 && frames < 240, "{frames}");
    assert!(counters(&a).await.0 + counters(&b).await.0 > 0);
    let mut sockets = vec![];
    for task in tasks {
        sockets.push(task.await.unwrap());
    }
    link_status(&a, "ab", "connected").await;
    say(&mut sockets[0], frame(1000, json!("after"))).await;
    assert!(wait_for(&mut bob, 5000, is_frame).await.is_some());
    a.task.abort();
    b.task.abort();
}

/// Wait until a fresh peer on `node` is told about `names`.
async fn welcome_lists(node: &Node, user: &str, names: &[&str]) {
    for _ in 0..50 {
        let (mut ws, welcome) = peer(node, user).await;
        let _ = ws.close(None).await;
        if names.iter().all(|n| welcome.iter().any(|p| p == n)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("{user} never saw {names:?}");
}
/// Each of the three peers sends once; the other two hear it exactly once.
async fn everyone_hears_everyone_once(peers: &mut [(&str, &mut Ws)], round: u64) {
    for i in 0..peers.len() {
        let tag = format!("{round}-{}", peers[i].0);
        say(&mut *peers[i].1, frame(round, json!(tag))).await;
        for (j, (name, ws)) in peers.iter_mut().enumerate() {
            if i != j {
                let got = wait_for(ws, 5000, |v| v["payload"] == tag.as_str()).await;
                assert!(got.is_some(), "{name} never heard {tag}");
            }
        }
        for (name, ws) in peers.iter_mut() {
            assert!(
                wait_for(ws, 300, is_frame).await.is_none(),
                "{name} got a stray or duplicate frame"
            );
        }
    }
}

#[tokio::test]
async fn a_ring_survives_losing_one_link_and_recovers_it() {
    let a = node("a", &["alice", "dave", "svc-c"]).await;
    let b = node("b", &["bob", "dave", "svc-a"]).await;
    let c = node("c", &["carol", "dave", "svc-b"]).await;
    link(&a, &b, "ab").await;
    link(&b, &c, "bc").await;
    link(&c, &a, "ca").await;
    for (n, id) in [(&a, "ab"), (&b, "bc"), (&c, "ca")] {
        link_status(n, id, "connected").await;
    }
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    let (mut carol, w3) = peer(&c, "carol").await;
    for (ws, w, names) in [
        (&mut alice, &w1, ["bob", "carol"]),
        (&mut bob, &w2, ["alice", "carol"]),
        (&mut carol, &w3, ["alice", "bob"]),
    ] {
        sees(ws, w, &names).await;
    }
    // Kill one link: the other two still connect all three services.
    unlink(&a, "ab").await;
    welcome_lists(&a, "dave", &["bob", "carol"]).await;
    welcome_lists(&b, "dave", &["alice", "carol"]).await;
    welcome_lists(&c, "dave", &["alice", "bob"]).await;
    everyone_hears_everyone_once(
        &mut [
            ("alice", &mut alice),
            ("bob", &mut bob),
            ("carol", &mut carol),
        ],
        1,
    )
    .await;
    // Recover it: presence and frames stay correct, with no duplicates.
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    welcome_lists(&a, "dave", &["bob", "carol"]).await;
    welcome_lists(&b, "dave", &["alice", "carol"]).await;
    welcome_lists(&c, "dave", &["alice", "bob"]).await;
    everyone_hears_everyone_once(
        &mut [
            ("alice", &mut alice),
            ("bob", &mut bob),
            ("carol", &mut carol),
        ],
        2,
    )
    .await;
    for n in [a, b, c] {
        n.task.abort();
    }
}

/// Presence joins carried by different links, or by a snapshot after a late attach, reach a peer
/// in no fixed order; waiting for one name must not lose the join of another.
#[tokio::test]
async fn joins_reach_a_peer_in_any_order() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let n = node("order", &["observer", "svc-one"]).await;
    let (mut observer, welcome) = peer(&n, "observer").await;
    let issued: Value = post(
        &n.base,
        &n.token,
        "/v1/hubs/game/tickets",
        json!({"userId":"svc-one","serverId":"room","role":"service"}),
    )
    .await
    .json()
    .await
    .unwrap();
    let (mut service, _) = connect_async(format!(
        "{}/v1/live",
        n.base.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    service
        .send(Message::Text(
            json!({"type":"auth","ticket":issued["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(service
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .contains("welcome"));
    // The far side announces carol first, then bob; the observer asks for bob first.
    for name in ["carol", "bob"] {
        say(
            &mut service,
            Message::Text(
                json!({"type":"link.presence","trace":["remote"],"event":"join","peer":name})
                    .to_string()
                    .into(),
            ),
        )
        .await;
    }
    sees(&mut observer, &welcome, &["bob", "carol"]).await;
    n.task.abort();
}

#[tokio::test]
async fn linked_sender_must_be_announced_on_the_same_link() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let n = node("claims", &["observer", "svc-one", "svc-two"]).await;
    let (mut observer, _) = peer(&n, "observer").await;
    let mut services = Vec::new();
    for user in ["svc-one", "svc-two"] {
        let issued: Value = post(
            &n.base,
            &n.token,
            "/v1/hubs/game/tickets",
            json!({"userId":user,"serverId":"room","role":"service"}),
        )
        .await
        .json()
        .await
        .unwrap();
        let (mut ws, _) = connect_async(format!(
            "{}/v1/live",
            n.base.replacen("http://", "ws://", 1)
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({"type":"auth","ticket":issued["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&ws.next().await.unwrap().unwrap().into_text().unwrap())
                .unwrap()["type"],
            "welcome"
        );
        services.push(ws);
    }
    for (ws, name) in services.iter_mut().zip(["remote-one", "remote-two"]) {
        say(
            ws,
            Message::Text(
                json!({"type":"link.presence","trace":["remote"],"event":"join","peer":name})
                    .to_string()
                    .into(),
            ),
        )
        .await;
        assert!(wait_for(&mut observer, 5000, |v| v["type"] == "presence"
            && v["peer"] == name)
        .await
        .is_some());
    }
    let before = counters(&n).await.1;
    for (id, sender) in [
        ("forged-unknown", "ghost"),
        ("forged-other", "remote-two"),
        ("honest", "remote-one"),
    ] {
        say(&mut services[0], Message::Text(json!({"type":"link.frame","id":id,"trace":["remote"],"frame":{"from":sender,"v":1,"type":"message","channel":"updates","seq":1,"payload":id}}).to_string().into())).await;
    }
    let got = wait_for(&mut observer, 5000, is_frame).await.unwrap();
    assert_eq!(got["payload"], "honest");
    assert_eq!(got["from"], "remote-one");
    assert!(frames_within(&mut observer, 300).await.is_empty());
    assert_eq!(counters(&n).await.1, before + 2);
    n.task.abort();
}

#[tokio::test]
async fn remote_connection_can_be_listed_and_kicked_by_its_owner() {
    let (a, b) = pair().await;
    let (mut alice, w1) = peer(&a, "alice").await;
    let (mut bob, w2) = peer(&b, "bob").await;
    sees(&mut alice, &w1, &["bob"]).await;
    sees(&mut bob, &w2, &["alice"]).await;
    let client = reqwest::Client::new();
    let remote = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let values: Value = client
                .get(format!("{}/v1/connections?user=bob", a.base))
                .bearer_auth(&a.token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if let Some(info) = values
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["remote"] == true)
            {
                break info.clone();
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    assert!(!remote.to_string().contains(&b.token));
    assert_eq!(
        client
            .delete(format!(
                "{}/v1/connections/{}",
                a.base,
                remote["id"].as_str().unwrap()
            ))
            .bearer_auth(&a.token)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert!(wait_for(&mut bob, 5000, |v| v["type"] == "error"
        && v["code"] == "kicked")
    .await
    .is_some());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_bob, _) = peer(&b, "bob").await;
    a.task.abort();
    b.task.abort();
}

#[tokio::test]
async fn banning_a_linked_password_peer_closes_its_own_socket() {
    use futures_util::SinkExt;
    let token = "linked-password-administrator-token-123456789";
    let service = HubService::new(HubConfig {
        token: token.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..Default::default()
    })
    .unwrap();
    let (base, task) = serve(service).await;
    let a = Node {
        name: "a".into(),
        base,
        token: token.into(),
        task,
    };
    let b = node("b", &["bob", "svc-a"]).await;
    assert_eq!(
        post(
            &a.base,
            &a.token,
            "/v1/accounts",
            json!({"username":"alice","password":"password-for-alice-123","role":"user"})
        )
        .await
        .status(),
        201
    );
    let session: Value = post(
        &a.base,
        &a.token,
        "/v1/sessions",
        json!({"username":"alice","password":"password-for-alice-123"}),
    )
    .await
    .json()
    .await
    .unwrap();
    assert_eq!(
        post(
            &a.base,
            &a.token,
            "/v1/hubs",
            json!({"id":"game","mode":"internal","auth":{"kind":"password"}})
        )
        .await
        .status(),
        201
    );
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room"}])),
        ("join", json!(["alice", "room"])),
    ] {
        assert_eq!(
            post(
                &a.base,
                &a.token,
                "/v1/hubs/game/call",
                json!({"method":method,"args":args})
            )
            .await
            .status(),
            200
        );
    }
    link(&a, &b, "ab").await;
    link_status(&a, "ab", "connected").await;
    let issued: Value = post(
        &a.base,
        &a.token,
        "/v1/hubs/game/tickets",
        json!({"userId":"alice","serverId":"room","credential":session["token"]}),
    )
    .await
    .json()
    .await
    .unwrap();
    let (mut alice, _) = tokio_tungstenite::connect_async(format!(
        "{}/v1/live",
        a.base.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    alice
        .send(Message::Text(
            json!({"type":"auth","ticket":issued["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(wait_for(&mut alice, 5000, |v| v["type"] == "welcome")
        .await
        .is_some());
    let (mut bob, welcome) = peer(&b, "bob").await;
    sees(&mut bob, &welcome, &["alice"]).await;
    assert_eq!(
        reqwest::Client::new()
            .patch(format!("{}/v1/accounts/alice", a.base))
            .bearer_auth(&a.token)
            .json(&json!({"enabled":false}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(wait_for(&mut alice, 5000, |v| v["type"] == "error"
        && v["code"] == "banned")
    .await
    .is_some());
    assert!(wait_for(&mut bob, 5000, |v| v["type"] == "presence"
        && v["event"] == "leave"
        && v["peer"] == "alice")
    .await
    .is_some());
    a.task.abort();
    b.task.abort();
}
