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
    assert_eq!(post(&base,A,"/v1/links",json!({"id":"link","address":proxy_base,"remoteToken":B,"hubId":"game","userId":"service","serverId":"room"})).await.status(),201);
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
    assert!(!list.to_string().contains(B));
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
        .all(|frame| !frame.contains(A) && !frame.contains(B)));
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
    assert!(!matches!(
        tokio::time::timeout(Duration::from_secs(2), alice.next())
            .await
            .unwrap(),
        Some(Ok(Message::Text(_)))
    ));
    task.abort();
    remote_task.abort();
}
