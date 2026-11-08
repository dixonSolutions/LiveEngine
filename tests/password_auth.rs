#![cfg(feature = "host")]
use futures_util::{SinkExt, StreamExt};
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio_tungstenite::tungstenite::Message;
const ADMIN: &str = "password-auth-admin-token-0123456789";
struct Server {
    base: String,
    service: HubService,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server() -> Server {
    let service = HubService::new(HubConfig {
        token: ADMIN.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        rate_limit_requests: 10000,
        ..Default::default()
    })
    .unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let clone = service.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            l,
            clone
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        base,
        service,
        task,
    }
}
async fn request(s: &Server, method: reqwest::Method, path: &str, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new()
        .request(method, format!("{}{path}", s.base))
        .bearer_auth(ADMIN)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = r.status().as_u16();
    (status, r.json().await.unwrap_or(Value::Null))
}
async fn post(s: &Server, path: &str, body: Value) -> (u16, Value) {
    request(s, reqwest::Method::POST, path, body).await
}
async fn account(s: &Server, name: &str, role: &str) -> String {
    let (status, b) = post(s,"/v1/accounts",json!({"username":name,"password":"a-long-password-123","role":role,"claims":{"role":"spoof","color":"blue"}})).await;
    assert_eq!(status, 201, "{b}");
    let (status, b) = post(
        s,
        "/v1/sessions",
        json!({"username":name,"password":"a-long-password-123"}),
    )
    .await;
    assert_eq!(status, 200, "{b}");
    b["token"].as_str().unwrap().to_owned()
}
async fn call(s: &Server, hub: &str, method: &str, args: Value) {
    let (status, b) = post(
        s,
        &format!("/v1/hubs/{hub}/call"),
        json!({"method":method,"args":args}),
    )
    .await;
    assert_eq!(status, 200, "{b}");
}
async fn frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    loop {
        let m = socket.next().await.unwrap().unwrap();
        if let Message::Text(t) = m {
            return serde_json::from_str(&t).unwrap();
        }
    }
}
#[tokio::test]
async fn password_hub_roles_tickets_welcome_presence_and_disabled() {
    let s = server().await;
    let alice = account(&s, "alice", "user").await;
    let bob = account(&s, "bob", "admin").await;
    assert_eq!(
        post(
            &s,
            "/v1/hubs",
            json!({"id":"game","mode":"internal","auth":{"kind":"password"}})
        )
        .await
        .0,
        201
    );
    for u in ["alice", "bob"] {
        call(&s, "game", "upsertUser", json!([{"id":u}])).await;
    }
    call(
        &s,
        "game",
        "upsertServer",
        json!([{"id":"room","capacity":2}]),
    )
    .await;
    for u in ["alice", "bob"] {
        call(&s, "game", "join", json!([u, "room"])).await;
    }
    let ticket = |u: &str, c: Value| json!({"userId":u,"serverId":"room","credential":c});
    assert_eq!(
        post(&s, "/v1/hubs/game/tickets", ticket("alice", Value::Null))
            .await
            .0,
        403
    );
    assert_eq!(
        post(&s, "/v1/hubs/game/tickets", ticket("bob", json!(alice)))
            .await
            .0,
        403
    );
    let (status, a) = post(&s, "/v1/hubs/game/tickets", ticket("alice", json!(alice))).await;
    assert_eq!(status, 200, "{a}");
    assert_eq!(
        a["identity"],
        json!({"identity":"alice","claims":{"role":"user","color":"blue"}})
    );
    let verified = s
        .service
        .verify_ticket(a["ticket"].as_str().unwrap(), &["game".into()], "room")
        .unwrap();
    assert_eq!(verified.identity.unwrap().identity, "alice");
    let (_, a) = post(&s, "/v1/hubs/game/tickets", ticket("alice", json!(alice))).await;
    let (_, b) = post(&s, "/v1/hubs/game/tickets", ticket("bob", json!(bob))).await;
    let ws = s.base.replacen("http:", "ws:", 1) + "/v1/live";
    let (mut x, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
    x.send(Message::Text(
        json!({"type":"auth","ticket":a["ticket"]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let welcome = frame(&mut x).await;
    assert_eq!(welcome["identity"]["identity"], "alice");
    let (mut y, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
    y.send(Message::Text(
        json!({"type":"auth","ticket":b["ticket"]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let welcome = frame(&mut y).await;
    assert_eq!(welcome["identity"]["claims"]["role"], "admin");
    assert_eq!(welcome["peerIdentities"]["alice"]["identity"], "alice");
    let joined = frame(&mut x).await;
    assert_eq!(joined["identity"]["identity"], "bob");
    assert_eq!(joined["identity"]["claims"]["role"], "admin");
    assert_eq!(post(&s, "/v1/hubs", json!({"id":"restricted","mode":"internal","auth":{"kind":"password","roles":["admin"]}})).await.0, 201);
    assert_eq!(post(&s, "/v1/route", json!({"path":[],"op":"authorize","body":{"id":"restricted","userId":"alice","serverId":"room","credential":alice}})).await.0, 403);
    assert_eq!(
        request(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/alice",
            json!({"enabled":false})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        post(&s, "/v1/hubs/game/tickets", ticket("alice", json!(alice)))
            .await
            .0,
        403
    );
    assert_eq!(post(&s,"/v1/route",json!({"path":[],"op":"authorize","body":{"id":"restricted","userId":"alice","serverId":"room","credential":alice}})).await.0,403);
    assert_eq!(post(&s,"/v1/route",json!({"path":[],"op":"authorize","body":{"id":"restricted","userId":"bob","serverId":"room","credential":bob}})).await.0,200);
}

#[tokio::test]
async fn password_auth_is_verified_by_external_owner_and_remote_central() {
    let owner = server().await;
    let root = server().await;
    let session = account(&owner, "alice", "user").await;
    let local_session = account(&root, "alice", "admin").await;
    assert_eq!(
        post(
            &owner,
            "/v1/hubs",
            json!({"id":"game","mode":"internal","auth":{"kind":"password","roles":["user"]}})
        )
        .await
        .0,
        201
    );
    call(&owner, "game", "upsertUser", json!([{"id":"alice"}])).await;
    call(&owner, "game", "upsertServer", json!([{"id":"room"}])).await;
    call(&owner, "game", "join", json!(["alice", "room"])).await;
    for (id, mode) in [("proxy", "external"), ("nest", "central")] {
        let mut body = json!({"id":id,"mode":mode,"address":owner.base,"remoteToken":ADMIN});
        if mode == "external" {
            body["remoteHubId"] = json!("game");
        }
        let (status, b) = post(&root, "/v1/hubs", body).await;
        assert_eq!(status, 201, "{b}");
    }
    let proxy =
        |credential: &str| json!({"userId":"alice","serverId":"room","credential":credential});
    assert_eq!(
        post(&root, "/v1/hubs/proxy/tickets", proxy(&local_session))
            .await
            .0,
        403
    );
    let (status, b) = post(&root, "/v1/hubs/proxy/tickets", proxy(&session)).await;
    assert_eq!(status, 200, "{b}");
    assert_eq!(b["identity"]["claims"]["role"], "user");
    let route = |credential: &str| json!({"path":["nest"],"op":"ticket","body":{"id":"game","userId":"alice","serverId":"room","credential":credential}});
    assert_eq!(post(&root, "/v1/route", route(&local_session)).await.0, 403);
    let (status, b) = post(&root, "/v1/route", route(&session)).await;
    assert_eq!(status, 200, "{b}");
    assert_eq!(b["identity"]["identity"], "alice");
    let exports = reqwest::Client::new()
        .get(format!("{}/v1/registry/export", owner.base))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!exports.contains(&session));
    assert!(!exports.contains("accounts"));
}

#[tokio::test]
async fn peer_pair_requires_both_sessions_and_reports_verified_identities() {
    let s = std::sync::Arc::new(server().await);
    let a = account(&s, "alice", "user").await;
    let b = account(&s, "bob", "admin").await;
    assert_eq!(
        post(
            &s,
            "/v1/hubs",
            json!({"id":"game","mode":"internal","auth":{"kind":"password"}})
        )
        .await
        .0,
        201
    );
    for u in ["alice", "bob"] {
        call(
            &s,
            "game",
            "upsertUser",
            json!([{"id":u,"scopes":{"game":"g"}}]),
        )
        .await;
    }
    let pair = |id: &str, credential: &str, wait| json!({"subject":{"kind":"peer","id":id,"scopes":{"game":"g"},"credential":credential},"target":{"kind":"peer","requirements":{"game":"g"}},"waitMs":wait});
    assert_eq!(post(&s, "/v1/pair", pair("alice", &a, 0)).await.0, 403);
    let (status, _) = post(&s, "/v1/pair", pair("alice", &b, 0)).await;
    assert_eq!(status, 403);
    let waiting = {
        let s = s.clone();
        let body = pair("alice", &a, 3000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, bob) = post(&s, "/v1/pair", pair("bob", &b, 0)).await;
    assert_eq!(status, 200, "{bob}");
    let (status, alice) = waiting.await.unwrap();
    assert_eq!(status, 200, "{alice}");
    assert_eq!(alice["identity"]["identity"], "alice");
    assert_eq!(alice["identity"]["claims"]["role"], "user");
    assert_eq!(bob["identity"]["identity"], "bob");
    assert_eq!(bob["identity"]["claims"]["role"], "admin");
    assert_eq!(alice["room"], bob["room"]);
    let ws = s.base.replacen("http:", "ws:", 1) + "/v1/live";
    let (mut first, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
    first
        .send(Message::Text(
            json!({"type":"auth","ticket":alice["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), frame(&mut first))
        .await
        .unwrap();
    let (mut second, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
    second
        .send(Message::Text(
            json!({"type":"auth","ticket":bob["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let welcome = tokio::time::timeout(std::time::Duration::from_secs(5), frame(&mut second))
        .await
        .unwrap();
    assert_eq!(welcome["peerIdentities"]["alice"]["identity"], "alice");
    assert_eq!(welcome["peerIdentities"]["alice"]["claims"]["role"], "user");
    let presence = tokio::time::timeout(std::time::Duration::from_secs(5), frame(&mut first))
        .await
        .unwrap();
    assert_eq!(presence["identity"]["identity"], "bob");
    assert_eq!(presence["identity"]["claims"]["role"], "admin");
}
