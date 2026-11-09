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
    assert_eq!(post(&s, "/v1/pair", pair("alice", &a, 0)).await.0, 404);
    let waiting = {
        let s = s.clone();
        let body = pair("bob", &b, 3000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, _) = post(&s, "/v1/pair", pair("alice", &b, 0)).await;
    assert_eq!(status, 403);
    let (status, alice) = post(&s, "/v1/pair", pair("alice", &a, 0)).await;
    assert_eq!(status, 200, "{alice}");
    let (status, bob) = waiting.await.unwrap();
    assert_eq!(status, 200, "{bob}");
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

#[tokio::test]
async fn a_waiting_peer_joins_a_seated_room_with_only_its_own_session() {
    let s = std::sync::Arc::new(server().await);
    let sessions = [
        account(&s, "alice", "user").await,
        account(&s, "bob", "user").await,
        account(&s, "carol", "user").await,
    ];
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
    for u in ["alice", "bob", "carol"] {
        call(
            &s,
            "game",
            "upsertUser",
            json!([{"id":u,"scopes":{"game":"g"}}]),
        )
        .await;
    }
    let pair = |id: &str, credential: &str, wait| json!({"subject":{"kind":"peer","id":id,"scopes":{"game":"g"},"credential":credential},"target":{"kind":"peer","requirements":{"game":"g"},"capacity":3},"waitMs":wait});
    let waiting = {
        let s = s.clone();
        let body = pair("alice", &sessions[0], 5000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, bob) = post(&s, "/v1/pair", pair("bob", &sessions[1], 5000)).await;
    assert_eq!(status, 200, "{bob}");
    let (status, alice) = waiting.await.unwrap();
    assert_eq!(status, 200, "{alice}");
    // Carol waits like a page does. She finds a seated room with a free place and joins it; the
    // seated peers' sessions are not hers to present.
    let (status, carol) = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        post(&s, "/v1/pair", pair("carol", &sessions[2], 5000)),
    )
    .await
    .expect("carol was left waiting for a partner who is already seated");
    assert_eq!(status, 200, "{carol}");
    assert_eq!(carol["room"], alice["room"]);
    assert_eq!(carol["identity"]["identity"], "carol");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peers_pairing_at_once_under_a_password_hub_all_find_a_room() {
    let s = std::sync::Arc::new(server().await);
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
    let names: Vec<String> = (0..8).map(|i| format!("crowd-{i}")).collect();
    let mut sessions = Vec::new();
    for name in &names {
        sessions.push(account(&s, name, "user").await);
        call(
            &s,
            "game",
            "upsertUser",
            json!([{"id":name,"scopes":{"game":"g"}}]),
        )
        .await;
    }
    // Every page asks at the same moment, as a crowd pressing Find match does. A peer that finds a
    // candidate which has not queued yet (or a room with a free place) must not be left waiting.
    let mut tasks = Vec::new();
    for (name, session) in names.iter().zip(&sessions) {
        let s = s.clone();
        let body = json!({"subject":{"kind":"peer","id":name,"scopes":{"game":"g"},"credential":session},"target":{"kind":"peer","requirements":{"game":"g"},"capacity":4},"waitMs":6000});
        tasks.push(tokio::spawn(
            async move { post(&s, "/v1/pair", body).await },
        ));
    }
    let started = std::time::Instant::now();
    let mut rooms = std::collections::HashMap::<String, usize>::new();
    for task in tasks {
        let (status, answer) = task.await.unwrap();
        assert_eq!(status, 200, "{answer}");
        *rooms
            .entry(answer["room"].as_str().unwrap().to_owned())
            .or_default() += 1;
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "a peer waited out its time"
    );
    assert_eq!(rooms.values().sum::<usize>(), 8);
    assert!(
        rooms.values().all(|members| (2..=4).contains(members)),
        "{rooms:?}"
    );
}

#[tokio::test]
async fn connections_kick_ban_and_unban() {
    let s = server().await;
    let session = account(&s, "alice", "user").await;
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
    call(&s, "game", "upsertUser", json!([{"id":"alice"}])).await;
    call(&s, "game", "upsertServer", json!([{"id":"room"}])).await;
    call(&s, "game", "join", json!(["alice", "room"])).await;
    let ticket =
        |credential: &str| json!({"userId":"alice","serverId":"room","credential":credential});
    let connect = |ticket: Value| {
        let ws = s.base.replacen("http:", "ws:", 1) + "/v1/live";
        async move {
            let (mut socket, _) = tokio_tungstenite::connect_async(ws).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"auth","ticket":ticket}).to_string().into(),
                ))
                .await
                .unwrap();
            assert_eq!(frame(&mut socket).await["type"], "welcome");
            socket
        }
    };
    let (_, t) = post(&s, "/v1/hubs/game/tickets", ticket(&session)).await;
    let mut socket = connect(t["ticket"].clone()).await;
    let (_, list) = request(
        &s,
        reqwest::Method::GET,
        "/v1/connections?hub=game&room=room&user=alice",
        Value::Null,
    )
    .await;
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["hub"], json!(["game"]));
    assert_eq!(list[0]["transport"], "websocket");
    assert_eq!(list[0]["remote"], false);
    assert!(!list[0].to_string().contains(&session));
    let id = list[0]["id"].as_str().unwrap();
    let denied = reqwest::Client::new()
        .get(format!("{}/v1/connections", s.base))
        .bearer_auth(&session)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    assert_eq!(
        request(
            &s,
            reqwest::Method::DELETE,
            &format!("/v1/connections/{id}"),
            Value::Null
        )
        .await
        .0,
        204
    );
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(3), frame(&mut socket))
            .await
            .unwrap()["code"],
        "kicked"
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (_, t) = post(&s, "/v1/hubs/game/tickets", ticket(&session)).await;
    let mut socket = connect(t["ticket"].clone()).await;
    let (_, unused) = post(&s, "/v1/hubs/game/tickets", ticket(&session)).await;
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
        tokio::time::timeout(std::time::Duration::from_secs(3), frame(&mut socket))
            .await
            .unwrap()["code"],
        "banned"
    );
    assert_eq!(
        post(&s, "/v1/hubs/game/tickets", ticket(&session)).await.0,
        403
    );
    assert_eq!(post(&s, "/v1/pair", json!({"subject":{"kind":"peer","id":"alice","scopes":{},"credential":session},"target":{"kind":"hub","requirements":{}}})).await.0, 403);
    let ws = s.base.replacen("http:", "ws:", 1) + "/v1/live";
    let (mut stale, _) = tokio_tungstenite::connect_async(ws).await.unwrap();
    stale
        .send(Message::Text(
            json!({"type":"auth","ticket":unused["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(frame(&mut stale).await["code"], "banned");
    assert_eq!(
        request(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/alice",
            json!({"enabled":true})
        )
        .await
        .0,
        200
    );
    let (_, login) = post(
        &s,
        "/v1/sessions",
        json!({"username":"alice","password":"a-long-password-123"}),
    )
    .await;
    let (status, renewed) = post(
        &s,
        "/v1/hubs/game/tickets",
        ticket(login["token"].as_str().unwrap()),
    )
    .await;
    assert_eq!(status, 200);
    let _reconnected = connect(renewed["ticket"].clone()).await;
}

#[tokio::test]
async fn generation_changes_invalidate_tickets_and_close_only_owner_sockets() {
    for change in ["password", "reset", "role", "disable", "delete"] {
        let s = server().await;
        let alice = account(&s, "alice", "admin").await;
        let bob = account(&s, "bob", "user").await;
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
        for user in ["alice", "bob"] {
            call(&s, "game", "upsertUser", json!([{"id":user}])).await;
        }
        call(
            &s,
            "game",
            "upsertServer",
            json!([{"id":"room","capacity":2}]),
        )
        .await;
        for user in ["alice", "bob"] {
            call(&s, "game", "join", json!([user, "room"])).await;
        }
        let issue = |user: &str, credential: &str| json!({"userId":user,"serverId":"room","credential":credential});
        let (_, active) = post(&s, "/v1/hubs/game/tickets", issue("alice", &alice)).await;
        let (_, unused) = post(&s, "/v1/hubs/game/tickets", issue("alice", &alice)).await;
        let (_, other) = post(&s, "/v1/hubs/game/tickets", issue("bob", &bob)).await;
        let ws = s.base.replacen("http:", "ws:", 1) + "/v1/live";
        let (mut a, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
        a.send(Message::Text(
            json!({"type":"auth","ticket":active["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        assert_eq!(frame(&mut a).await["type"], "welcome");
        let (mut b, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
        b.send(Message::Text(
            json!({"type":"auth","ticket":other["ticket"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        assert_eq!(frame(&mut b).await["type"], "welcome");
        assert_eq!(frame(&mut a).await["type"], "presence");
        let status = match change {
            "password" => reqwest::Client::new()
                .post(format!("{}/v1/accounts/me/password", s.base))
                .bearer_auth(&alice)
                .json(&json!({"current":"a-long-password-123","next":"new-long-password-123"}))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            "reset" => {
                request(
                    &s,
                    reqwest::Method::PATCH,
                    "/v1/accounts/alice",
                    json!({"password":"new-long-password-123"}),
                )
                .await
                .0
            }
            "role" => {
                request(
                    &s,
                    reqwest::Method::PATCH,
                    "/v1/accounts/alice",
                    json!({"role":"user"}),
                )
                .await
                .0
            }
            "disable" => {
                request(
                    &s,
                    reqwest::Method::PATCH,
                    "/v1/accounts/alice",
                    json!({"enabled":false}),
                )
                .await
                .0
            }
            _ => {
                request(
                    &s,
                    reqwest::Method::DELETE,
                    "/v1/accounts/alice",
                    Value::Null,
                )
                .await
                .0
            }
        };
        assert!(status == 200 || status == 204, "{change}: {status}");
        let code = match change {
            "disable" => "banned",
            "delete" => "account-deleted",
            _ => "credentials-changed",
        };
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(3), frame(&mut a))
                .await
                .unwrap()["code"],
            code,
            "{change}"
        );
        let (mut stale, _) = tokio_tungstenite::connect_async(&ws).await.unwrap();
        stale
            .send(Message::Text(
                json!({"type":"auth","ticket":unused["ticket"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(frame(&mut stale).await["type"], "error", "{change}");
        let (_, still_valid) = post(&s, "/v1/hubs/game/tickets", issue("bob", &bob)).await;
        assert!(still_valid["ticket"].is_string(), "{change}");
        // Bob receives Alice's presence departure but is not disconnected.
        let event = tokio::time::timeout(std::time::Duration::from_secs(3), frame(&mut b))
            .await
            .unwrap();
        assert_ne!(event["type"], "error", "{change}");
    }
}

/// Password auth is opt-in per hub: accounts on a service never lock down its open hubs.
#[tokio::test]
async fn open_hubs_stay_open_when_the_service_has_accounts() {
    let s = std::sync::Arc::new(server().await);
    account(&s, "alice", "admin").await;
    for (id, body) in [
        (
            "open",
            json!({"id":"open","mode":"internal","scopes":{"game":"open"}}),
        ),
        (
            "locked",
            json!({"id":"locked","mode":"internal","scopes":{"game":"locked"},"auth":{"kind":"password"}}),
        ),
    ] {
        assert_eq!(post(&s, "/v1/hubs", body).await.0, 201, "{id}");
        for u in [format!("{id}-a"), format!("{id}-b")] {
            call(&s, id, "upsertUser", json!([{"id":u,"scopes":{"game":id}}])).await;
        }
    }
    let pair = |hub: &str, id: &str, wait| json!({"subject":{"kind":"peer","id":id,"scopes":{"game":hub}},"target":{"kind":"peer","requirements":{"game":hub}},"waitMs":wait});
    let locked_waiter = {
        let s = s.clone();
        let body = pair("locked", "locked-b", 3000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        post(&s, "/v1/pair", pair("locked", "locked-a", 0)).await.0,
        403
    );
    locked_waiter.abort();
    let waiting = {
        let s = s.clone();
        let body = pair("open", "open-a", 3000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, b) = post(&s, "/v1/pair", pair("open", "open-b", 0)).await;
    assert_eq!(status, 200, "{b}");
    let (status, a) = waiting.await.unwrap();
    assert_eq!(status, 200, "{a}");
    assert_eq!(a["room"], b["room"]);
    assert!(a.get("identity").is_none_or(Value::is_null), "{a}");
    assert!(a["ticket"].is_string() && b["ticket"].is_string());
}

/// The waiter and the arrival must agree: both paired into one room, or neither.
#[tokio::test]
async fn an_open_hub_pairs_a_waiter_and_an_arrival_consistently() {
    let s = std::sync::Arc::new(server().await);
    assert_eq!(
        post(&s, "/v1/hubs", json!({"id":"game","mode":"internal"}))
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
    let pair = |id: &str, wait| json!({"subject":{"kind":"peer","id":id,"scopes":{"game":"g"}},"target":{"kind":"peer","requirements":{"game":"g"}},"waitMs":wait});
    let waiting = {
        let s = s.clone();
        let body = pair("alice", 3000);
        tokio::spawn(async move { post(&s, "/v1/pair", body).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let (status, b) = post(&s, "/v1/pair", pair("bob", 0)).await;
    assert_eq!(status, 200, "the arrival must pair with the waiter: {b}");
    let (status, a) = waiting.await.unwrap();
    assert_eq!(status, 200, "{a}");
    assert_eq!(a["room"], b["room"]);
}
