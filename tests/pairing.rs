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
    let waiter = spawn_pair(
        &c,
        &remote,
        REMOTE,
        json!({"subject":{"kind":"peer","id":"alice","scopes":{"rank":7}},"target":{"kind":"peer","requirements":{"skill":2}},"waitMs":3000}),
    );
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    for (subject, target, link) in [
        (
            json!({"kind":"peer","id":"bob","scopes":{"rank":7,"skill":2}}),
            json!({"kind":"peer","requirements":{"rank":7},"capacity":3}),
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
    let (status, body) = waiter.await.unwrap();
    assert_eq!(status, 200, "{body}");
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

#[tokio::test]
async fn peer_pairing_uses_user_schema_and_rolls_back_failures() {
    let (base, task) = server(TOKEN).await;
    let c = reqwest::Client::new();
    let (status, body) = post(&c, &base, TOKEN, "/v1/hubs", json!({"id":"game","mode":"internal", "userSchema":{"type":"object","required":["rank"],"properties":{"rank":{"type":"integer"}}}, "serverSchema":{"type":"object","required":["host"],"properties":{"host":{"type":"string"}}}})).await;
    assert_eq!(status, 201, "{body}");
    let call = |method: &str, args: Value| json!({"method":method,"args":args});
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("upsertUser", json!([{"id":"alice","scopes":{"rank":1}}])),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let pair = |id: &str, scopes: Value| json!({"subject":{"kind":"peer","id":id,"scopes":scopes},"target":{"kind":"peer","requirements":{"rank":1}}});
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", pair("bad", json!({}))).await;
    assert_ne!(status, 200);
    let (_, users) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("listUsers", json!([])),
    )
    .await;
    assert_eq!(users["result"].as_array().unwrap().len(), 1);
    let (_, rooms) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        json!({"method":"listServers", "args":[]}),
    )
    .await;
    assert_eq!(rooms["result"], json!([]));
    let mut alice_request = pair("alice", json!({"rank":1}));
    alice_request["target"]["requirements"] = json!({"rank":2});
    alice_request["waitMs"] = json!(3000);
    let alice = spawn_pair(&c, &base, TOKEN, alice_request);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let (status, body) = post(&c, &base, TOKEN, "/v1/pair", pair("bob", json!({"rank":2}))).await;
    assert_eq!(status, 200, "{body}");
    let room = body["room"].clone();
    let (status, answer) = alice.await.unwrap();
    assert_eq!(status, 200, "{answer}");
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call(
            "upsertUser",
            json!([{"id":"candidate","scopes":{"rank":9}}]),
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", json!({"subject":{"kind":"peer","id":"bob","scopes":{"rank":2}},"target":{"kind":"peer","requirements":{"rank":9}}})).await;
    assert_ne!(status, 200);
    let (_, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("getServer", json!(["pair:candidate"])),
    )
    .await;
    assert_eq!(body["result"], Value::Null);
    let (_, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("getUser", json!(["candidate"])),
    )
    .await;
    assert_eq!(body["result"]["serverId"], Value::Null);
    let (_, result) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("upsertUser", json!([{"id":"other","scopes":{"rank":3}}])),
    )
    .await;
    assert_eq!(result["result"], true);
    let (_, result) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("findServersForUser", json!(["other"])),
    )
    .await;
    assert_eq!(result["result"], json!([]));
    let (status, _) = post(&c, &base, TOKEN, "/v1/hubs/game/call", call("pairPeers", json!([{"room":"failed","capacity":2,"users":[{"id":"new","scopes":{"rank":4}},{"id":"bob","scopes":{"rank":2}}]}]))).await;
    assert_ne!(status, 200);
    for (method, id) in [("getUser", "new"), ("getServer", "failed")] {
        let (_, body) = post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs/game/call",
            call(method, json!([id])),
        )
        .await;
        assert_eq!(body["result"], Value::Null);
    }
    for id in ["alice", "bob"] {
        post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs/game/call",
            call("leave", json!([id])),
        )
        .await;
    }
    let (_, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/game/call",
        call("getServer", json!([room])),
    )
    .await;
    assert_eq!(body["result"], Value::Null);
    task.abort();
}

#[tokio::test]
async fn pairing_target_id_picks_one_of_several_hubs_with_the_same_scopes() {
    let (base, task) = server(TOKEN).await;
    let c = reqwest::Client::new();
    for id in ["alpha", "beta"] {
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs",
            json!({"id":id,"mode":"internal","scopes":{"game":"same"}}),
        )
        .await;
        assert_eq!(status, 201, "{body}");
    }
    let pair =
        |target: Value| json!({"subject":{"kind":"peer","id":"zoe","scopes":{}},"target":target});
    // Without an id the first match in id order wins; with one, the named hub does.
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(json!({"kind":"hub","requirements":{"game":"same"}})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["room"], "pair:hub:alpha");
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(json!({"kind":"hub","id":"beta","requirements":{"game":"same"}})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["room"], "pair:hub:beta");
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(json!({"kind":"hub","id":"gamma"})),
    )
    .await;
    assert_eq!(status, 404);
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(json!({"kind":"hub","id":7})),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(json!({"kind":"hub","id":""})),
    )
    .await;
    assert_eq!(status, 400);
    task.abort();
}

#[tokio::test]
async fn peer_pairing_skips_candidates_seated_in_other_kinds_of_room() {
    let (base, task) = server(TOKEN).await;
    let c = reqwest::Client::new();
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs",
        json!({"id":"arena","mode":"internal","scopes":{"game":"g"}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let pair = |subject: &str, target: Value| json!({"path":[],"subject":{"kind":"peer","id":subject,"scopes":{"game":"g"}},"target":target});
    // "a-seated" sits in the hub's shared room, "b-app" in a room an application made. Both sort
    // before "c-waiting", which waits for a peer.
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair("a-seated", json!({"kind":"hub","id":"arena"})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["room"], "pair:hub:arena");
    for (method, args) in [
        ("upsertServer", json!([{"id":"app-room","capacity":4}])),
        ("upsertUser", json!([{"id":"b-app","scopes":{"game":"g"}}])),
        ("join", json!(["b-app", "app-room"])),
        (
            "upsertUser",
            json!([{"id":"c-waiting","scopes":{"game":"g"}}]),
        ),
    ] {
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs/arena/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let mut request = pair(
        "c-waiting",
        json!({"kind":"peer","requirements":{"game":"g"}}),
    );
    request["waitMs"] = json!(3000);
    let waiting = spawn_pair(&c, &base, TOKEN, request);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(
            "newcomer",
            json!({"kind":"peer","requirements":{"game":"g"},"capacity":3}),
        ),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["link"], "peer-peer");
    assert_eq!(body["room"], "pair:c-waiting");
    let (status, answer) = waiting.await.unwrap();
    assert_eq!(status, 200, "{answer}");
    // With the only eligible peer paired, a third joins that room, and a fourth finds only rooms it cannot join.
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair("third", json!({"kind":"peer","requirements":{"game":"g"}})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["room"], "pair:c-waiting");
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        pair(
            "fourth",
            json!({"kind":"peer","requirements":{"game":"other"}}),
        ),
    )
    .await;
    assert_eq!(status, 404);
    task.abort();
}

#[tokio::test]
async fn pairing_uses_room_flag_instead_of_room_name() {
    let (base, task) = server(TOKEN).await;
    let c = reqwest::Client::new();
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs",
        json!({"id":"arena","mode":"internal"}),
    )
    .await;
    assert_eq!(status, 201);
    for (method, args) in [
        (
            "upsertServer",
            json!([{"id":"pair:misleading","capacity":4}]),
        ),
        ("upsertUser", json!([{"id":"a","scopes":{}}])),
        ("join", json!(["a", "pair:misleading"])),
        (
            "pairPeers",
            json!([{"room":"unusual-name","capacity":4,"users":[{"id":"b","scopes":{}}]}]),
        ),
    ] {
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs/arena/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        json!({"subject":{"kind":"peer","id":"new","scopes":{}},"target":{"kind":"peer"}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["room"], "unusual-name");
    task.abort();
}

fn waiting_config(token: &str) -> HubConfig {
    HubConfig {
        token: token.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        public_url: Some("ws://live.example.test/v1/live".into()),
        request_timeout: std::time::Duration::from_secs(1),
        max_pair_wait: std::time::Duration::from_secs(3),
        ..HubConfig::default()
    }
}

async fn server_with(config: HubConfig) -> (String, tokio::task::JoinHandle<()>) {
    let service = HubService::new(config).unwrap();
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

async fn arena(c: &reqwest::Client, base: &str, token: &str) {
    let (status, body) = post(
        c,
        base,
        token,
        "/v1/hubs",
        json!({"id":"arena","mode":"internal","scopes":{"game":"g"}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
}

/// A peer registers as a candidate, as a game's bridge does before it asks to pair.
async fn register(c: &reqwest::Client, base: &str, token: &str, id: &str) {
    let (status, body) = post(
        c,
        base,
        token,
        "/v1/hubs/arena/call",
        json!({"method":"upsertUser","args":[{"id":id,"scopes":{"game":"g"}}]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
}

fn wait_request(id: &str, wait_ms: u64) -> Value {
    json!({
        "subject":{"kind":"peer","id":id,"scopes":{"game":"g"}},
        "target":{"kind":"peer","requirements":{"game":"g"}},
        "waitMs":wait_ms
    })
}

fn spawn_pair(
    c: &reqwest::Client,
    base: &str,
    token: &'static str,
    body: Value,
) -> tokio::task::JoinHandle<(u16, Value)> {
    let (c, base) = (c.clone(), base.to_owned());
    tokio::spawn(async move { post(&c, &base, token, "/v1/pair", body).await })
}

async fn users(c: &reqwest::Client, base: &str, token: &str) -> Vec<Value> {
    let (status, body) = post(
        c,
        base,
        token,
        "/v1/hubs/arena/call",
        json!({"method":"listUsers","args":[]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    body["result"].as_array().unwrap().clone()
}

#[tokio::test]
async fn a_waiting_peer_is_woken_by_the_later_arrival() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "early").await;
    let started = std::time::Instant::now();
    let early = spawn_pair(&c, &base, TOKEN, wait_request("early", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(!early.is_finished(), "the first peer must wait");
    // A second request for the same peer cannot queue it twice.
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", wait_request("early", 500)).await;
    assert_eq!(status, 409);
    register(&c, &base, TOKEN, "late").await;
    let (status, late) = post(&c, &base, TOKEN, "/v1/pair", wait_request("late", 0)).await;
    assert_eq!(status, 200, "{late}");
    let woken_after = std::time::Instant::now();
    let (status, early) = early.await.unwrap();
    assert_eq!(status, 200, "{early}");
    assert!(woken_after.elapsed() < std::time::Duration::from_millis(500));
    assert!(started.elapsed() >= std::time::Duration::from_millis(400));
    assert_eq!(early["room"], late["room"]);
    assert_eq!(early["link"], "peer-peer");
    assert_eq!(early["userId"], "early");
    assert_ne!(early["ticket"], late["ticket"]);
    for user in users(&c, &base, TOKEN).await {
        assert_eq!(user["status"], "paired");
        assert_eq!(user["serverId"], early["room"]);
    }
    task.abort();
}

/// Both peers are registered before either asks to pair. The waiter must not seat the registered
/// arrival on its own: it queues, the arrival claims it, and both get tickets for one room.
async fn a_registered_arrival_pairs_with_a_waiter(config: HubConfig) {
    let (base, task) = server_with(config).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "early").await;
    register(&c, &base, TOKEN, "late").await;
    let early = spawn_pair(&c, &base, TOKEN, wait_request("early", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !early.is_finished(),
        "the waiter must not pair with an idle registration"
    );
    for user in users(&c, &base, TOKEN).await {
        assert_eq!(user["status"], "waiting", "{user}");
    }
    let (status, late) = post(&c, &base, TOKEN, "/v1/pair", wait_request("late", 0)).await;
    assert_eq!(status, 200, "{late}");
    let (status, early) = early.await.unwrap();
    assert_eq!(status, 200, "{early}");
    assert_eq!(early["room"], late["room"]);
    assert_ne!(early["ticket"], late["ticket"]);
    task.abort();
}

#[tokio::test]
async fn a_registered_arrival_pairs_with_a_waiter_under_the_waiting_config() {
    a_registered_arrival_pairs_with_a_waiter(waiting_config(TOKEN)).await;
}

#[tokio::test]
async fn a_registered_arrival_pairs_with_a_waiter_under_the_default_config() {
    a_registered_arrival_pairs_with_a_waiter(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        public_url: Some("ws://live.example.test/v1/live".into()),
        ..HubConfig::default()
    })
    .await;
}

#[tokio::test]
async fn a_wait_that_times_out_leaves_no_state_behind() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "lonely").await;
    let started = std::time::Instant::now();
    let (status, body) = post(&c, &base, TOKEN, "/v1/pair", wait_request("lonely", 600)).await;
    assert_eq!(status, 408, "{body}");
    assert!(started.elapsed() >= std::time::Duration::from_millis(550));
    // The seat is free again, and the user was not paired by the failed wait.
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", wait_request("lonely", 200)).await;
    assert_eq!(status, 408);
    let seated = users(&c, &base, TOKEN).await;
    assert_eq!(seated[0]["status"], "waiting");
    let (status, body) = post(&c, &base, TOKEN, "/v1/pair", wait_request("lonely", 0)).await;
    assert_eq!(status, 404, "{body}");
    task.abort();
}

#[tokio::test]
async fn the_wait_is_capped_and_outlives_the_request_timeout() {
    // The service answers other requests within one second, and a wait is capped at three.
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "capped").await;
    let started = std::time::Instant::now();
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        wait_request("capped", 600_000),
    )
    .await;
    let waited = started.elapsed();
    assert_eq!(status, 408, "{body}");
    assert!(
        waited >= std::time::Duration::from_millis(2900),
        "{waited:?}"
    );
    assert!(
        waited < std::time::Duration::from_millis(3900),
        "{waited:?}"
    );
    let (status, _) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        wait_request("capped", u64::MAX),
    )
    .await;
    assert_eq!(status, 408);
    let mut bad = wait_request("capped", 1);
    bad["waitMs"] = json!("soon");
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", bad).await;
    assert_eq!(status, 400);
    let mut hub = wait_request("capped", 100);
    hub["target"] = json!({"kind":"hub"});
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", hub).await;
    assert_eq!(status, 400);
    task.abort();
}

#[tokio::test]
async fn a_dropped_request_leaves_the_queue() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "gone").await;
    let waiting = spawn_pair(&c, &base, TOKEN, wait_request("gone", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", wait_request("gone", 100)).await;
    assert_eq!(status, 409);
    waiting.abort();
    let _ = waiting.await;
    // The seat frees as soon as the server notices the closed connection.
    let mut freed = false;
    for _ in 0..40 {
        let (status, _) = post(&c, &base, TOKEN, "/v1/pair", wait_request("gone", 50)).await;
        if status == 408 {
            freed = true;
            break;
        }
        assert_eq!(status, 409);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(freed, "a dropped waiting request must free its seat");
    task.abort();
}

#[tokio::test]
async fn a_wait_through_a_remote_nest_uses_its_own_deadline() {
    let (remote, remote_task) = server_with(waiting_config(REMOTE)).await;
    let (root, root_task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &remote, REMOTE).await;
    register(&c, &remote, REMOTE, "far").await;
    let (status, body) = post(
        &c,
        &root,
        TOKEN,
        "/v1/hubs",
        json!({"id":"nest","mode":"central","address":remote,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let route = |id: &str, wait_ms: u64| json!({"path":["nest"],"op":"pair","body":wait_request(id, wait_ms)});
    let waiting = {
        let (c, root, body) = (c.clone(), root.clone(), route("far", 3000));
        tokio::spawn(async move { post(&c, &root, TOKEN, "/v1/route", body).await })
    };
    // Past the one-second request timeout of both services.
    tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
    assert!(!waiting.is_finished());
    register(&c, &remote, REMOTE, "near").await;
    let (status, near) = post(&c, &root, TOKEN, "/v1/route", route("near", 0)).await;
    assert_eq!(status, 200, "{near}");
    let (status, far) = waiting.await.unwrap();
    assert_eq!(status, 200, "{far}");
    assert_eq!(far["room"], near["room"]);
    // A nested wait that nobody answers ends in 408 at its own deadline.
    let (status, body) = post(
        &c,
        &remote,
        REMOTE,
        "/v1/hubs/arena/call",
        json!({"method":"upsertUser","args":[{"id":"alone","scopes":{"game":"other"}}]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let mut unanswered = wait_request("alone", 1500);
    unanswered["subject"]["scopes"] = json!({"game":"other"});
    unanswered["target"]["requirements"] = json!({"game":"other"});
    let started = std::time::Instant::now();
    let (status, _) = post(
        &c,
        &root,
        TOKEN,
        "/v1/route",
        json!({"path":["nest"],"op":"pair","body":unanswered}),
    )
    .await;
    assert_eq!(status, 408);
    assert!(started.elapsed() >= std::time::Duration::from_millis(1400));
    root_task.abort();
    remote_task.abort();
}

#[tokio::test]
async fn simultaneous_peers_pair_exactly_once() {
    for _ in 0..3 {
        let (base, task) = server_with(waiting_config(TOKEN)).await;
        let c = reqwest::Client::new();
        let ids: Vec<String> = (0..24).map(|n| format!("p{n:02}")).collect();
        arena(&c, &base, TOKEN).await;
        // Every peer registers and asks to pair at the same instant, at default limits.
        let calls: Vec<_> = ids
            .iter()
            .map(|id| {
                let (c, base, id) = (c.clone(), base.clone(), id.clone());
                tokio::spawn(async move {
                    register(&c, &base, TOKEN, &id).await;
                    {
                        let mut request = wait_request(&id, 3000);
                        request["target"]["capacity"] = json!(24);
                        post(&c, &base, TOKEN, "/v1/pair", request).await
                    }
                })
            })
            .collect();
        let mut rooms = std::collections::BTreeMap::new();
        for (id, call) in ids.iter().zip(calls) {
            let (status, body) = call.await.unwrap();
            assert_eq!(status, 200, "{id}: {body}");
            assert_eq!(body["userId"], json!(id));
            assert!(rooms
                .insert(id.clone(), body["room"].as_str().unwrap().to_owned())
                .is_none());
        }
        let seated = users(&c, &base, TOKEN).await;
        assert_eq!(seated.len(), ids.len());
        let mut members: std::collections::BTreeMap<String, usize> = Default::default();
        for user in &seated {
            // Each peer is in the one room its own answer named.
            assert_eq!(user["status"], "paired");
            assert_eq!(user["serverId"], json!(rooms[user["id"].as_str().unwrap()]));
            *members
                .entry(rooms[user["id"].as_str().unwrap()].clone())
                .or_default() += 1;
        }
        assert!(members.values().all(|n| *n >= 2), "{members:?}");
        task.abort();
    }
}

#[tokio::test]
async fn the_same_peer_id_waits_separately_in_two_hubs() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    for (hub, arena) in [("north-hub", "north"), ("south-hub", "south")] {
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            "/v1/hubs",
            json!({"id":hub,"mode":"internal","scopes":{"arena":arena}}),
        )
        .await;
        assert_eq!(status, 201, "{body}");
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            &format!("/v1/hubs/{hub}/call"),
            json!({"method":"upsertUser","args":[{"id":"twin","scopes":{"arena":arena}}]}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let request = |id: &str, arena: &str| {
        json!({
            "subject":{"kind":"peer","id":id,"scopes":{"arena":arena}},
            "target":{"kind":"peer","requirements":{"arena":arena}},
            "waitMs":3000
        })
    };
    // The same id waits in both hubs at once: neither request is a duplicate of the other.
    let north = spawn_pair(&c, &base, TOKEN, request("twin", "north"));
    let south = spawn_pair(&c, &base, TOKEN, request("twin", "south"));
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(!north.is_finished() && !south.is_finished());
    for (hub, arena, partner) in [("north-hub", "north", "n2"), ("south-hub", "south", "s2")] {
        let (status, body) = post(
            &c,
            &base,
            TOKEN,
            &format!("/v1/hubs/{hub}/call"),
            json!({"method":"upsertUser","args":[{"id":partner,"scopes":{"arena":arena}}]}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        let mut arrival = request(partner, arena);
        arrival["waitMs"] = json!(0);
        let (status, body) = post(&c, &base, TOKEN, "/v1/pair", arrival).await;
        assert_eq!(status, 200, "{body}");
    }
    let (status, north) = north.await.unwrap();
    assert_eq!(status, 200, "{north}");
    let (status, south) = south.await.unwrap();
    assert_eq!(status, 200, "{south}");
    assert_eq!(north["room"], "pair:twin");
    assert_eq!(south["room"], "pair:twin");
    // Each twin was seated in its own hub, beside its own partner.
    for (hub, partner) in [("north-hub", "n2"), ("south-hub", "s2")] {
        let (_, user) = post(
            &c,
            &base,
            TOKEN,
            &format!("/v1/hubs/{hub}/call"),
            json!({"method":"getUser","args":[partner]}),
        )
        .await;
        assert_eq!(user["result"]["serverId"], "pair:twin", "{hub}");
        assert_eq!(user["result"]["status"], "paired");
    }
    task.abort();
}

#[tokio::test]
async fn a_full_queue_refuses_with_503() {
    let (base, task) = server_with(HubConfig {
        max_pending_pairs: 1,
        ..waiting_config(TOKEN)
    })
    .await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "first").await;
    let first = spawn_pair(&c, &base, TOKEN, wait_request("first", 2000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    register(&c, &base, TOKEN, "second").await;
    let (status, body) = post(&c, &base, TOKEN, "/v1/pair", wait_request("second", 500)).await;
    assert_eq!(status, 503, "{body}");
    // The refused request left nothing, and the waiter still holds its place.
    assert!(!first.is_finished());
    let (status, _) = post(&c, &base, TOKEN, "/v1/pair", wait_request("second", 0)).await;
    assert_eq!(status, 200);
    let (status, body) = first.await.unwrap();
    assert_eq!(status, 200, "{body}");
    task.abort();
}

#[tokio::test]
async fn a_waiter_on_a_hub_with_auth_is_authorized_when_woken() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    let (ann, ben) = (
        "player-ann-token-0123456789abcdefgh",
        "player-ben-token-0123456789abcdefgh",
    );
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs",
        json!({"id":"arena","mode":"internal","scopes":{"game":"g"},"auth":{"kind":"token","tokens":[
            {"token":ann,"identity":"ann"},{"token":ben,"identity":"ben"}]}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    register(&c, &base, TOKEN, "ann").await;
    let with = |id: &str, credential: &str, wait_ms: u64| {
        let mut body = wait_request(id, wait_ms);
        body["subject"]["credential"] = json!(credential);
        body
    };
    let waiting = spawn_pair(&c, &base, TOKEN, with("ann", ann, 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    register(&c, &base, TOKEN, "ben").await;
    let (status, ben_answer) = post(&c, &base, TOKEN, "/v1/pair", with("ben", ben, 0)).await;
    assert_eq!(status, 200, "{ben_answer}");
    let (status, ann_answer) = waiting.await.unwrap();
    assert_eq!(status, 200, "{ann_answer}");
    assert_eq!(ann_answer["room"], ben_answer["room"]);
    assert_eq!(ann_answer["userId"], "ann");
    assert!(!ann_answer.to_string().contains(ann));

    // A waiter whose credential is refused when it wakes gets 403 and does not stay seated.
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs",
        json!({"id":"gated","mode":"internal","scopes":{"game":"g"},"auth":{"kind":"token","tokens":[
            {"token":"player-dan-token-0123456789abcdefgh","identity":"dan"}]}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let gated_user = |id: &'static str| {
        let (c, base) = (c.clone(), base.clone());
        async move {
            let (status, body) = post(
                &c,
                &base,
                TOKEN,
                "/v1/hubs/gated/call",
                json!({"method":"upsertUser","args":[{"id":id,"scopes":{"game":"h"}}]}),
            )
            .await;
            assert_eq!(status, 200, "{body}");
        }
    };
    gated_user("cat").await;
    let gated = |id: &str, credential: &str, wait_ms: u64| {
        json!({
            "subject":{"kind":"peer","id":id,"scopes":{"game":"h"},"credential":credential},
            "target":{"kind":"peer","requirements":{"game":"h"}},
            "waitMs":wait_ms
        })
    };
    let waiting = spawn_pair(&c, &base, TOKEN, gated("cat", "not-a-credential", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    gated_user("dan").await;
    let (status, body) = post(
        &c,
        &base,
        TOKEN,
        "/v1/pair",
        gated("dan", "player-dan-token-0123456789abcdefgh", 0),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = waiting.await.unwrap();
    assert_eq!(status, 403);
    let (_, cat) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/gated/call",
        json!({"method":"getUser","args":["cat"]}),
    )
    .await;
    assert_ne!(cat["result"]["status"], "paired", "{cat}");
    task.abort();
}

async fn capacity_crowd(capacity: u32, count: usize) -> (Vec<Value>, Vec<Value>) {
    let (base, task) = server(TOKEN).await;
    let client = reqwest::Client::new();
    assert_eq!(
        post(
            &client,
            &base,
            TOKEN,
            "/v1/hubs",
            json!({"id":"arena","mode":"internal"})
        )
        .await
        .0,
        201
    );
    for i in 0..count {
        let (status, body) = post(
            &client,
            &base,
            TOKEN,
            "/v1/hubs/arena/call",
            json!({"method":"upsertUser", "args":[{"id":format!("p{i:02}"),"scopes":{"arena":"capacity"}}]}),
        )
        .await;
        assert_eq!(status, 200, "{body}");
    }
    let mut jobs = Vec::new();
    for i in 0..count {
        let c = client.clone();
        let b = base.clone();
        jobs.push(tokio::spawn(async move {
            post(&c, &b, TOKEN, "/v1/pair", json!({"subject":{"kind":"peer","id":format!("p{i:02}"),"scopes":{"arena":"capacity"}},"target":{"kind":"peer","requirements":{"arena":"capacity"},"capacity":capacity},"waitMs":3000})).await
        }));
    }
    if capacity == 2 && count == 5 {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while jobs.iter().filter(|job| job.is_finished()).count() < 4 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            jobs.iter().filter(|job| job.is_finished()).count(),
            4,
            "one peer must still be waiting"
        );
    }
    let mut matched = Vec::new();
    for job in jobs {
        let (status, body) = job.await.unwrap();
        assert!(status == 200 || status == 408, "{status}: {body}");
        if status == 200 {
            matched.push(body);
        }
    }
    let (status, body) = post(
        &client,
        &base,
        TOKEN,
        "/v1/hubs/arena/call",
        json!({"method":"listServers", "args":[]}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let rooms = body["result"].as_array().unwrap().clone();
    task.abort();
    (matched, rooms)
}

#[tokio::test]
async fn capacity_two_five_simultaneous_peers() {
    let (matched, rooms) = capacity_crowd(2, 5).await;
    assert_eq!(matched.len(), 4);
    assert_eq!(rooms.len(), 2);
    assert!(rooms
        .iter()
        .all(|room| room["members"].as_array().unwrap().len() == 2));
}

#[tokio::test]
async fn capacity_three_twelve_simultaneous_peers() {
    let (_, rooms) = capacity_crowd(3, 12).await;
    assert!(
        rooms
            .iter()
            .map(|r| r["members"].as_array().unwrap().len())
            .sum::<usize>()
            >= 10
    );
    assert!(rooms
        .iter()
        .all(|room| room["members"].as_array().unwrap().len() <= 3));
}

#[tokio::test]
async fn capacity_invalid_returns_bad_request() {
    let (base, task) = server(TOKEN).await;
    let client = reqwest::Client::new();
    for capacity in [
        json!(0),
        json!(1),
        json!(65),
        json!(-1),
        json!(2.5),
        json!(null),
        json!("2"),
    ] {
        let body = json!({"subject":{"kind":"peer","id":"a","scopes":{}},"target":{"kind":"peer","capacity":capacity}});
        assert_eq!(
            post(&client, &base, TOKEN, "/v1/pair", body.clone())
                .await
                .0,
            400
        );
        assert_eq!(
            post(
                &client,
                &base,
                TOKEN,
                "/v1/route",
                json!({"path":[],"op":"pair","body":body})
            )
            .await
            .0,
            400
        );
    }
    task.abort();
}

#[tokio::test]
async fn never_seated_idle_peer_can_later_request_a_pair() {
    let (base, task) = server_with(waiting_config(TOKEN)).await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "idle").await;
    register(&c, &base, TOKEN, "arrival").await;
    for absent in [false, true] {
        let mut request = wait_request("arrival", 0);
        if absent {
            request.as_object_mut().unwrap().remove("waitMs");
        }
        let (status, body) = post(&c, &base, TOKEN, "/v1/pair", request).await;
        assert_eq!(status, 404, "{body}");
    }
    for user in users(&c, &base, TOKEN).await {
        assert_ne!(user["status"], "paired", "{user}");
        assert!(user["serverId"].is_null(), "{user}");
    }
    let waiting = spawn_pair(&c, &base, TOKEN, wait_request("arrival", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    let (status, idle) = post(&c, &base, TOKEN, "/v1/pair", wait_request("idle", 0)).await;
    assert_eq!(status, 200, "{idle}");
    let (status, arrival) = waiting.await.unwrap();
    assert_eq!(status, 200, "{arrival}");
    assert_eq!(idle["room"], arrival["room"]);
    assert_ne!(idle["ticket"], arrival["ticket"]);
    task.abort();
}

#[tokio::test]
async fn ticket_failure_after_pair_peers_leaves_nobody_seated() {
    let (base, task) = server_with(HubConfig {
        public_url: None,
        ..waiting_config(TOKEN)
    })
    .await;
    let c = reqwest::Client::new();
    arena(&c, &base, TOKEN).await;
    register(&c, &base, TOKEN, "early").await;
    register(&c, &base, TOKEN, "late").await;
    let waiting = spawn_pair(&c, &base, TOKEN, wait_request("early", 3000));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!waiting.is_finished());
    let (status, body) = post(&c, &base, TOKEN, "/v1/pair", wait_request("late", 0)).await;
    assert_eq!(status, 503, "{body}");
    for user in users(&c, &base, TOKEN).await {
        assert_ne!(user["status"], "paired", "{user}");
        assert!(user["serverId"].is_null(), "{user}");
    }
    let (status, rooms) = post(
        &c,
        &base,
        TOKEN,
        "/v1/hubs/arena/call",
        json!({"method":"listServers","args":[]}),
    )
    .await;
    assert_eq!(status, 200, "{rooms}");
    assert_eq!(rooms["result"], json!([]));
    waiting.abort();
    task.abort();
}
