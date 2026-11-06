#![cfg(feature = "host")]
use liveengine::{
    hub::{HubConfig, HubService},
    live::TicketSigner,
};
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};

const TOKEN: &str = "live-ticket-integration-token-123456789";
async fn post(client: &reqwest::Client, base: &str, path: &str, body: Value) -> (u16, Value) {
    let response = client
        .post(format!("{base}{path}"))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap();
    (
        response.status().as_u16(),
        response.json().await.unwrap_or(Value::Null),
    )
}
#[tokio::test]
async fn tickets_over_tcp_are_bound_single_use_and_secret() {
    let path = std::env::temp_dir().join(format!(
        "live-tickets-{}-{}.json",
        std::process::id(),
        rand::random::<u64>()
    ));
    let config = HubConfig {
        token: TOKEN.into(),
        state_file: Some(path.clone()),
        public_url: Some("https://live.example.test/v1/live".into()),
        ..HubConfig::default()
    };
    let service = HubService::new(config.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = service.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            server
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let client = reqwest::Client::new();
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"game","mode":"internal"})
        )
        .await
        .0,
        201
    );
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertUser", json!([{"id":"bob"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        assert_eq!(
            post(
                &client,
                &base,
                "/v1/hubs/game/call",
                json!({"method":method,"args":args})
            )
            .await
            .0,
            200
        );
    }
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs/game/tickets",
            json!({"userId":"bob","serverId":"room"})
        )
        .await
        .0,
        403
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/hubs/game/tickets"))
            .json(&json!({"userId":"alice","serverId":"room"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let (status, issued) = post(
        &client,
        &base,
        "/v1/hubs/game/tickets",
        json!({"userId":"alice","serverId":"room"}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(issued["url"], "https://live.example.test/v1/live");
    let ticket = issued["ticket"].as_str().unwrap();
    let binding = vec!["game".to_string()];
    assert!(service.verify_ticket(ticket, &binding, "wrong").is_err());
    let mut forged = ticket.to_string();
    forged.push('A');
    assert!(service.verify_ticket(&forged, &binding, "room").is_err());
    let claims = service.verify_ticket(ticket, &binding, "room").unwrap();
    assert_eq!(claims.user, "alice");
    assert_eq!(claims.role, "peer");
    assert!(service.verify_ticket(ticket, &binding, "room").is_err());
    let (_, nested) = post(&client, &base, "/v1/route", json!({"path":[],"op":"ticket","body":{"id":"game","userId":"alice","serverId":"room","role":"service"}})).await;
    let nested_ticket = nested["ticket"].as_str().unwrap();
    let snapshot: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let key = snapshot["signing_key"].to_string();
    assert_ne!(key, "null");
    for route in [
        "/v1/hubs",
        "/v1/hubs/game",
        "/v1/tree",
        "/v1/registry/export",
        "/v1/contract",
    ] {
        let text = client
            .get(format!("{base}{route}"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!text.contains("signing_key") && !text.contains(&key));
    }
    assert!(!issued.to_string().contains(&key) && !nested.to_string().contains(&key));
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"central","mode":"central"})
        )
        .await
        .0,
        201
    );
    let route = |op: &str, body: Value| json!({"path":["central"],"op":op,"body":body});
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/route",
            route("register", json!({"id":"nested","mode":"internal"}))
        )
        .await
        .0,
        201
    );
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room"}])),
        ("join", json!(["alice", "room"])),
    ] {
        assert_eq!(
            post(
                &client,
                &base,
                "/v1/route",
                route("call", json!({"id":"nested","method":method,"args":args}))
            )
            .await
            .0,
            200
        );
    }
    let (status, nested_local) = post(
        &client,
        &base,
        "/v1/route",
        route(
            "ticket",
            json!({"id":"nested","userId":"alice","serverId":"room"}),
        ),
    )
    .await;
    assert_eq!(status, 200);
    assert!(service
        .verify_ticket(
            nested_local["ticket"].as_str().unwrap(),
            &["central".into(), "nested".into()],
            "room"
        )
        .is_ok());
    task.abort();
    let restored = HubService::new(config).unwrap();
    assert_eq!(
        restored
            .verify_ticket(nested_ticket, &binding, "room")
            .unwrap()
            .role,
        "service"
    );
    std::fs::remove_file(path).unwrap();
}
#[tokio::test]
async fn expired_tickets_are_rejected() {
    let signer = TicketSigner::new(TicketSigner::random_key());
    let path = vec!["game".to_string()];
    let (ticket, _) = signer
        .issue(
            path.clone(),
            "room".into(),
            "alice".into(),
            "peer".into(),
            1,
        )
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(signer.verify(&ticket, &path, "room").is_err());
}

mod sockets {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };
    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
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
    async fn setup(ttl: u64) -> Fixture {
        setup_channels(ttl, None).await
    }
    async fn setup_channels(ttl: u64, channels: Option<Value>) -> Fixture {
        setup_config(ttl, channels, vec![], None).await
    }
    async fn setup_config(
        ttl: u64,
        channels: Option<Value>,
        ice_servers: Vec<Value>,
        turn_secret: Option<String>,
    ) -> Fixture {
        let service = HubService::new(HubConfig {
            token: TOKEN.into(),
            public_url: Some("ws://localhost/v1/live".into()),
            ticket_ttl: Duration::from_secs(ttl),
            live_allowed_origins: vec!["https://allowed.test".into()],
            ice_servers,
            turn_secret,
            ..HubConfig::default()
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
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
        let fixture = Fixture {
            base: format!("http://{addr}"),
            ws: format!("ws://{addr}/v1/live"),
            client: reqwest::Client::new(),
            task,
        };
        if channels.is_some() {
            assert_eq!(
                post(
                    &fixture.client,
                    &fixture.base,
                    "/v1/schemas",
                    json!({"id":"message","version":"1","schema":{"type":"integer"}})
                )
                .await
                .0,
                201
            );
        }
        let mut registration = json!({"id":"game","mode":"internal"});
        if let Some(channels) = channels {
            registration["channels"] = channels;
        }
        assert_eq!(
            post(&fixture.client, &fixture.base, "/v1/hubs", registration)
                .await
                .0,
            201
        );
        for (method, args) in [
            ("upsertUser", json!([{"id":"alice"}])),
            ("upsertUser", json!([{"id":"bob"}])),
            ("upsertUser", json!([{"id":"carol"}])),
            ("upsertServer", json!([{"id":"room","capacity":3}])),
            ("join", json!(["alice", "room"])),
            ("join", json!(["bob", "room"])),
            ("join", json!(["carol", "room"])),
        ] {
            assert_eq!(
                post(
                    &fixture.client,
                    &fixture.base,
                    "/v1/hubs/game/call",
                    json!({"method":method,"args":args})
                )
                .await
                .0,
                200
            );
        }
        fixture
    }
    async fn ticket(f: &Fixture, user: &str) -> String {
        let (status, value) = post(
            &f.client,
            &f.base,
            "/v1/hubs/game/tickets",
            json!({"userId":user,"serverId":"room"}),
        )
        .await;
        assert_eq!(status, 200);
        value["ticket"].as_str().unwrap().to_owned()
    }
    async fn auth(f: &Fixture, ticket: &str) -> Socket {
        let (mut socket, _) = connect_async(&f.ws).await.unwrap();
        socket
            .send(Message::Text(
                json!({"type":"auth","ticket":ticket}).to_string().into(),
            ))
            .await
            .unwrap();
        socket
    }
    async fn read(socket: &mut Socket) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = message.into_text().unwrap();
        assert!(!text.contains(TOKEN));
        serde_json::from_str(&text).unwrap()
    }
    async fn closed(socket: &mut Socket) {
        let result = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap();
        if let Some(Ok(Message::Text(text))) = result {
            let error: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(error["type"], "error");
            assert!(["auth", "channel", "rate", "schema", "frame", "duplicate"]
                .contains(&error["code"].as_str().unwrap()));
            let next = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap();
            assert!(matches!(
                next,
                None | Some(Ok(Message::Close(_))) | Some(Err(_))
            ));
        } else {
            assert!(matches!(
                result,
                None | Some(Ok(Message::Close(_))) | Some(Err(_))
            ));
        }
    }
    #[tokio::test]
    async fn relay_broadcast_target_and_presence() {
        let f = setup(60).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        assert_eq!(read(&mut a).await["peers"], json!([]));
        let mut b = auth(&f, &ticket(&f, "bob").await).await;
        assert_eq!(read(&mut b).await["peers"], json!(["alice"]));
        assert_eq!(read(&mut a).await["event"], "join");
        let mut c = auth(&f, &ticket(&f, "carol").await).await;
        assert_eq!(read(&mut c).await["peers"].as_array().unwrap().len(), 2);
        assert_eq!(read(&mut a).await["peer"], "carol");
        assert_eq!(read(&mut b).await["peer"], "carol");
        a.send(Message::Text(
            json!({"v":1,"type":"data","channel":"x","seq":1,"payload":{"n":2}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let frame = read(&mut b).await;
        assert_eq!(frame["from"], "alice");
        assert_eq!(frame["payload"]["n"], 2);
        assert_eq!(read(&mut c).await["from"], "alice");
        b.send(Message::Text(
            json!({"v":1,"type":"data","channel":"x","to":"alice","seq":2,"payload":"hello"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        assert_eq!(read(&mut a).await["from"], "bob");
        assert!(tokio::time::timeout(Duration::from_millis(100), c.next())
            .await
            .is_err());
        b.close(None).await.unwrap();
        assert_eq!(read(&mut a).await["event"], "leave");
    }
    #[tokio::test]
    async fn signalling_is_targeted_and_rejects_cross_room_and_oversize() {
        let f = setup_channels(60, Some(json!({"data":{"delivery":"reliable"}}))).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        read(&mut a).await;
        let mut b = auth(&f, &ticket(&f, "bob").await).await;
        read(&mut b).await;
        read(&mut a).await;
        let mut c = auth(&f, &ticket(&f, "carol").await).await;
        read(&mut c).await;
        read(&mut a).await;
        read(&mut b).await;
        for kind in ["signal.offer", "signal.answer", "signal.ice"] {
            let frame = json!({"v":1,"type":kind,"channel":"signalling","to":"bob","seq":1,"payload":{"opaque":"sdp"}});
            a.send(Message::Text(frame.to_string().into()))
                .await
                .unwrap();
            let received = read(&mut b).await;
            assert_eq!(received["type"], kind);
            assert_eq!(received["payload"], frame["payload"]);
            assert_eq!(received["from"], "alice");
            assert!(tokio::time::timeout(Duration::from_millis(100), c.next())
                .await
                .is_err());
        }
        for (method, args) in [
            ("upsertUser", json!([{"id":"dave"}])),
            ("upsertServer", json!([{"id":"other"}])),
            ("join", json!(["dave", "other"])),
        ] {
            assert_eq!(
                post(
                    &f.client,
                    &f.base,
                    "/v1/hubs/game/call",
                    json!({"method":method,"args":args})
                )
                .await
                .0,
                200
            );
        }
        let (_, issued) = post(
            &f.client,
            &f.base,
            "/v1/hubs/game/tickets",
            json!({"userId":"dave","serverId":"other"}),
        )
        .await;
        let mut d = auth(&f, issued["ticket"].as_str().unwrap()).await;
        read(&mut d).await;
        a.send(Message::Text(json!({"v":1,"type":"signal.offer","channel":"signal","to":"dave","seq":1,"payload":null}).to_string().into())).await.unwrap();
        closed(&mut a).await;
        assert!(tokio::time::timeout(Duration::from_millis(100), d.next())
            .await
            .is_err());
        b.send(Message::Text(json!({"v":1,"type":"signal.ice","channel":"signal","to":"carol","seq":1,"payload":"x".repeat(17000)}).to_string().into())).await.unwrap();
        closed(&mut b).await;
    }

    #[tokio::test]
    async fn turn_credentials_have_coturn_format_and_expiry() {
        use base64::engine::general_purpose::STANDARD;
        use ring::hmac;
        use std::time::{SystemTime, UNIX_EPOCH};
        let secret = "test-turn-secret-at-least-24-characters";
        let servers = vec![
            json!({"urls":"stun:stun.example.test"}),
            json!({"urls":["turn:turn.example.test", "turns:turn.example.test"]}),
        ];
        let f = setup_config(60, None, servers, Some(secret.into())).await;
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        let welcome = read(&mut a).await;
        assert!(!welcome.to_string().contains(secret));
        assert_eq!(welcome["iceServers"][0]["urls"], "stun:stun.example.test");
        let turn = &welcome["iceServers"][1];
        let username = turn["username"].as_str().unwrap();
        let expiry: u64 = username.split(':').next().unwrap().parse().unwrap();
        assert!(expiry >= before + 3600 && expiry <= before + 3602);
        assert!(username.contains(":alice:"));
        let signature = STANDARD
            .decode(turn["credential"].as_str().unwrap())
            .unwrap();
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret.as_bytes()),
            username.as_bytes(),
            &signature,
        )
        .unwrap();
        let mut b = auth(&f, &ticket(&f, "bob").await).await;
        assert_ne!(read(&mut b).await["iceServers"][1]["username"], username);
    }
    #[tokio::test]
    async fn server_event_types_cannot_be_relayed_by_peers() {
        for kind in [
            "presence",
            "welcome",
            "error",
            "auth",
            "link.other",
            "signal.other",
        ] {
            let f = setup(60).await;
            let mut a = auth(&f, &ticket(&f, "alice").await).await;
            read(&mut a).await;
            let mut b = auth(&f, &ticket(&f, "bob").await).await;
            read(&mut b).await;
            read(&mut a).await;
            a.send(Message::Text(json!({"v":1,"type":kind,"channel":"x","seq":1,"payload":null,"peer":"ghost","event":"join"}).to_string().into())).await.unwrap();
            closed(&mut a).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(200), async {
                    loop {
                        let value = read(&mut b).await;
                        if value["type"] == kind || value["peer"] == "ghost" {
                            return;
                        }
                    }
                })
                .await
                .is_err(),
                "forged {kind} reached another peer"
            );
        }
    }
    #[tokio::test]
    async fn spoof_from_is_rejected() {
        let f = setup(60).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        read(&mut a).await;
        a.send(Message::Text(
            json!({"v":1,"type":"data","channel":"x","seq":1,"payload":null,"from":"bob"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        closed(&mut a).await;
    }
    #[tokio::test]
    async fn forged_claims_and_swapped_signature_are_refused() {
        let f = setup(60).await;
        let a = ticket(&f, "alice").await;
        let b = ticket(&f, "bob").await;
        let (payload, sig) = a.split_once('.').unwrap();
        let mut claims: Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        claims["user"] = json!("bob");
        let changed = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap()),
            sig
        );
        let swapped = format!("{}.{}", payload, b.split_once('.').unwrap().1);
        for bad in ["invalid.ticket".to_owned(), changed, swapped] {
            let mut socket = auth(&f, &bad).await;
            closed(&mut socket).await;
        }
    }
    #[tokio::test]
    async fn reused_and_expired_tickets_are_refused() {
        let f = setup(1).await;
        let t = ticket(&f, "alice").await;
        let mut a = auth(&f, &t).await;
        read(&mut a).await;
        let mut reused = auth(&f, &t).await;
        closed(&mut reused).await;
        let expired = ticket(&f, "bob").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut b = auth(&f, &expired).await;
        closed(&mut b).await;
    }
    #[tokio::test]
    async fn auth_timeout_and_bad_origin() {
        let f = setup(60).await;
        let mut request = f.ws.as_str().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://bad.test".parse().unwrap());
        assert!(connect_async(request).await.is_err());
        let (mut socket, _) = connect_async(&f.ws).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(7), socket.next())
            .await
            .unwrap();
        let text = result.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({"type":"error","code":"auth"})
        );
    }
    #[tokio::test]
    async fn channel_schema_undeclared_and_rate_enforced() {
        let f = setup_channels(
            60,
            Some(json!({"valid":{"delivery":"reliable","schemaRef":"message@1","maxRate":1}})),
        )
        .await;
        for (channel, payload, twice, code) in [
            ("valid", json!("bad"), false, "schema"),
            ("missing", json!(1), false, "channel"),
            ("valid", json!(1), true, "rate"),
        ] {
            let mut a = auth(&f, &ticket(&f, "alice").await).await;
            read(&mut a).await;
            let frame = json!({"v":1,"type":"data","channel":channel,"seq":1,"payload":payload})
                .to_string();
            a.send(Message::Text(frame.clone().into())).await.unwrap();
            if twice {
                a.send(Message::Text(frame.into())).await.unwrap();
            }
            assert_eq!(read(&mut a).await, json!({"type":"error","code":code}));
            closed(&mut a).await;
        }
    }

    #[tokio::test]
    async fn nested_hub_inherits_channel_schema() {
        let f = setup_channels(60, Some(json!({"x":{"delivery":"reliable"}}))).await;
        assert_eq!(post(&f.client, &f.base, "/v1/hubs", json!({"id":"central","mode":"central","channels":{"inherited":{"delivery":"reliable","schemaRef":"message@1"}}})).await.0, 201);
        let route = |op: &str, body: Value| json!({"path":["central"],"op":op,"body":body});
        assert_eq!(
            post(
                &f.client,
                &f.base,
                "/v1/route",
                route("register", json!({"id":"child","mode":"internal"}))
            )
            .await
            .0,
            201
        );
        for (method, args) in [
            ("upsertUser", json!([{"id":"alice"}])),
            ("upsertServer", json!([{"id":"room"}])),
            ("join", json!(["alice", "room"])),
        ] {
            assert_eq!(
                post(
                    &f.client,
                    &f.base,
                    "/v1/route",
                    route("call", json!({"id":"child","method":method,"args":args}))
                )
                .await
                .0,
                200
            );
        }
        let (status, issued) = post(
            &f.client,
            &f.base,
            "/v1/route",
            route(
                "ticket",
                json!({"id":"child","userId":"alice","serverId":"room"}),
            ),
        )
        .await;
        assert_eq!(status, 200);
        let mut socket = auth(&f, issued["ticket"].as_str().unwrap()).await;
        read(&mut socket).await;
        socket
            .send(Message::Text(
                json!({"v":1,"type":"data","channel":"inherited","seq":1,"payload":"invalid"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        closed(&mut socket).await;
    }

    #[tokio::test]
    async fn unpair_disconnects_and_resume_keeps_presence() {
        let f = setup(60).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        read(&mut a).await;
        let mut b = auth(&f, &ticket(&f, "bob").await).await;
        read(&mut b).await;
        read(&mut a).await;
        a.close(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        assert_eq!(read(&mut a).await["peers"], json!(["bob"]));
        assert!(tokio::time::timeout(Duration::from_millis(650), b.next())
            .await
            .is_err());
        assert_eq!(
            post(
                &f.client,
                &f.base,
                "/v1/hubs/game/call",
                json!({"method":"leave","args":["alice","room"]})
            )
            .await
            .0,
            200
        );
        closed(&mut a).await;
        assert_eq!(read(&mut b).await["event"], "leave");
    }
    #[tokio::test]
    async fn oversized_frame_is_rejected() {
        let f = setup(60).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        read(&mut a).await;
        let frame = json!({"v":1,"type":"data","channel":"x","seq":1,"payload":"x".repeat(70_000)})
            .to_string();
        a.send(Message::Text(frame.into())).await.unwrap();
        assert_eq!(read(&mut a).await, json!({"type":"error","code":"frame"}));
        closed(&mut a).await;
    }
    #[tokio::test]
    async fn tickets_do_not_collide_with_membership_checks() {
        let f = setup(60).await;
        for _ in 0..20 {
            let (a, b) = tokio::join!(ticket(&f, "alice"), ticket(&f, "bob"));
            let _ = tokio::join!(
                auth(&f, &a),
                auth(&f, &b),
                ticket(&f, "carol"),
                ticket(&f, "alice"),
                ticket(&f, "bob"),
            );
        }
    }
    #[tokio::test]
    async fn slow_consumer_reliable_and_lossy() {
        let Some(delivery) = std::env::var("LIVEENGINE_TEST_SLOW_CASE").ok() else {
            for delivery in ["reliable", "lossy"] {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "sockets::slow_consumer_reliable_and_lossy"])
                    .env("LIVEENGINE_TEST_SLOW_CASE", delivery)
                    .env("LIVEENGINE_LIVE_SEND_QUEUE", "1")
                    .env("LIVEENGINE_LIVE_MESSAGES_PER_SECOND", "100000")
                    .env("LIVEENGINE_LIVE_BYTES_PER_SECOND", "16777216")
                    .status()
                    .unwrap();
                assert!(status.success(), "{delivery} slow consumer test failed");
            }
            return;
        };
        let f = setup_channels(60, Some(json!({"x":{"delivery":delivery}}))).await;
        let mut a = auth(&f, &ticket(&f, "alice").await).await;
        read(&mut a).await;
        let mut b = auth(&f, &ticket(&f, "bob").await).await;
        read(&mut b).await;
        read(&mut a).await;
        let frame = json!({"v":1,"type":"data","channel":"x","seq":1,"payload":"x".repeat(16_000)})
            .to_string();
        for _ in 0..600 {
            a.send(Message::Text(frame.clone().into())).await.unwrap();
        }
        if delivery == "reliable" {
            tokio::time::timeout(Duration::from_secs(5), async {
                while matches!(b.next().await, Some(Ok(Message::Text(_)))) {}
            })
            .await
            .expect("slow reliable consumer remained connected");
        } else {
            let mut received = 0;
            while let Ok(Some(Ok(Message::Text(_)))) =
                tokio::time::timeout(Duration::from_millis(300), b.next()).await
            {
                received += 1;
            }
            assert!(received < 600, "lossy queue did not drop any frames");
            b.send(Message::Text(
                json!({"v":1,"type":"data","channel":"x","seq":2,"payload":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            assert_eq!(read(&mut a).await["from"], "bob");
        }
    }
    #[tokio::test]
    async fn connection_caps() {
        match std::env::var("LIVEENGINE_TEST_CAP_CASE").ok().as_deref() {
            None => {
                for case in ["global", "room"] {
                    let status = std::process::Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "sockets::connection_caps"])
                        .env("LIVEENGINE_TEST_CAP_CASE", case)
                        .env(
                            "LIVEENGINE_LIVE_MAX_CONNECTIONS",
                            if case == "global" { "1" } else { "2" },
                        )
                        .env("LIVEENGINE_LIVE_MAX_CONNECTIONS_PER_ROOM", "1")
                        .status()
                        .unwrap();
                    assert!(status.success(), "{case} cap test failed");
                }
            }
            Some(case) => {
                let f = setup(60).await;
                let mut a = auth(&f, &ticket(&f, "alice").await).await;
                read(&mut a).await;
                if case == "global" {
                    assert!(connect_async(&f.ws).await.is_err());
                } else {
                    let mut b = auth(&f, &ticket(&f, "bob").await).await;
                    closed(&mut b).await;
                }
            }
        }
    }
}
