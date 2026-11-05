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
        let service = HubService::new(HubConfig {
            token: TOKEN.into(),
            public_url: Some("ws://localhost/v1/live".into()),
            ticket_ttl: Duration::from_secs(ttl),
            live_allowed_origins: vec!["https://allowed.test".into()],
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
        assert_eq!(
            post(
                &fixture.client,
                &fixture.base,
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
        assert!(matches!(
            result,
            None | Some(Ok(Message::Close(_))) | Some(Err(_))
        ));
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
        assert!(matches!(
            result,
            None | Some(Ok(Message::Close(_))) | Some(Err(_))
        ));
    }
}
