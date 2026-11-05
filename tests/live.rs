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
