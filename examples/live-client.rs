//! Join a live room from Rust: pair two peers, connect both over the hub relay and trade frames.
//!
//! The example starts its own hub on a free local port, so it needs nothing else running:
//!   cargo run --example live-client
//!
//! Against your own hub, keep the administrator token on the server, call `HubClient::pair` (or
//! `HubClient::ticket`) there, and hand the descriptor to `LiveClient::connect`.

use liveengine::{
    hub::{HubClient, HubConfig, HubService},
    live_client::{LiveClient, LiveClientOptions, LiveDescriptor, LiveEvent},
};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "example-live-client-token-0123456789";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        public_url: Some(format!("ws://{addr}/v1/live")),
        ..HubConfig::default()
    })?;
    let router = service.router();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
    });

    // Server side: register a hub and pair two peers that want the same mode.
    let hub = HubClient::new(&format!("http://{addr}"), TOKEN)?;
    hub.register_hub(json!({"id":"lobby","mode":"internal"}))
        .await?;
    // Peers are registered before they ask to pair, so each can be found as a candidate.
    let users = json!([
        {"id":"ada","scopes":{"mode":"duel"}},
        {"id":"bea","scopes":{"mode":"duel"}}
    ]);
    hub.call_hub("lobby", "upsertUsers", vec![users]).await?;
    let pair = |id: &'static str| {
        let hub = hub.clone();
        async move {
            hub.pair(json!({
                "subject": {"kind":"peer","id":id,"scopes":{"mode":"duel"}},
                "target": {"kind":"peer","requirements":{"mode":"duel"}},
                "waitMs": 5000
            }))
            .await
        }
    };
    let (a, b) = tokio::join!(pair("ada"), pair("bea"));
    let (a, b): (Value, Value) = (a?, b?);
    println!("paired in room {}", a["room"]);

    // Client side: each peer joins with its own descriptor.
    let (ada, mut ada_events) = LiveClient::connect(
        LiveDescriptor::from_value(&a)?,
        LiveClientOptions::default(),
    )
    .await?;
    let (bea, mut bea_events) = LiveClient::connect(
        LiveDescriptor::from_value(&b)?,
        LiveClientOptions::default(),
    )
    .await?;

    ada.send("chat", json!({"text":"hello from ada"})).await?;
    bea.send_to("ada", "chat", json!({"text":"hi ada"})).await?;

    let mut got = 0;
    while got < 2 {
        tokio::select! {
            Some(event) = ada_events.recv() => if report("ada", event) { got += 1 },
            Some(event) = bea_events.recv() => if report("bea", event) { got += 1 },
        }
    }
    if let Some(rtt) = ada.rtt() {
        println!("ada: round trip to the hub {rtt:?}");
    }
    ada.close().await;
    bea.close().await;
    Ok(())
}

/// Print an event; true for a message.
fn report(who: &str, event: LiveEvent) -> bool {
    match event {
        LiveEvent::Welcome { peers, .. } => println!("{who}: welcome, peers {peers:?}"),
        LiveEvent::Join { peer, .. } => println!("{who}: {peer} joined"),
        LiveEvent::Leave { peer } => println!("{who}: {peer} left"),
        LiveEvent::Message(frame) => {
            println!("{who}: {} says {}", frame.from, frame.payload);
            return true;
        }
        LiveEvent::Rtt(_) => {}
        other => println!("{who}: {other:?}"),
    }
    false
}
