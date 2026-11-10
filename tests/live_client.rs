#![cfg(feature = "host")]
//! The Rust live-room client against a real in-process hub.
use futures_util::{SinkExt, StreamExt};
use liveengine::{
    hub::{HubClient, HubConfig, HubService},
    live_client::{
        LiveClient, LiveClientOptions, LiveDescriptor, LiveError, LiveEvent, LiveFrame, LiveSend,
    },
};
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const TOKEN: &str = "live-client-integration-token-123456789";

struct Fixture {
    hub: HubClient,
    ws: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn setup(channels: Option<Value>) -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        public_url: Some(format!("ws://{addr}/v1/live")),
        ..HubConfig::default()
    })
    .unwrap();
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
    let hub = HubClient::new(&format!("http://{addr}"), TOKEN).unwrap();
    if channels.is_some() {
        hub.create_schema(json!({"id":"message","version":"1","schema":{"type":"integer"}}))
            .await
            .unwrap();
    }
    let mut registration = json!({"id":"game","mode":"internal"});
    if let Some(channels) = channels {
        registration["channels"] = channels;
    }
    hub.register_hub(registration).await.unwrap();
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertUser", json!([{"id":"bob"}])),
        ("upsertUser", json!([{"id":"carol"}])),
        ("upsertServer", json!([{"id":"room","capacity":3}])),
        ("join", json!(["alice", "room"])),
        ("join", json!(["bob", "room"])),
        ("join", json!(["carol", "room"])),
    ] {
        hub.call_hub("game", method, args.as_array().unwrap().clone())
            .await
            .unwrap();
    }
    Fixture {
        hub,
        ws: format!("ws://{addr}/v1/live"),
        task,
    }
}

async fn descriptor(f: &Fixture, user: &str) -> LiveDescriptor {
    let issued = f
        .hub
        .ticket(&[], "game", user, "room", "peer", None)
        .await
        .unwrap();
    let mut descriptor = LiveDescriptor::from_value(&issued).unwrap();
    descriptor.user_id = Some(user.into());
    descriptor
}

async fn join(f: &Fixture, user: &str) -> (LiveClient, mpsc::Receiver<LiveEvent>) {
    join_with(f, user, LiveClientOptions::default()).await
}

async fn join_with(
    f: &Fixture,
    user: &str,
    options: LiveClientOptions,
) -> (LiveClient, mpsc::Receiver<LiveEvent>) {
    LiveClient::connect(descriptor(f, user).await, options)
        .await
        .unwrap()
}

/// The next event other than a round-trip time.
async fn next(events: &mut mpsc::Receiver<LiveEvent>) -> LiveEvent {
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("an event in time")
            .expect("the event stream is open");
        if !matches!(event, LiveEvent::Rtt(_)) {
            return event;
        }
    }
}

async fn message(events: &mut mpsc::Receiver<LiveEvent>) -> LiveFrame {
    match next(events).await {
        LiveEvent::Message(frame) => frame,
        other => panic!("expected a message, got {other:?}"),
    }
}

#[tokio::test]
async fn two_clients_exchange_messages_and_see_presence() {
    let f = setup(None).await;
    let (alice, mut alice_events) = join(&f, "alice").await;
    match next(&mut alice_events).await {
        LiveEvent::Welcome {
            peers, reconnected, ..
        } => {
            assert!(peers.is_empty());
            assert!(!reconnected);
        }
        other => panic!("{other:?}"),
    }
    let (bob, mut bob_events) = join(&f, "bob").await;
    assert!(
        matches!(next(&mut bob_events).await, LiveEvent::Welcome { peers, .. } if peers == ["alice"])
    );
    assert_eq!(bob.peers(), ["alice"]);
    assert!(matches!(next(&mut alice_events).await, LiveEvent::Join { peer, .. } if peer == "bob"));
    assert_eq!(alice.peers(), ["bob"]);

    alice.send("chat", json!({"text":"hi"})).await.unwrap();
    let frame = message(&mut bob_events).await;
    assert_eq!(frame.from, "alice");
    assert_eq!(frame.channel, "chat");
    assert_eq!(frame.frame_type, "message");
    assert_eq!(frame.payload, json!({"text":"hi"}));
    assert_eq!(frame.to, None);

    bob.send_to("alice", "chat", json!([1, 2, 3]))
        .await
        .unwrap();
    bob.send_frame(
        LiveSend::new("moves", json!(7))
            .to("alice")
            .frame_type("move"),
    )
    .await
    .unwrap();
    let frame = message(&mut alice_events).await;
    assert_eq!(
        (frame.from.as_str(), frame.to.as_deref()),
        ("bob", Some("alice"))
    );
    assert_eq!(frame.payload, json!([1, 2, 3]));
    let frame = message(&mut alice_events).await;
    assert_eq!((frame.frame_type.as_str(), frame.seq), ("move", 1));

    bob.close().await;
    assert!(matches!(
        next(&mut bob_events).await,
        LiveEvent::Closed { error: None }
    ));
    assert!(bob.is_closed());
    assert_eq!(bob.send("chat", json!(null)).await, Err(LiveError::Closed));
    // The leave follows once the hub's resume window passes.
    assert!(matches!(next(&mut alice_events).await, LiveEvent::Leave { peer } if peer == "bob"));
    assert!(alice.peers().is_empty());
}

#[tokio::test]
async fn round_trip_time_is_measured() {
    let f = setup(None).await;
    let options = LiveClientOptions {
        rtt_interval: Some(Duration::from_millis(50)),
        ..LiveClientOptions::default()
    };
    let (alice, mut events) = join_with(&f, "alice", options).await;
    let mut seen = 0;
    while seen < 2 {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        if let LiveEvent::Rtt(rtt) = event {
            assert!(rtt < Duration::from_secs(5));
            seen += 1;
        }
    }
    assert!(alice.rtt().is_some());
    // Probes are not relayed to other peers.
    let (_bob, mut bob_events) = join(&f, "bob").await;
    next(&mut bob_events).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), next(&mut bob_events))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_schema_rejection_surfaces_the_hub_error() {
    let f = setup(Some(
        json!({"scores":{"delivery":"reliable","schemaRef":"message@1"}}),
    ))
    .await;
    let (alice, mut events) = join(&f, "alice").await;
    next(&mut events).await;
    alice.send("scores", json!(5)).await.unwrap();
    alice.send("scores", json!("not a number")).await.unwrap();
    let schema = LiveError::Hub {
        code: "schema".into(),
    };
    assert_eq!(next(&mut events).await, LiveEvent::Error(schema.clone()));
    assert_eq!(
        next(&mut events).await,
        LiveEvent::Closed {
            error: Some(schema.clone())
        }
    );
    assert!(alice.is_closed());

    // With `renew`, the client reconnects with a fresh ticket instead.
    let hub = f.hub.clone();
    let options = LiveClientOptions {
        retry_base: Duration::from_millis(10),
        ..LiveClientOptions::default()
    }
    .with_renew(move || {
        let hub = hub.clone();
        async move { hub.ticket(&[], "game", "bob", "room", "peer", None).await }
    });
    let (bob, mut events) = join_with(&f, "bob", options).await;
    next(&mut events).await;
    bob.send("scores", json!("bad")).await.unwrap();
    assert_eq!(next(&mut events).await, LiveEvent::Error(schema.clone()));
    assert!(
        matches!(next(&mut events).await, LiveEvent::Reconnecting { attempt: 1, error, .. } if error == schema)
    );
    assert!(matches!(
        next(&mut events).await,
        LiveEvent::Welcome {
            reconnected: true,
            ..
        }
    ));
    bob.send("scores", json!(1)).await.unwrap();
    assert!(!bob.is_closed());
}

#[tokio::test]
async fn oversize_frames_are_refused_both_ways() {
    let f = setup(None).await;
    let (alice, mut alice_events) = join(&f, "alice").await;
    next(&mut alice_events).await;
    let refused = alice.send("chat", json!("x".repeat(70_000))).await;
    assert!(
        matches!(refused, Err(LiveError::FrameTooLarge { limit: 65536, .. })),
        "{refused:?}"
    );
    // Nothing was sent: the connection is still open.
    alice.send("chat", json!("small")).await.unwrap();

    let small = LiveClientOptions {
        max_frame_bytes: 1024,
        ..LiveClientOptions::default()
    };
    let (_bob, mut bob_events) = join_with(&f, "bob", small).await;
    next(&mut bob_events).await;
    alice.send("chat", json!("y".repeat(4000))).await.unwrap();
    assert!(matches!(
        next(&mut bob_events).await,
        LiveEvent::Error(LiveError::FrameTooLarge { limit: 1024, .. })
    ));
    assert!(matches!(
        next(&mut bob_events).await,
        LiveEvent::Closed {
            error: Some(LiveError::FrameTooLarge { .. })
        }
    ));
}

#[tokio::test]
async fn bad_and_spent_tickets_are_refused() {
    let f = setup(None).await;
    let bad = LiveDescriptor::new(f.ws.clone(), "invalid.ticket");
    let refused = LiveClient::connect(bad, LiveClientOptions::default()).await;
    assert_eq!(
        refused.unwrap_err(),
        LiveError::Hub {
            code: "auth".into()
        }
    );
    let spent = descriptor(&f, "alice").await;
    let (alice, _events) = LiveClient::connect(spent.clone(), LiveClientOptions::default())
        .await
        .unwrap();
    let again = LiveClient::connect(spent, LiveClientOptions::default()).await;
    assert_eq!(again.unwrap_err().code(), Some("auth"));
    let no_ticket = LiveDescriptor::from_value(&json!({"url": f.ws}));
    assert!(matches!(no_ticket, Err(LiveError::InvalidDescriptor(_))));
    let reserved = alice
        .send_frame(LiveSend::new("x", json!(null)).frame_type("presence"))
        .await;
    assert_eq!(reserved, Err(LiveError::ReservedType("presence".into())));
}

#[tokio::test]
async fn interoperates_with_a_plain_websocket_peer() {
    let f = setup(None).await;
    let (alice, mut events) = join(&f, "alice").await;
    next(&mut events).await;
    let issued = f
        .hub
        .ticket(&[], "game", "bob", "room", "peer", None)
        .await
        .unwrap();
    let (mut bob, _) = connect_async(&f.ws).await.unwrap();
    bob.send(Message::Text(
        json!({"type":"auth","ticket":issued["ticket"]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let read = |text: Message| serde_json::from_str::<Value>(text.to_text().unwrap()).unwrap();
    let welcome = read(bob.next().await.unwrap().unwrap());
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(welcome["peers"], json!(["alice"]));
    assert_eq!(welcome["pong"], true);
    assert!(matches!(next(&mut events).await, LiveEvent::Join { peer, .. } if peer == "bob"));
    bob.send(Message::Text(
        json!({"v":1,"type":"move","channel":"game","seq":9,"payload":{"x":1}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let frame = message(&mut events).await;
    assert_eq!(
        (frame.from.as_str(), frame.frame_type.as_str(), frame.seq),
        ("bob", "move", 9)
    );
    alice.send_to("bob", "game", json!({"y":2})).await.unwrap();
    let received = loop {
        let value = read(bob.next().await.unwrap().unwrap());
        if value["type"] != "presence" {
            break value;
        }
    };
    assert_eq!(received["from"], "alice");
    assert_eq!(received["to"], "bob");
    assert_eq!(received["v"], 1);
    assert_eq!(received["payload"], json!({"y":2}));
    // A plain peer can probe the round trip too; the hub answers it alone.
    bob.send(Message::Text(
        json!({"type":"ping","id":"abc"}).to_string().into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        read(bob.next().await.unwrap().unwrap()),
        json!({"type":"pong","id":"abc"})
    );
    alice.close().await;
}
