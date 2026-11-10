#![cfg(feature = "host")]
//! A live room driven over a transport defined here, plugged in through
//! `HubService::serve_live_transport`, alongside a WebSocket peer in the same room.
use axum::extract::ws::Message;
use futures_util::{SinkExt, StreamExt};
use liveengine::{
    hub::{HubConfig, HubService, LiveLimits},
    transport::{Incoming, LiveTransport, Outgoing, Received, TransportError},
};
use serde_json::{json, Value};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

const TOKEN: &str = "live-transport-integration-token-123456789";

/// The server's end of an in-memory connection: frames arrive on one channel and leave on another.
struct MemoryTransport {
    incoming: mpsc::Receiver<Message>,
    outgoing: mpsc::Sender<Message>,
}

impl LiveTransport for MemoryTransport {
    fn name(&self) -> &'static str {
        "memory"
    }
    fn recv(&mut self) -> Received<'_> {
        Box::pin(async move { self.incoming.recv().await.map(Ok) })
    }
    fn split(self) -> (Outgoing, Incoming) {
        let sink = futures_util::sink::unfold(self.outgoing, |tx, message: Message| async move {
            tx.send(message).await.map_err(|_| TransportError)?;
            Ok::<_, TransportError>(tx)
        });
        let stream = futures_util::stream::unfold(self.incoming, |mut rx| async move {
            rx.recv().await.map(|message| (Ok(message), rx))
        });
        (Box::pin(sink), Box::pin(stream))
    }
}

/// The peer's end of an in-memory connection.
struct MemoryPeer {
    send: mpsc::Sender<Message>,
    receive: mpsc::Receiver<Message>,
}

impl MemoryPeer {
    /// Connect to the service and run the connection on its own task.
    fn connect(service: &HubService, remote: Option<IpAddr>) -> Self {
        let (send, incoming) = mpsc::channel(64);
        let (outgoing, receive) = mpsc::channel(64);
        let service = service.clone();
        tokio::spawn(async move {
            service
                .serve_live_transport(MemoryTransport { incoming, outgoing }, remote)
                .await
        });
        Self { send, receive }
    }
    /// Send a frame; a connection the service already closed ignores it.
    async fn send(&self, value: Value) {
        let _ = self
            .send
            .send(Message::Text(value.to_string().into()))
            .await;
    }
    async fn read(&mut self) -> Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(2), self.receive.recv())
                .await
                .expect("no frame from the service")
                .expect("connection ended");
            if let Message::Text(text) = message {
                return serde_json::from_str(&text).unwrap();
            }
        }
    }
}

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

struct Fixture {
    service: HubService,
    base: String,
    client: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn setup(live_limits: LiveLimits) -> Fixture {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        public_url: Some("ws://localhost/v1/live".into()),
        live_limits,
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = service.router();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let f = Fixture {
        service,
        base: format!("http://{addr}"),
        client: reqwest::Client::new(),
        task,
    };
    assert_eq!(
        post(
            &f.client,
            &f.base,
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
        ("join", json!(["bob", "room"])),
    ] {
        let body = json!({"method":method,"args":args});
        assert_eq!(
            post(&f.client, &f.base, "/v1/hubs/game/call", body).await.0,
            200
        );
    }
    f
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn read_bob(bob: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(2), bob.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(&message.into_text().unwrap()).unwrap()
}

async fn ticket(f: &Fixture, user: &str) -> String {
    let body = json!({"userId":user,"serverId":"room"});
    let (status, value) = post(&f.client, &f.base, "/v1/hubs/game/tickets", body).await;
    assert_eq!(status, 200);
    value["ticket"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn a_room_runs_over_a_custom_transport() {
    let f = setup(LiveLimits::builtin()).await;
    let mut alice = MemoryPeer::connect(&f.service, None);
    alice
        .send(json!({"type":"auth","ticket":ticket(&f, "alice").await}))
        .await;
    let welcome = alice.read().await;
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(welcome["peers"], json!([]));

    // A WebSocket peer joins the same room and sees the memory peer.
    let addr = f.base.trim_start_matches("http://");
    let (mut bob, _) = connect_async(format!("ws://{addr}/v1/live")).await.unwrap();
    let auth = json!({"type":"auth","ticket":ticket(&f, "bob").await});
    bob.send(WsMessage::Text(auth.to_string().into()))
        .await
        .unwrap();
    assert_eq!(read_bob(&mut bob).await["peers"], json!(["alice"]));
    let joined = alice.read().await;
    assert_eq!(
        (joined["event"].as_str(), joined["peer"].as_str()),
        (Some("join"), Some("bob"))
    );

    alice
        .send(json!({"v":1,"type":"data","channel":"x","seq":1,"payload":{"n":1}}))
        .await;
    let frame = read_bob(&mut bob).await;
    assert_eq!(
        (frame["from"].as_str(), &frame["payload"]),
        (Some("alice"), &json!({"n":1}))
    );
    let reply = json!({"v":1,"type":"data","channel":"x","seq":1,"payload":"hi","to":"alice"});
    bob.send(WsMessage::Text(reply.to_string().into()))
        .await
        .unwrap();
    let frame = alice.read().await;
    assert_eq!(
        (frame["from"].as_str(), frame["payload"].as_str()),
        (Some("bob"), Some("hi"))
    );

    // The connection listing names each transport.
    let connections: Value = f
        .client
        .get(format!("{}/v1/connections", f.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut transports: Vec<_> = connections
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["user"].as_str().unwrap(),
                c["transport"].as_str().unwrap(),
            )
        })
        .collect();
    transports.sort();
    assert_eq!(transports, [("alice", "memory"), ("bob", "websocket")]);

    // Room rules apply as over a WebSocket: a spoofed sender ends the connection.
    alice
        .send(json!({"v":1,"type":"data","channel":"x","seq":2,"payload":null,"from":"bob"}))
        .await;
    assert_eq!(alice.read().await, json!({"type":"error","code":"frame"}));
}

#[tokio::test]
async fn a_custom_transport_gets_the_same_auth_and_limits() {
    let f = setup(LiveLimits {
        max_preauth_per_ip: 1,
        max_frame_bytes: 1024,
        ..LiveLimits::builtin()
    })
    .await;
    // A bad or reused ticket is refused.
    let mut forged = MemoryPeer::connect(&f.service, None);
    forged
        .send(json!({"type":"auth","ticket":"invalid.ticket"}))
        .await;
    assert_eq!(forged.read().await, json!({"type":"error","code":"auth"}));
    let used = ticket(&f, "alice").await;
    let mut alice = MemoryPeer::connect(&f.service, None);
    alice.send(json!({"type":"auth","ticket":used})).await;
    assert_eq!(alice.read().await["type"], "welcome");
    let mut replay = MemoryPeer::connect(&f.service, None);
    replay.send(json!({"type":"auth","ticket":used})).await;
    assert_eq!(replay.read().await, json!({"type":"error","code":"auth"}));

    // A frame over the configured size ends the connection.
    alice
        .send(json!({"v":1,"type":"data","channel":"x","seq":1,"payload":"x".repeat(2000)}))
        .await;
    assert_eq!(alice.read().await, json!({"type":"error","code":"frame"}));

    // One pending connection per address: a second from it is refused until the first authenticates.
    let address = Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)));
    let pending = MemoryPeer::connect(&f.service, address);
    let mut refused = MemoryPeer::connect(&f.service, address);
    assert_eq!(refused.read().await, json!({"type":"error","code":"rate"}));
    let other = Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 8)));
    let mut bob = MemoryPeer::connect(&f.service, other);
    bob.send(json!({"type":"auth","ticket":ticket(&f, "bob").await}))
        .await;
    assert_eq!(bob.read().await["type"], "welcome");
    drop(pending);
    let mut admitted = None;
    for _ in 0..50 {
        let mut peer = MemoryPeer::connect(&f.service, address);
        peer.send(json!({"type":"auth","ticket":ticket(&f, "alice").await}))
            .await;
        let reply = peer.read().await;
        if reply["type"] == "welcome" {
            admitted = Some(peer);
            break;
        }
        assert_eq!(reply, json!({"type":"error","code":"rate"}));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(admitted.is_some(), "the pending slot was not released");
}
