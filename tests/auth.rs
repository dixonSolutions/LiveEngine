#![cfg(feature = "host")]
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine as _};
use liveengine::{
    auth::{AuthError, AuthRequest, AuthVerifier, Identity},
    hub::{HubConfig, HubService, HubSpec},
};
use ring::{
    hmac,
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde_json::{json, Map, Value};
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const TOKEN: &str = "auth-integration-admin-token-123456789";
const SECRET: &str = "shared-secret-of-at-least-32-bytes-long!!";

async fn serve(service: &HubService) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = service.clone();
    tokio::spawn(async move {
        axum::serve(
            listener,
            server
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    base
}
fn config() -> HubConfig {
    HubConfig {
        token: TOKEN.into(),
        public_url: Some("https://live.example.test/v1/live".into()),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..HubConfig::default()
    }
}
async fn post(base: &str, path: &str, body: Value) -> (u16, Value) {
    let response = reqwest::Client::new()
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
async fn get(base: &str, path: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{base}{path}"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
/// Register hub `game` with the given auth and seat alice in `room`.
async fn hub_with(base: &str, auth: Value) -> u16 {
    let (status, _) = post(
        base,
        "/v1/hubs",
        json!({"id":"game","mode":"internal","auth":auth}),
    )
    .await;
    if status != 201 {
        return status;
    }
    seat(base).await;
    status
}
async fn seat(base: &str) {
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        let (status, _) = post(
            base,
            "/v1/hubs/game/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200);
    }
}
async fn ticket(base: &str, user: &str, credential: Option<&str>) -> (u16, Value) {
    let mut body = json!({"userId":user,"serverId":"room"});
    if let Some(credential) = credential {
        body["credential"] = credential.into();
    }
    post(base, "/v1/hubs/game/tickets", body).await
}
async fn accepts(base: &str, credential: &str) -> bool {
    let (status, body) = ticket(base, "alice", Some(credential)).await;
    if status == 200 {
        assert!(body["ticket"].is_string());
        true
    } else {
        // Every refusal is the same opaque answer.
        assert_eq!(status, 403, "{body}");
        assert_eq!(body, json!({"error":"credential refused"}));
        false
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn seg(value: &Value) -> String {
    B64.encode(serde_json::to_vec(value).unwrap())
}
fn hs256(header: Value, payload: Value, secret: &[u8]) -> String {
    let input = format!("{}.{}", seg(&header), seg(&payload));
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    format!("{input}.{}", B64.encode(hmac::sign(&key, input.as_bytes())))
}
fn eddsa(header: Value, payload: Value, pair: &Ed25519KeyPair) -> String {
    let input = format!("{}.{}", seg(&header), seg(&payload));
    format!("{input}.{}", B64.encode(pair.sign(input.as_bytes())))
}
fn keypair() -> (Ed25519KeyPair, String) {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let public = B64.encode(pair.public_key().as_ref());
    (pair, public)
}
fn mac_credential(payload: &Value, secret: &str) -> String {
    let first = seg(payload);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    format!("{first}.{}", B64.encode(hmac::sign(&key, first.as_bytes())))
}

#[tokio::test]
async fn token_kind_accepts_and_refuses_and_keeps_secrets() {
    let path = std::env::temp_dir().join(format!(
        "auth-token-{}-{}.json",
        std::process::id(),
        rand::random::<u64>()
    ));
    let mut config = config();
    config.state_file = Some(path.clone());
    let service = HubService::new(config.clone()).unwrap();
    service.initialize_state().await.unwrap();
    let base = serve(&service).await;
    let good = "player-alice-token-0123456789abcdef";
    let other = "player-bob-token-0123456789abcdef-x";
    let auth = json!({"kind":"token","tokens":[
        {"token":good,"identity":"alice","claims":{"team":"red","level":7}},
        {"token":other,"identity":"bob"}
    ]});
    assert_eq!(hub_with(&base, auth).await, 201);
    assert!(accepts(&base, good).await);
    assert!(!accepts(&base, "player-alice-token-0123456789abcdeX").await);
    assert!(!accepts(&base, "").await);
    assert!(!accepts(&base, &good[..good.len() - 1]).await);
    // A valid credential for another identity cannot mint alice's ticket.
    assert!(!accepts(&base, other).await);
    // Missing credential is refused.
    assert_eq!(ticket(&base, "alice", None).await.0, 403);
    // Non-members stay refused by membership even with a credential.
    assert_eq!(ticket(&base, "bob", Some(other)).await.0, 403);

    // Secrets are never returned by any route or the export, but the kind is public.
    for body in [
        get(&base, "/v1/hubs/game").await,
        get(&base, "/v1/hubs").await,
        get(&base, "/v1/registry/export").await,
        get(&base, "/v1/tree").await,
    ] {
        let text = body.to_string();
        assert!(!text.contains(good) && !text.contains(other), "{text}");
        assert!(!text.contains("team"), "{text}");
    }
    assert_eq!(
        get(&base, "/v1/hubs/game").await["auth"],
        json!({"kind":"token"})
    );
    // Secrets persist like remote tokens, with a private file, and survive a restart.
    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(saved.contains(good));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
            0
        );
    }
    let restored = HubService::new(config).unwrap();
    let base2 = serve(&restored).await;
    seat(&base2).await;
    assert!(accepts(&base2, good).await);
    assert!(!accepts(&base2, "player-alice-token-0123456789abcdeX").await);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn hub_without_auth_is_unchanged_and_bad_definitions_are_refused() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    assert_eq!(hub_with(&base, Value::Null).await, 201);
    assert_eq!(ticket(&base, "alice", None).await.0, 200);
    for (id, auth) in [
        ("a", json!({"kind":"none"})),
        ("b", json!({"kind":"token","tokens":[]})),
        ("c", json!({"kind":"token","tokens":[{"token":"short"}]})),
        ("d", json!({"kind":"hmac","secret":"short"})),
        ("e", json!({"kind":"jwt","alg":"none","secret":SECRET})),
        ("f", json!({"kind":"jwt","alg":"RS256","secret":SECRET})),
        ("g", json!({"kind":"jwt","alg":"HS256"})),
        (
            "h",
            json!({"kind":"jwt","alg":"HS256","secret":SECRET,"publicKey":"AAAA"}),
        ),
        ("i", json!({"kind":"jwt","alg":"EdDSA","publicKey":"AAAA"})),
        ("j", json!({"kind":"jwt","alg":"EdDSA","secret":SECRET})),
        (
            "k",
            json!({"kind":"webhook","url":"http://192.0.2.1:9/check"}),
        ),
        (
            "l",
            json!({"kind":"webhook","url":"http://127.0.0.1:9/x","timeoutMs":5}),
        ),
        ("m", json!({"kind":"custom","name":"missing"})),
        (
            "n",
            json!({"kind":"token","tokens":[{"token":"player-alice-token-0123456789abcdef"}],"extra":1}),
        ),
    ] {
        let (status, _) = post(
            &base,
            "/v1/hubs",
            json!({"id":id,"mode":"internal","auth":auth}),
        )
        .await;
        assert_eq!(status, 400, "{auth}");
    }
    let (status, _) = post(
        &base,
        "/v1/hubs",
        json!({"id":"c1","mode":"central","auth":{"kind":"hmac","secret":SECRET}}),
    )
    .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn hmac_kind_accepts_and_refuses() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"hmac","secret":SECRET})).await,
        201
    );
    let valid = mac_credential(&json!({"sub":"alice","exp":now()+60,"team":"blue"}), SECRET);
    assert!(accepts(&base, &valid).await);
    assert!(
        !accepts(
            &base,
            &mac_credential(
                &json!({"sub":"alice","exp":now()+60}),
                "another-secret-of-32-bytes-or-more!!"
            )
        )
        .await
    );
    assert!(
        !accepts(
            &base,
            &mac_credential(&json!({"sub":"alice","exp":now()-5}), SECRET)
        )
        .await
    );
    assert!(!accepts(&base, &mac_credential(&json!({"sub":"alice"}), SECRET)).await);
    assert!(
        !accepts(
            &base,
            &mac_credential(&json!({"sub":"bob","exp":now()+60}), SECRET)
        )
        .await
    );
    // Tampered payload under the old mac.
    let (_, mac) = valid.split_once('.').unwrap();
    let forged = format!(
        "{}.{mac}",
        seg(&json!({"sub":"alice","exp":now()+60,"team":"gold"}))
    );
    assert!(!accepts(&base, &forged).await);
    assert!(!accepts(&base, "not-a-credential").await);
}

#[tokio::test]
async fn jwt_hs256_accepts_and_refuses() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let auth = json!({"kind":"jwt","alg":"HS256","secret":SECRET,"issuer":"idp","audience":"game"});
    assert_eq!(hub_with(&base, auth).await, 201);
    let header = json!({"alg":"HS256","typ":"JWT"});
    let claims = |extra: Value| {
        let mut base = json!({"sub":"alice","exp":now()+60,"iss":"idp","aud":"game"});
        for (k, v) in extra.as_object().unwrap() {
            base[k] = v.clone();
        }
        base
    };
    let secret = SECRET.as_bytes();
    assert!(accepts(&base, &hs256(header.clone(), claims(json!({})), secret)).await);
    assert!(
        accepts(
            &base,
            &hs256(header.clone(), claims(json!({"aud":["x","game"]})), secret)
        )
        .await
    );
    for bad in [
        claims(json!({"exp":now()-10})),
        claims(json!({"nbf":now()+600})),
        claims(json!({"iss":"evil"})),
        claims(json!({"aud":"other"})),
        claims(json!({"sub":"bob"})),
        claims(json!({"sub":""})),
    ] {
        assert!(!accepts(&base, &hs256(header.clone(), bad, secret)).await);
    }
    let mut missing_exp = claims(json!({}));
    missing_exp.as_object_mut().unwrap().remove("exp");
    assert!(!accepts(&base, &hs256(header.clone(), missing_exp, secret)).await);
    assert!(
        !accepts(
            &base,
            &hs256(
                header.clone(),
                claims(json!({})),
                b"wrong-secret-of-at-least-32-bytes!!"
            )
        )
        .await
    );
    let good = hs256(header.clone(), claims(json!({})), secret);
    let mut tampered = good.clone();
    tampered.push('A');
    assert!(!accepts(&base, &tampered).await);
    assert!(!accepts(&base, &format!("{good}.extra")).await);
    assert!(!accepts(&base, "a.b").await);
}

#[tokio::test]
async fn jwt_eddsa_accepts_and_refuses_and_algorithm_confusion_is_refused() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let (pair, public) = keypair();
    let (rogue, _) = keypair();
    let auth = json!({"kind":"jwt","alg":"EdDSA","publicKey":public,"identityClaim":"uid"});
    assert_eq!(hub_with(&base, auth).await, 201);
    let header = json!({"alg":"EdDSA"});
    let claims = json!({"uid":"alice","exp":now()+60,"role":"admin"});
    assert!(accepts(&base, &eddsa(header.clone(), claims.clone(), &pair)).await);
    assert!(!accepts(&base, &eddsa(header.clone(), claims.clone(), &rogue)).await);
    assert!(
        !accepts(
            &base,
            &eddsa(header.clone(), json!({"uid":"alice","exp":now()-30}), &pair)
        )
        .await
    );
    // Identity claim is configured: `sub` alone is not an identity here.
    assert!(
        !accepts(
            &base,
            &eddsa(header.clone(), json!({"sub":"alice","exp":now()+60}), &pair)
        )
        .await
    );
    // HS256 token keyed with the public key bytes, the classic confusion attack.
    let public_bytes = B64.decode(&public).unwrap();
    for key in [public_bytes.clone(), public.clone().into_bytes()] {
        assert!(!accepts(&base, &hs256(json!({"alg":"HS256"}), claims.clone(), &key)).await);
        // The same token claiming the configured algorithm still fails verification.
        assert!(!accepts(&base, &hs256(json!({"alg":"EdDSA"}), claims.clone(), &key)).await);
    }
    // `none`, in several spellings, and an empty signature.
    for alg in ["none", "None", "NONE", "nOnE", ""] {
        let unsigned = format!("{}.{}.", seg(&json!({"alg":alg})), seg(&claims));
        assert!(!accepts(&base, &unsigned).await);
        let signed = eddsa(json!({"alg":alg}), claims.clone(), &pair);
        assert!(!accepts(&base, &signed).await);
    }
    // A valid signature under a header that names another algorithm, or none at all.
    assert!(!accepts(&base, &eddsa(json!({"alg":"HS256"}), claims.clone(), &pair)).await);
    assert!(!accepts(&base, &eddsa(json!({}), claims.clone(), &pair)).await);
    assert!(
        !accepts(
            &base,
            &eddsa(json!({"alg":"EdDSA","crit":["x"]}), claims.clone(), &pair)
        )
        .await
    );
}

#[tokio::test]
async fn jwt_hs256_hub_refuses_other_algorithms_and_none() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"jwt","alg":"HS256","secret":SECRET})).await,
        201
    );
    let claims = json!({"sub":"alice","exp":now()+60});
    let (pair, _) = keypair();
    assert!(!accepts(&base, &eddsa(json!({"alg":"EdDSA"}), claims.clone(), &pair)).await);
    for alg in ["none", "None", "HS384", "HS512", "RS256"] {
        let unsigned = format!("{}.{}.", seg(&json!({"alg":alg})), seg(&claims));
        assert!(!accepts(&base, &unsigned).await);
        assert!(
            !accepts(
                &base,
                &hs256(json!({"alg":alg}), claims.clone(), SECRET.as_bytes())
            )
            .await
        );
    }
    assert!(
        accepts(
            &base,
            &hs256(json!({"alg":"HS256"}), claims, SECRET.as_bytes())
        )
        .await
    );
}

/// A tiny webhook the tests control: returns (url, requests seen).
/// Every webhook request seen: the authorization header and the JSON body.
type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;
async fn webhook(status: u16, body: &'static str, delay: Duration) -> (String, Seen) {
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    let seen: Seen = Arc::default();
    let app =
        Router::new()
            .route(
                "/check",
                post(
                    move |State(seen): State<Seen>,
                          headers: HeaderMap,
                          Json(request): Json<Value>| async move {
                        let authorization = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_owned);
                        seen.lock().unwrap().push((authorization, request));
                        tokio::time::sleep(delay).await;
                        (
                            axum::http::StatusCode::from_u16(status).unwrap(),
                            [("content-type", "application/json")],
                            body,
                        )
                    },
                ),
            )
            .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/check", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, seen)
}

#[tokio::test]
async fn webhook_accepts_and_fails_closed() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let (url, seen) = webhook(
        200,
        r#"{"allow":true,"identity":"alice","claims":{"k":1}}"#,
        Duration::ZERO,
    )
    .await;
    let auth = json!({"kind":"webhook","url":url,"secret":"webhook-bearer-secret-value"});
    assert_eq!(hub_with(&base, auth).await, 201);
    assert!(accepts(&base, "opaque-credential").await);
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].0.as_deref(),
            Some("Bearer webhook-bearer-secret-value")
        );
        assert_eq!(seen[0].1["credential"], "opaque-credential");
        assert_eq!(seen[0].1["userId"], "alice");
        assert_eq!(seen[0].1["serverId"], "room");
        assert_eq!(seen[0].1["hub"], "game");
    }
    // No credential never reaches the webhook.
    assert_eq!(ticket(&base, "alice", None).await.0, 403);
    assert_eq!(seen.lock().unwrap().len(), 1);
    // The webhook secret is never returned.
    let text = get(&base, "/v1/registry/export").await.to_string()
        + &get(&base, "/v1/hubs/game").await.to_string();
    assert!(!text.contains("webhook-bearer-secret-value"));
}

#[tokio::test]
async fn webhook_fails_closed_on_status_body_and_identity() {
    for (status, body) in [
        (403, r#"{"allow":true}"#),
        (500, r#"{"allow":true}"#),
        (401, r#"{"allow":true}"#),
        (204, ""),
        (200, r#"{"allow":false}"#),
        (200, r#"{"identity":"alice"}"#),
        (200, r#"{"allow":"true"}"#),
        (200, "not json"),
        (200, ""),
        (200, r#"{"allow":true,"identity":"bob"}"#),
        (200, r#"{"allow":true,"claims":"x"}"#),
    ] {
        let service = HubService::new(config()).unwrap();
        let base = serve(&service).await;
        let (url, seen) = webhook(status, body, Duration::ZERO).await;
        assert_eq!(
            hub_with(&base, json!({"kind":"webhook","url":url})).await,
            201
        );
        assert!(!accepts(&base, "credential").await, "{status} {body}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }
    // 3xx redirects are not followed.
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let (target, target_seen) = webhook(200, r#"{"allow":true}"#, Duration::ZERO).await;
    let redirect = {
        use axum::{response::Redirect, routing::post, Router};
        let app = Router::new().route(
            "/check",
            post(move || {
                let target = target.clone();
                async move { Redirect::temporary(&target) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/check", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    };
    assert_eq!(
        hub_with(&base, json!({"kind":"webhook","url":redirect})).await,
        201
    );
    assert!(!accepts(&base, "credential").await);
    assert!(target_seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn webhook_fails_closed_on_timeout_and_unreachable() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let (slow, _) = webhook(200, r#"{"allow":true}"#, Duration::from_secs(5)).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"webhook","url":slow,"timeoutMs":300})).await,
        201
    );
    let started = Instant::now();
    assert!(!accepts(&base, "credential").await);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );

    // A listener that was closed: connection refused.
    let dead = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}/check", listener.local_addr().unwrap())
    };
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"webhook","url":dead})).await,
        201
    );
    assert!(!accepts(&base, "credential").await);
}

#[tokio::test]
async fn webhook_uses_the_allowed_host_rules() {
    let (url, _) = webhook(200, r#"{"allow":true}"#, Duration::ZERO).await;
    let mut config = config();
    config.allowed_remote_hosts = Vec::new();
    let service = HubService::new(config).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"webhook","url":url})).await,
        400
    );
    for url in [
        "ftp://127.0.0.1/x",
        "http://user:pw@127.0.0.1/x",
        "not a url",
    ] {
        let service = HubService::new(self::config()).unwrap();
        let base = serve(&service).await;
        assert_eq!(
            hub_with(&base, json!({"kind":"webhook","url":url})).await,
            400
        );
    }
}

type Calls = Arc<Mutex<Vec<(String, String, String, String)>>>;
struct Directory {
    calls: Calls,
}
impl AuthVerifier for Directory {
    fn verify<'a>(
        &'a self,
        request: &'a AuthRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Identity, AuthError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((
                request.hub.to_string(),
                request.room.to_string(),
                request.role.to_string(),
                request.credential.to_string(),
            ));
            match request.credential {
                "open-sesame" => {
                    let mut claims = Map::new();
                    claims.insert("tier".into(), json!("gold"));
                    Ok(Identity {
                        identity: request.user.to_string(),
                        claims,
                    })
                }
                "impostor" => Ok(Identity {
                    identity: "someone-else".into(),
                    claims: Map::new(),
                }),
                _ => Err(AuthError),
            }
        })
    }
}

#[tokio::test]
async fn custom_verifier_accepts_and_refuses() {
    let service = HubService::new(config()).unwrap();
    let calls = Calls::default();
    let directory = || Directory {
        calls: calls.clone(),
    };
    service.register_verifier("directory", directory()).unwrap();
    assert!(service.register_verifier("directory", directory()).is_err());
    assert!(service.register_verifier("bad name", directory()).is_err());
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"custom","name":"directory"})).await,
        201
    );
    assert!(accepts(&base, "open-sesame").await);
    assert!(!accepts(&base, "wrong").await);
    assert!(!accepts(&base, "impostor").await);
    assert_eq!(ticket(&base, "alice", None).await.0, 403);
    let calls = calls.lock().unwrap().clone();
    assert_eq!(
        calls[0],
        (
            "game".into(),
            "room".into(),
            "peer".into(),
            "open-sesame".into()
        )
    );
    assert_eq!(calls.len(), 3);
    assert_eq!(
        get(&base, "/v1/hubs/game").await["auth"],
        json!({"kind":"custom"})
    );
}

#[tokio::test]
async fn custom_verifier_registers_through_the_rust_api() {
    let service = HubService::new(config()).unwrap();
    service
        .register_verifier(
            "directory",
            Directory {
                calls: Calls::default(),
            },
        )
        .unwrap();
    let info = service
        .register(HubSpec {
            id: "game".into(),
            auth: Some(json!({"kind":"custom","name":"directory"})),
            ..HubSpec::default()
        })
        .await
        .unwrap();
    assert_eq!(info.auth.unwrap().kind, "custom");
    let base = serve(&service).await;
    seat(&base).await;
    assert!(accepts(&base, "open-sesame").await);
    assert!(!accepts(&base, "nope").await);
}

#[tokio::test]
async fn pairing_checks_the_credential_before_changing_membership() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let good = "player-carol-token-0123456789abcdef";
    let (status, _) = post(
        &base,
        "/v1/hubs",
        json!({"id":"game","mode":"internal","auth":{"kind":"token","tokens":[{"token":good,"identity":"carol"}]}}),
    )
    .await;
    assert_eq!(status, 201);
    let pair = |credential: Option<&str>| {
        let mut subject = json!({"kind":"peer","id":"carol","scopes":{}});
        if let Some(credential) = credential {
            subject["credential"] = credential.into();
        }
        json!({"subject":subject,"target":{"kind":"hub"}})
    };
    for bad in [None, Some("player-carol-token-0123456789abcdeX")] {
        let (status, body) = post(&base, "/v1/pair", pair(bad)).await;
        assert_eq!(status, 403, "{body}");
        let (_, user) = post(
            &base,
            "/v1/hubs/game/call",
            json!({"method":"getUser","args":["carol"]}),
        )
        .await;
        assert!(
            user["result"].is_null(),
            "refused pairing created the user: {user}"
        );
    }
    let (status, body) = post(&base, "/v1/pair", pair(Some(good))).await;
    assert_eq!(status, 200, "{body}");
    assert!(body["ticket"].is_string());
    assert!(!body.to_string().contains(good));
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
/// Connect with a ticket and return the socket with its welcome frame.
async fn join(base: &str, ticket: &str) -> (Socket, Value) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!(
        "{}/v1/live",
        base.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    socket
        .send(Message::Text(
            json!({"type":"auth","ticket":ticket}).to_string().into(),
        ))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    (
        socket,
        serde_json::from_str(&frame.into_text().unwrap()).unwrap(),
    )
}
async fn next_presence(socket: &mut Socket) -> Value {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("no presence frame")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "presence" {
                return value;
            }
        }
    }
}

#[tokio::test]
async fn verified_identity_and_claims_travel_into_welcome_and_presence() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let alice = "player-alice-token-0123456789abcdef";
    let bob = "player-bob-token-0123456789abcdef-x";
    let carol = "player-carol-token-0123456789abcdef";
    let auth = json!({"kind":"token","tokens":[
        {"token":alice,"identity":"alice","claims":{"team":"red","level":7}},
        {"token":bob,"identity":"bob","claims":{"team":"blue","nested":{"any":["thing"]}}},
        {"token":carol,"identity":"carol","claims":{"blob":"x".repeat(3000)}}
    ]});
    assert_eq!(hub_with(&base, auth).await, 201);
    for (method, args) in [
        ("upsertUser", json!([{"id":"bob"}])),
        ("upsertUser", json!([{"id":"carol"}])),
        ("join", json!(["bob", "room"])),
    ] {
        let (status, _) = post(
            &base,
            "/v1/hubs/game/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200);
    }
    let (status, issued) = ticket(&base, "alice", Some(alice)).await;
    assert_eq!(status, 200);
    let (mut a, welcome) = join(&base, issued["ticket"].as_str().unwrap()).await;
    assert_eq!(welcome["type"], "welcome");
    assert_eq!(
        welcome["identity"],
        json!({"identity":"alice","claims":{"team":"red","level":7}})
    );
    assert!(welcome.get("peerIdentities").is_none(), "{welcome}");
    let (status, issued) = ticket(&base, "bob", Some(bob)).await;
    assert_eq!(status, 200);
    let (_b, welcome_b) = join(&base, issued["ticket"].as_str().unwrap()).await;
    // Claims stay opaque: nested user-defined values arrive unchanged.
    assert_eq!(
        welcome_b["identity"],
        json!({"identity":"bob","claims":{"team":"blue","nested":{"any":["thing"]}}})
    );
    assert_eq!(
        welcome_b["peerIdentities"]["alice"],
        json!({"identity":"alice","claims":{"team":"red","level":7}})
    );
    // The peer already in the room is told who joined, with the same bounded claims.
    let presence = next_presence(&mut a).await;
    assert_eq!(presence["event"], "join");
    assert_eq!(presence["peer"], "bob");
    assert_eq!(
        presence["identity"],
        json!({"identity":"bob","claims":{"team":"blue","nested":{"any":["thing"]}}})
    );
    // No credential ever appears in a frame.
    for frame in [&welcome, &welcome_b, &presence] {
        let text = frame.to_string();
        assert!(!text.contains(alice) && !text.contains(bob), "{text}");
    }
    // Claims over the live bound are refused, not truncated.
    let (status, body) = ticket(&base, "carol", Some(carol)).await;
    assert_eq!(status, 413, "{body}");
    assert!(!body.to_string().contains(carol));
}

#[tokio::test]
async fn hub_without_auth_sends_no_identity() {
    let service = HubService::new(config()).unwrap();
    let base = serve(&service).await;
    let (status, _) = post(&base, "/v1/hubs", json!({"id":"game","mode":"internal"})).await;
    assert_eq!(status, 201);
    seat(&base).await;
    let (status, issued) = ticket(&base, "alice", None).await;
    assert_eq!(status, 200);
    let (_socket, welcome) = join(&base, issued["ticket"].as_str().unwrap()).await;
    assert_eq!(welcome["type"], "welcome");
    assert!(welcome.get("identity").is_none() && welcome.get("peerIdentities").is_none());
}

/// Accepts each credential once and counts every call, so a second check is visible.
#[derive(Clone, Default)]
struct Once {
    used: Arc<Mutex<std::collections::HashSet<String>>>,
    calls: Arc<Mutex<Vec<String>>>,
}
impl AuthVerifier for Once {
    fn verify<'a>(
        &'a self,
        request: &'a AuthRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Identity, AuthError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(request.credential.to_string());
            if !request.credential.starts_with("once-")
                || !self
                    .used
                    .lock()
                    .unwrap()
                    .insert(request.credential.to_string())
            {
                return Err(AuthError);
            }
            let mut claims = Map::new();
            claims.insert("tier".into(), json!("gold"));
            Ok(Identity {
                identity: request.user.to_string(),
                claims,
            })
        })
    }
}
impl Once {
    fn count(&self, credential: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| *c == credential)
            .count()
    }
}
async fn call_as(base: &str, token: &str, method: &str, path: &str, body: Value) -> (u16, Value) {
    let response = reqwest::Client::new()
        .request(method.parse().unwrap(), format!("{base}{path}"))
        .bearer_auth(token)
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
async fn a_single_use_credential_is_verified_once_by_pairing() {
    let service = HubService::new(config()).unwrap();
    let once = Once::default();
    service.register_verifier("once", once.clone()).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"custom","name":"once"})).await,
        201
    );
    let pair = |credential: &str| json!({"subject":{"kind":"peer","id":"carol","scopes":{},"credential":credential},"target":{"kind":"hub"}});
    let (status, body) = post(&base, "/v1/pair", pair("once-1")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(once.count("once-1"), 1, "the credential was checked twice");
    // The verified identity and claims are in the ticket.
    let room = body["room"].as_str().unwrap().to_owned();
    let claims = service
        .verify_ticket(body["ticket"].as_str().unwrap(), &["game".into()], &room)
        .unwrap();
    let identity = claims.identity.unwrap();
    assert_eq!(identity.identity, "carol");
    assert_eq!(identity.claims["tier"], "gold");
    // Spent, so a repeat is refused, and refused before membership changes.
    let (status, _) = post(&base, "/v1/pair", pair("once-1")).await;
    assert_eq!(status, 403);
    assert_eq!(once.count("once-1"), 2);
    // A hub that does not verify returns no receipt and no identity.
    let (status, _) = post(&base, "/v1/hubs", json!({"id":"open","mode":"internal"})).await;
    assert_eq!(status, 201);
}

#[tokio::test]
async fn a_verification_receipt_is_single_use_and_bound() {
    let service = HubService::new(config()).unwrap();
    let once = Once::default();
    service.register_verifier("once", once.clone()).unwrap();
    let base = serve(&service).await;
    assert_eq!(
        hub_with(&base, json!({"kind":"custom","name":"once"})).await,
        201
    );
    let (status, bob) = post(
        &base,
        "/v1/hubs/game/call",
        json!({"method":"upsertUser","args":[{"id":"bob"}]}),
    )
    .await;
    assert_eq!(status, 200, "{bob}");
    let receipt = |credential: &'static str, user: &'static str| {
        let base = base.clone();
        async move {
            let (status, body) = post(
                &base,
                "/v1/route",
                json!({"path":[],"op":"authorize","body":{"id":"game","userId":user,"serverId":"room","role":"peer","credential":credential}}),
            )
            .await;
            assert_eq!(status, 200, "{body}");
            body["receipt"].as_str().unwrap().to_owned()
        }
    };
    let r = receipt("once-a", "alice").await;
    let redeem = |verified: String, user: &'static str, role: &'static str| {
        let base = base.clone();
        async move {
            post(
                &base,
                "/v1/hubs/game/tickets",
                json!({"userId":user,"serverId":"room","role":role,"verified":verified}),
            )
            .await
        }
    };
    // Bound to its user, room-less mismatches and role.
    assert_eq!(redeem(r.clone(), "bob", "peer").await.0, 403);
    assert_eq!(redeem(r.clone(), "alice", "service").await.0, 403);
    assert_eq!(redeem("garbage".into(), "alice", "peer").await.0, 403);
    assert_eq!(redeem(format!("{r}x"), "alice", "peer").await.0, 403);
    // A wrong attempt does not spend it; the right one works once.
    let (status, body) = redeem(r.clone(), "alice", "peer").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(redeem(r, "alice", "peer").await.0, 403);
    // The credential was verified once, at `authorize`, and not again at ticket issue.
    assert_eq!(once.count("once-a"), 1);
    // A ticket's own signature is not a receipt, and a receipt is not a ticket.
    let (_, issued) = post(
        &base,
        "/v1/hubs/game/tickets",
        json!({"userId":"alice","serverId":"room","credential":"once-b"}),
    )
    .await;
    let ticket = issued["ticket"].as_str().unwrap().to_owned();
    assert_eq!(redeem(ticket, "alice", "peer").await.0, 403);
    let r2 = receipt("once-c", "alice").await;
    assert!(service
        .verify_ticket(&r2, &["game".into()], "room")
        .is_err());
}

#[tokio::test]
async fn a_hub_with_auth_behind_a_remote_central_refuses_bad_credentials() {
    const REMOTE: &str = "auth-remote-admin-token-1234567890123";
    let remote = HubService::new(HubConfig {
        token: REMOTE.into(),
        ..config()
    })
    .unwrap();
    let once = Once::default();
    remote.register_verifier("once", once.clone()).unwrap();
    let remote_base = serve(&remote).await;
    let (status, body) = call_as(
        &remote_base,
        REMOTE,
        "POST",
        "/v1/hubs",
        json!({"id":"game","mode":"internal","auth":{"kind":"custom","name":"once"}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":4}])),
        ("join", json!(["alice", "room"])),
    ] {
        let (status, _) = call_as(
            &remote_base,
            REMOTE,
            "POST",
            "/v1/hubs/game/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200);
    }
    let front = HubService::new(config()).unwrap();
    let front_base = serve(&front).await;
    let (status, body) = post(
        &front_base,
        "/v1/hubs",
        json!({"id":"r","mode":"central","address":remote_base,"remoteToken":REMOTE}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let via_ticket = |credential: Option<&'static str>| {
        let front_base = front_base.clone();
        async move {
            let mut body = json!({"id":"game","userId":"alice","serverId":"room"});
            if let Some(credential) = credential {
                body["credential"] = credential.into();
            }
            post(
                &front_base,
                "/v1/route",
                json!({"path":["r"],"op":"ticket","body":body}),
            )
            .await
        }
    };
    assert_eq!(via_ticket(None).await.0, 403);
    assert_eq!(via_ticket(Some("wrong")).await.0, 403);
    let (status, body) = via_ticket(Some("once-1")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(via_ticket(Some("once-1")).await.0, 403);
    // Pairing through the remote central: refused without a credential and without side
    // effects; accepted with a good one, which the remote verifies once.
    let pair = |credential: Option<&str>| {
        let mut subject = json!({"kind":"peer","id":"carol","scopes":{}});
        if let Some(credential) = credential {
            subject["credential"] = credential.into();
        }
        json!({"path":["r"],"subject":subject,"target":{"kind":"hub"}})
    };
    for bad in [None, Some("nope"), Some("once-1")] {
        let (status, body) = post(&front_base, "/v1/pair", pair(bad)).await;
        assert_eq!(status, 403, "{body}");
    }
    let (_, user) = call_as(
        &remote_base,
        REMOTE,
        "POST",
        "/v1/hubs/game/call",
        json!({"method":"getUser","args":["carol"]}),
    )
    .await;
    assert!(
        user["result"].is_null(),
        "refused pairing made a user: {user}"
    );
    let (status, body) = post(&front_base, "/v1/pair", pair(Some("once-2"))).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(once.count("once-2"), 1, "checked more than once");
    let room = body["room"].as_str().unwrap().to_owned();
    let claims = remote
        .verify_ticket(body["ticket"].as_str().unwrap(), &["game".into()], &room)
        .unwrap();
    assert_eq!(claims.identity.unwrap().identity, "carol");
}

#[tokio::test]
async fn an_imported_hub_with_auth_refuses_bad_credentials() {
    const REMOTE: &str = "auth-import-admin-token-12345678901234";
    let source = HubService::new(HubConfig {
        token: REMOTE.into(),
        ..config()
    })
    .unwrap();
    let source_base = serve(&source).await;
    let good = "player-alice-token-0123456789abcdef";
    let (status, body) = call_as(
        &source_base,
        REMOTE,
        "POST",
        "/v1/hubs",
        json!({"id":"game","mode":"internal","auth":{"kind":"token","tokens":[{"token":good,"identity":"alice","claims":{"team":"red"}},{"token":"player-dave-token-0123456789abcdef","identity":"dave","claims":{"team":"red"}}]}}),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":4}])),
        ("join", json!(["alice", "room"])),
    ] {
        let (status, _) = call_as(
            &source_base,
            REMOTE,
            "POST",
            "/v1/hubs/game/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200);
    }
    let importer = HubService::new(config()).unwrap();
    let base = serve(&importer).await;
    let (status, body) = post(
        &base,
        "/v1/registries/import",
        json!({"address":source_base,"token":REMOTE,"prefix":"b-"}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let imported = |credential: Option<&str>| {
        let mut body = json!({"userId":"alice","serverId":"room"});
        if let Some(credential) = credential {
            body["credential"] = credential.into();
        }
        let base = base.clone();
        async move { post(&base, "/v1/hubs/b-game/tickets", body).await }
    };
    assert_eq!(imported(None).await.0, 403);
    assert_eq!(
        imported(Some("player-alice-token-0123456789abcdeX"))
            .await
            .0,
        403
    );
    let (status, body) = imported(Some(good)).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        &base,
        "/v1/pair",
        json!({"subject":{"kind":"peer","id":"dave","scopes":{}},"target":{"kind":"hub"}}),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    let (status, body) = post(
        &base,
        "/v1/pair",
        json!({"subject":{"kind":"peer","id":"dave","scopes":{},"credential":"player-dave-token-0123456789abcdef"},"target":{"kind":"hub"}}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let room = body["room"].as_str().unwrap().to_owned();
    let claims = source
        .verify_ticket(body["ticket"].as_str().unwrap(), &["game".into()], &room)
        .unwrap();
    assert_eq!(claims.identity.unwrap().claims["team"], "red");
}

#[tokio::test]
async fn register_internal_has_no_auth_and_hub_spec_is_the_way_to_set_it() {
    use liveengine::hub::{HubMode, HubRegistration};
    let service = HubService::new(config()).unwrap();
    let info = service
        .register_internal(
            HubRegistration {
                id: "custom".into(),
                mode: HubMode::Internal,
                address: None,
                remote_token: None,
                remote_hub_id: None,
                scopes: json!({}),
                scope_schema: None,
                ttl_seconds: None,
            },
            liveengine::Engine::new(1),
        )
        .await
        .unwrap();
    assert!(info.auth.is_none());
    let base = serve(&service).await;
    for (method, args) in [
        ("upsertUser", json!([{"id":"alice"}])),
        ("upsertServer", json!([{"id":"room","capacity":2}])),
        ("join", json!(["alice", "room"])),
    ] {
        let (status, _) = post(
            &base,
            "/v1/hubs/custom/call",
            json!({"method":method,"args":args}),
        )
        .await;
        assert_eq!(status, 200);
    }
    let (status, _) = post(
        &base,
        "/v1/hubs/custom/tickets",
        json!({"userId":"alice","serverId":"room"}),
    )
    .await;
    assert_eq!(status, 200);
    // The same hub definition through HubSpec carries auth and refuses a missing credential.
    let info = service
        .register(HubSpec {
            id: "guarded".into(),
            auth: Some(
                json!({"kind":"token","tokens":[{"token":"player-alice-token-0123456789abcdef"}]}),
            ),
            ..HubSpec::default()
        })
        .await
        .unwrap();
    assert_eq!(info.auth.unwrap().kind, "token");
    let (status, _) = post(
        &base,
        "/v1/hubs/guarded/tickets",
        json!({"userId":"alice","serverId":"room"}),
    )
    .await;
    assert_eq!(status, 403);
}
