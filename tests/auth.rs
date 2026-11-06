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
async fn webhook(
    status: u16,
    body: &'static str,
    delay: Duration,
) -> (String, Arc<Mutex<Vec<(Option<String>, Value)>>>) {
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    let seen: Arc<Mutex<Vec<(Option<String>, Value)>>> = Arc::default();
    let app = Router::new()
        .route(
            "/check",
            post(
                move |State(seen): State<Arc<Mutex<Vec<(Option<String>, Value)>>>>,
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
