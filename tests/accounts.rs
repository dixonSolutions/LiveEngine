#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::{net::SocketAddr, path::PathBuf};
const ADMIN: &str = "a-long-throwaway-admin-credential-for-tests";
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(file: Option<PathBuf>) -> Server {
    server_with_lockout(file, 30).await
}
async fn server_with_lockout(file: Option<PathBuf>, lockout: u64) -> Server {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| std::env::set_var("LIVEENGINE_PASSWORD_ITERATIONS", "100000"));
    let config = HubConfig {
        login_lockout_seconds: lockout,
        token: ADMIN.into(),
        state_file: file,
        rate_limit_requests: 10000,
        ..Default::default()
    };
    let service = HubService::new(config).unwrap();
    service.initialize_state().await.unwrap();
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
        .unwrap();
    });
    Server {
        url: format!("http://{address}"),
        task,
    }
}
async fn req(
    s: &Server,
    method: reqwest::Method,
    path: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (u16, Value) {
    let c = reqwest::Client::new();
    let mut r = c.request(method, format!("{}{path}", s.url));
    if let Some(t) = token {
        r = r.bearer_auth(t);
    }
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await.unwrap();
    let status = r.status().as_u16();
    let bytes = r.bytes().await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn create(s: &Server, name: &str, role: &str) {
    let (status,body)=req(s,reqwest::Method::POST,"/v1/accounts",Some(ADMIN),Some(json!({"username":name,"role":role,"password":"long-password-123","claims":{"custom":1}}))).await;
    assert_eq!(status, 201, "{body}");
    assert!(!body.to_string().contains("password"));
}
async fn login(s: &Server, name: &str, password: &str) -> (u16, Value) {
    req(
        s,
        reqwest::Method::POST,
        "/v1/sessions",
        None,
        Some(json!({"username":name,"password":password})),
    )
    .await
}
#[tokio::test]
async fn accounts_sessions_roles_and_revocation() {
    let s = server(None).await;
    create(&s, "Alice", "admin").await;
    create(&s, "Bob", "user").await;
    assert_eq!(
        req(
            &s,
            reqwest::Method::POST,
            "/v1/accounts",
            Some(ADMIN),
            Some(json!({"username":"aLiCe","role":"user","password":"long-password-123"}))
        )
        .await
        .0,
        409
    );
    let start = std::time::Instant::now();
    let wrong = login(&s, "Alice", "incorrect-password").await;
    let wrong_time = start.elapsed();
    let start = std::time::Instant::now();
    let unknown = login(&s, "Nobody", "incorrect-password").await;
    let unknown_time = start.elapsed();
    assert_eq!(wrong, unknown);
    assert!(wrong_time.as_millis() > 5 && unknown_time.as_millis() > 5);
    assert!(wrong_time < unknown_time * 10 && unknown_time < wrong_time * 10);
    let (_, a) = login(&s, "alice", "long-password-123").await;
    let admin = a["token"].as_str().unwrap();
    let (_, b) = login(&s, "Bob", "long-password-123").await;
    let user = b["token"].as_str().unwrap();
    for method in [reqwest::Method::GET, reqwest::Method::PATCH] {
        assert_eq!(
            req(
                &s,
                method,
                "/v1/token",
                Some(admin),
                Some(json!({"token":"replacement-token-at-least-24-bytes"}))
            )
            .await
            .0,
            404
        );
    }
    assert_eq!(
        req(&s, reqwest::Method::GET, "/v1/accounts", Some(ADMIN), None)
            .await
            .0,
        200
    );
    assert_eq!(
        req(&s, reqwest::Method::GET, "/v1/accounts", Some(admin), None)
            .await
            .0,
        200
    );
    for (method, path) in [
        (reqwest::Method::GET, "/v1/accounts"),
        (reqwest::Method::POST, "/v1/accounts"),
        (reqwest::Method::GET, "/v1/accounts/Alice"),
        (reqwest::Method::PATCH, "/v1/accounts/Alice"),
        (reqwest::Method::DELETE, "/v1/accounts/Alice"),
        (reqwest::Method::GET, "/v1/hubs"),
        (reqwest::Method::POST, "/v1/hubs"),
        (reqwest::Method::GET, "/v1/schemas"),
        (reqwest::Method::GET, "/v1/links"),
        (reqwest::Method::GET, "/v1/tree"),
        (reqwest::Method::POST, "/v1/pair"),
        (reqwest::Method::POST, "/v1/discover"),
        (reqwest::Method::GET, "/v1/registry/export"),
        (reqwest::Method::GET, "/v1/contract"),
        (reqwest::Method::GET, "/v1/contract/schema"),
        (reqwest::Method::POST, "/v1/schemas"),
        (reqwest::Method::GET, "/v1/schemas/example"),
        (reqwest::Method::DELETE, "/v1/schemas/example"),
        (reqwest::Method::GET, "/v1/templates"),
        (reqwest::Method::POST, "/v1/templates"),
        (reqwest::Method::GET, "/v1/templates/example"),
        (reqwest::Method::DELETE, "/v1/templates/example"),
        (reqwest::Method::POST, "/v1/tickets/watch"),
        (reqwest::Method::POST, "/v1/links"),
        (reqwest::Method::DELETE, "/v1/links"),
        (reqwest::Method::GET, "/v1/links/example"),
        (reqwest::Method::DELETE, "/v1/links/example"),
        (reqwest::Method::GET, "/v1/hubs/example"),
        (reqwest::Method::DELETE, "/v1/hubs/example"),
        (reqwest::Method::POST, "/v1/hubs/example/call"),
        (reqwest::Method::POST, "/v1/hubs/example/tickets"),
        (reqwest::Method::POST, "/v1/hubs/example/link-credentials"),
        (reqwest::Method::DELETE, "/v1/hubs/example/link-credentials"),
        (reqwest::Method::POST, "/v1/registries/import"),
    ] {
        assert_eq!(
            req(&s, method, path, Some(user), None).await.0,
            401,
            "{path}"
        );
    }
    assert_eq!(
        req(
            &s,
            reqwest::Method::POST,
            "/v1/route",
            Some(user),
            Some(json!({"path":[],"op":"list","body":{}}))
        )
        .await
        .0,
        401
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/accounts/bob",
            Some(user),
            None
        )
        .await
        .0,
        200
    );
    let (_, b2) = login(&s, "bob", "long-password-123").await;
    let user2 = b2["token"].as_str().unwrap();
    assert_eq!(
        req(
            &s,
            reqwest::Method::POST,
            "/v1/accounts/me/password",
            Some(user),
            Some(json!({"current":"long-password-123","next":"another-password-456"}))
        )
        .await
        .0,
        204
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(user2),
            None
        )
        .await
        .0,
        401
    );
    let (_, b3) = login(&s, "bob", "another-password-456").await;
    let user3 = b3["token"].as_str().unwrap();
    assert_eq!(
        req(
            &s,
            reqwest::Method::DELETE,
            "/v1/sessions",
            Some(user3),
            None
        )
        .await
        .0,
        204
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(user3),
            None
        )
        .await
        .0,
        401
    );
    let (_, body) = req(&s, reqwest::Method::GET, "/v1/accounts", Some(admin), None).await;
    assert!(!body.to_string().contains("hash") && !body.to_string().contains("password"));
    assert_eq!(
        req(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/Alice",
            Some(ADMIN),
            Some(json!({"role":"user"}))
        )
        .await
        .0,
        200
    );
    assert_eq!(
        req(&s, reqwest::Method::GET, "/v1/accounts", Some(admin), None)
            .await
            .0,
        401
    );
}
#[tokio::test]
async fn durable_restart_and_failed_write_rollback() {
    let dir = std::env::temp_dir().join(format!(
        "liveengine-account-{:032x}",
        rand::random::<u128>()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let s = server(Some(path.clone())).await;
    create(&s, "Carol", "admin").await;
    let (_, v) = login(&s, "Carol", "long-password-123").await;
    let token = v["token"].as_str().unwrap().to_owned();
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(!raw.contains(&token) && !raw.contains("long-password-123"));
    drop(s);
    let s = server(Some(path.clone())).await;
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(&token),
            None
        )
        .await
        .0,
        200
    );
    std::fs::rename(&dir, dir.with_extension("moved")).unwrap();
    assert_eq!(
        req(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/Carol",
            Some(ADMIN),
            Some(json!({"enabled":false}))
        )
        .await
        .0,
        500
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(&token),
            None
        )
        .await
        .0,
        200
    );
    drop(s);
    std::fs::remove_dir_all(dir.with_extension("moved")).unwrap();
}

#[tokio::test]
async fn login_lockout_backoff_recovers_and_expired_session_is_denied() {
    let dir =
        std::env::temp_dir().join(format!("liveengine-expiry-{:032x}", rand::random::<u128>()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.json");
    let s = server_with_lockout(Some(path.clone()), 4).await;
    create(&s, "Dave", "user").await;
    for _ in 0..5 {
        assert_eq!(login(&s, "Dave", "incorrect-password").await.0, 401);
    }
    assert_eq!(login(&s, "Dave", "long-password-123").await.0, 401);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let (status, value) = login(&s, "Dave", "long-password-123").await;
    assert_eq!(status, 200);
    let token = value["token"].as_str().unwrap().to_owned();
    drop(s);
    let mut saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for session in saved["accounts"]["sessions"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        session["expiresAt"] = json!(0);
    }
    std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    let s = server(Some(path)).await;
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(&token),
            None
        )
        .await
        .0,
        401
    );
    drop(s);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn account_administration_revokes_sessions_and_validates_input() {
    let s = server(None).await;
    for (name, password, role) in [
        ("ab", "long-password-123", "user"),
        ("bad name", "long-password-123", "user"),
        ("valid", "short", "user"),
        ("valid", "long-password-123", "other"),
    ] {
        assert_eq!(
            req(
                &s,
                reqwest::Method::POST,
                "/v1/accounts",
                Some(ADMIN),
                Some(json!({"username": name, "password": password, "role": role}))
            )
            .await
            .0,
            400
        );
    }
    create(&s, "Eve", "user").await;
    let (_, account) = req(
        &s,
        reqwest::Method::PATCH,
        "/v1/accounts/Eve",
        Some(ADMIN),
        Some(json!({"claims":{"team":"blue"}})),
    )
    .await;
    assert_eq!(account["claims"]["team"], "blue");
    let (_, account) = req(
        &s,
        reqwest::Method::PATCH,
        "/v1/accounts/Eve",
        Some(ADMIN),
        Some(json!({"claims":null})),
    )
    .await;
    assert!(account.get("claims").is_none());
    let (_, value) = login(&s, "Eve", "long-password-123").await;
    let token = value["token"].as_str().unwrap();
    assert_eq!(
        req(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/Eve",
            Some(ADMIN),
            Some(json!({"enabled": false}))
        )
        .await
        .0,
        200
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(token),
            None
        )
        .await
        .0,
        401
    );
    assert_eq!(login(&s, "Eve", "long-password-123").await.0, 401);
    assert_eq!(
        req(
            &s,
            reqwest::Method::PATCH,
            "/v1/accounts/Eve",
            Some(ADMIN),
            Some(json!({"enabled": true, "password": "a-different-password"}))
        )
        .await
        .0,
        200
    );
    let (_, value) = login(&s, "Eve", "a-different-password").await;
    let token = value["token"].as_str().unwrap();
    assert_eq!(
        req(
            &s,
            reqwest::Method::DELETE,
            "/v1/accounts/Eve",
            Some(ADMIN),
            None
        )
        .await
        .0,
        204
    );
    assert_eq!(
        req(
            &s,
            reqwest::Method::GET,
            "/v1/sessions/me",
            Some(token),
            None
        )
        .await
        .0,
        401
    );
}

#[tokio::test]
async fn login_lockout_doubles_after_another_failure() {
    let s = server_with_lockout(None, 1).await;
    create(&s, "Backoff", "user").await;
    for _ in 0..5 {
        assert_eq!(login(&s, "Backoff", "incorrect-password").await.0, 401);
    }
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(login(&s, "Backoff", "incorrect-password").await.0, 401);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(login(&s, "Backoff", "long-password-123").await.0, 401);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(login(&s, "Backoff", "long-password-123").await.0, 200);
}

#[tokio::test]
async fn default_login_lockout_refuses_correct_password_for_thirty_seconds() {
    let s = server(None).await;
    create(&s, "DefaultLock", "user").await;
    for _ in 0..5 {
        assert_eq!(login(&s, "DefaultLock", "incorrect-password").await.0, 401);
    }
    let start = std::time::Instant::now();
    assert_eq!(login(&s, "DefaultLock", "long-password-123").await.0, 401);
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        start + std::time::Duration::from_secs(29),
    ))
    .await;
    assert_eq!(login(&s, "DefaultLock", "long-password-123").await.0, 401);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(login(&s, "DefaultLock", "long-password-123").await.0, 200);
}
