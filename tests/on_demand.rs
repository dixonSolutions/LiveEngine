#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};

const TOKEN: &str = "on-demand-test-token-at-least-24-characters";
#[tokio::test]
async fn templates_idempotency_expiry_quota_and_security() {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        max_hubs: 2,
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
    let client = reqwest::Client::new();
    async fn req(
        c: &reqwest::Client,
        base: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let mut r = c
            .request(method, format!("{base}{path}"))
            .bearer_auth(TOKEN);
        if let Some(body) = body {
            r = r.json(&body);
        }
        let r = r.send().await.unwrap();
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }
    let post = reqwest::Method::POST;
    let get = reqwest::Method::GET;
    let del = reqwest::Method::DELETE;
    assert_eq!(
        client
            .get(format!("{base}/v1/templates"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(req(&client,&base,post.clone(),"/v1/templates",Some(json!({"name":"unsafe","mode":"external","address":"http://127.0.0.1:1","remoteToken":TOKEN}))).await.0,400);
    assert_eq!(
        req(
            &client,
            &base,
            post.clone(),
            "/v1/templates",
            Some(json!({"name":"game","mode":"internal","ttlSeconds":1,"scopes":{"a":1}}))
        )
        .await
        .0,
        201
    );
    assert_eq!(
        req(&client, &base, get.clone(), "/v1/templates/game", None)
            .await
            .1["name"],
        "game"
    );
    let (status, value) = req(
        &client,
        &base,
        post.clone(),
        "/v1/hubs",
        Some(json!({"id":"one","template":"game","scopes":{"b":2},"ifAbsent":true})),
    )
    .await;
    assert_eq!(status, 201);
    assert_eq!(value["scopes"], json!({"b":2}));
    let (status, value) = req(
        &client,
        &base,
        post.clone(),
        "/v1/hubs",
        Some(json!({"id":"one","mode":"internal","ifAbsent":true})),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(value["scopes"], json!({"b":2}));
    assert_eq!(
        req(
            &client,
            &base,
            post.clone(),
            "/v1/hubs",
            Some(json!({"id":"two","mode":"internal"}))
        )
        .await
        .0,
        201
    );
    assert_eq!(
        req(
            &client,
            &base,
            post.clone(),
            "/v1/hubs",
            Some(json!({"id":"three","mode":"internal"}))
        )
        .await
        .0,
        507
    );
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        req(
            &client,
            &base,
            post,
            "/v1/hubs",
            Some(json!({"id":"three","mode":"internal"}))
        )
        .await
        .0,
        201
    );
    let (_, list) = req(&client, &base, get, "/v1/templates", None).await;
    assert!(!list.to_string().contains(TOKEN));
    assert_eq!(
        req(&client, &base, del, "/v1/templates/game", None).await.0,
        204
    );
    task.abort();
}

#[tokio::test]
async fn template_credentials_are_write_only() {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
    let client = reqwest::Client::new();
    let secret = "template-credential-at-least-24-characters";
    let res = client.post(format!("{base}/v1/templates")).bearer_auth(TOKEN)
        .json(&json!({"name":"remote","mode":"external","address":"http://127.0.0.1:1","remoteToken":secret})).send().await.unwrap();
    assert_eq!(res.status(), 201);
    assert!(!res.text().await.unwrap().contains(secret));
    for path in [
        "/v1/templates",
        "/v1/templates/remote",
        "/v1/hubs",
        "/v1/registry/export",
    ] {
        let body = client
            .get(format!("{base}{path}"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(!body.contains(secret), "credential leaked from {path}");
    }
    task.abort();
}

#[tokio::test]
async fn routed_templates_and_central_quotas_share_global_budget() {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        max_hubs: 4,
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
    let client = reqwest::Client::new();
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
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/templates",
            json!({"name":"game","mode":"internal","ttlSeconds":1})
        )
        .await
        .0,
        201
    );
    let (status, central) = post(
        &client,
        &base,
        "/v1/hubs",
        json!({"id":"a","mode":"central","maxHubs":1}),
    )
    .await;
    assert_eq!(status, 201);
    assert_eq!(central["maxHubs"], 1);
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"bad","mode":"central","maxHubs":0})
        )
        .await
        .0,
        400
    );
    let register = |id| json!({"path":["a"],"op":"register","body":{"id":id,"template":"game","ifAbsent":true}});
    assert_eq!(
        post(&client, &base, "/v1/route", register("one")).await.0,
        201
    );
    assert_eq!(
        post(&client, &base, "/v1/route", register("one")).await.0,
        200
    );
    assert_eq!(
        post(&client, &base, "/v1/route", register("two")).await.0,
        507
    );
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"b","mode":"internal"})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"c","mode":"internal"})
        )
        .await
        .0,
        201
    );
    assert_eq!(
        post(
            &client,
            &base,
            "/v1/hubs",
            json!({"id":"d","mode":"internal"})
        )
        .await
        .0,
        507
    );
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(
        post(&client, &base, "/v1/route", register("two")).await.0,
        201
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/route"))
            .json(&register("three"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    task.abort();
}

#[tokio::test]
async fn templates_apply_auth_and_provider_hosting() {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        ..HubConfig::default()
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
    let client = reqwest::Client::new();
    let post = |path: &str, body: Value| {
        client
            .post(format!("{base}{path}"))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
    };
    assert_eq!(post("/v1/templates", json!({"name":"secure","mode":"internal","auth":{"kind":"token","tokens":[{"token":"template-user-token-at-least-24-chars"}]}})).await.unwrap().status(), 201);
    let response = post("/v1/hubs", json!({"id":"game","template":"secure"}))
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(
        response.json::<Value>().await.unwrap()["auth"]["kind"],
        "token"
    );
    assert_eq!(
        post(
            "/v1/templates",
            json!({"name":"hosted","mode":"central","hosting":"provider"})
        )
        .await
        .unwrap()
        .status(),
        201
    );
    let response = post("/v1/hubs", json!({"id":"central","template":"hosted"}))
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert!(response.json::<Value>().await.unwrap()["credential"]
        .as_str()
        .is_some());
    task.abort();
}
