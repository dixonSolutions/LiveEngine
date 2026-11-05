#![cfg(feature = "host")]
use liveengine::hub::{HubConfig, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "schema-test-token-at-least-24-characters";
async fn server() -> (String, tokio::task::JoinHandle<()>) {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
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
    (format!("http://{addr}"), task)
}
async fn req(
    client: &reqwest::Client,
    base: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let mut req = client
        .request(method, format!("{base}{path}"))
        .bearer_auth(TOKEN);
    if let Some(body) = body {
        req = req.json(&body);
    }
    let res = req.send().await.unwrap();
    let status = res.status().as_u16();
    let text = res.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}
#[tokio::test]
async fn schema_registry_enforces_versions_and_independent_entity_schemas() {
    let (base, task) = server().await;
    let c = reqwest::Client::new();
    let post = reqwest::Method::POST;
    let get = reqwest::Method::GET;
    let del = reqwest::Method::DELETE;
    assert_eq!(
        req(
            &c,
            &base,
            get.clone(),
            "/v1/schemas/liveengine.default@1",
            None
        )
        .await
        .0,
        200
    );
    let schema = json!({"id":"geo","version":"1","schema":{"type":"object","$defs":{"region":{"enum":["au"]}},"properties":{"region":{"$ref":"#/$defs/region"}},"required":["region"]}});
    assert_eq!(
        req(&c, &base, post.clone(), "/v1/schemas", Some(schema.clone()))
            .await
            .0,
        201
    );
    assert_eq!(
        req(&c, &base, post.clone(), "/v1/schemas", Some(schema))
            .await
            .0,
        409
    );
    assert_eq!(
        req(
            &c,
            &base,
            post.clone(),
            "/v1/schemas",
            Some(json!({"id":"evil","version":"1","schema":{"$ref":"http://127.0.0.1/secret"}}))
        )
        .await
        .0,
        400
    );
    assert_eq!(req(&c,&base,post.clone(),"/v1/hubs",Some(json!({"id":"game","mode":"internal","scopes":{"region":"au"},"schemaRef":"geo@1","userSchema":{"type":"object","required":["skill"]},"serverSchema":{"type":"object","required":["capacity"]}}))).await.0,201);
    assert_eq!(
        req(&c, &base, del.clone(), "/v1/schemas/geo@1", None)
            .await
            .0,
        409
    );
    for (method, scopes, expected) in [
        ("upsertUser", json!({}), 400),
        ("upsertUser", json!({"skill":5}), 200),
        ("upsertServer", json!({}), 400),
        ("upsertServer", json!({"capacity":1}), 200),
    ] {
        assert_eq!(
            req(
                &c,
                &base,
                post.clone(),
                "/v1/hubs/game/call",
                Some(json!({"method":method,"args":[{"id":method,"scopes":scopes}]}))
            )
            .await
            .0,
            expected
        );
    }
    assert_eq!(
        req(
            &c,
            &base,
            post,
            "/v1/hubs/game/call",
            Some(json!({"method":"generateUsers","args":[1]}))
        )
        .await
        .0,
        400
    );
    assert_eq!(
        req(&c, &base, del.clone(), "/v1/hubs/game", None).await.0,
        204
    );
    assert_eq!(req(&c, &base, del, "/v1/schemas/geo@1", None).await.0, 204);
    task.abort();
}
#[tokio::test]
async fn registry_import_transfers_schemas_and_rejects_conflicts() {
    let (remote, remote_task) = server().await;
    let (local, local_task) = server().await;
    let c = reqwest::Client::new();
    let create = |schema: Value| json!({"id":"rating","version":"1","schema":schema});
    assert_eq!(
        req(
            &c,
            &remote,
            reqwest::Method::POST,
            "/v1/schemas",
            Some(create(json!({"type":"object","required":["rating"]})))
        )
        .await
        .0,
        201
    );
    assert_eq!(
        req(
            &c,
            &remote,
            reqwest::Method::POST,
            "/v1/hubs",
            Some(
                json!({"id":"game","mode":"internal","scopes":{"rating":1},"schemaRef":"rating@1"})
            )
        )
        .await
        .0,
        201
    );
    let (_, export) = req(
        &c,
        &remote,
        reqwest::Method::GET,
        "/v1/registry/export",
        None,
    )
    .await;
    assert!(export["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["ref"] == "rating@1"));
    assert!(!export.to_string().contains(TOKEN));
    assert_eq!(
        req(
            &c,
            &local,
            reqwest::Method::POST,
            "/v1/registries/import",
            Some(json!({"address":remote,"token":TOKEN}))
        )
        .await
        .0,
        200
    );
    assert_eq!(
        req(
            &c,
            &local,
            reqwest::Method::POST,
            "/v1/hubs/game/call",
            Some(json!({"method":"upsertUser","args":[{"id":"bad"}]}))
        )
        .await
        .0,
        400
    );
    let (other, other_task) = server().await;
    assert_eq!(
        req(
            &c,
            &other,
            reqwest::Method::POST,
            "/v1/schemas",
            Some(create(json!({"type":"boolean"})))
        )
        .await
        .0,
        201
    );
    assert_eq!(
        req(
            &c,
            &local,
            reqwest::Method::POST,
            "/v1/registries/import",
            Some(json!({"address":other,"token":TOKEN,"prefix":"other"}))
        )
        .await
        .0,
        409
    );
    remote_task.abort();
    local_task.abort();
    other_task.abort();
}
