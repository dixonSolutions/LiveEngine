#![cfg(feature = "host")]
//! Every typed `HubClient` method against real in-process services.
use liveengine::hub::{HubClient, HubConfig, HubService, HUB_PROTOCOL};
use serde_json::{json, Value};
use std::net::SocketAddr;

const TOKEN: &str = "typed-client-admin-token-1234567890";

async fn service() -> (String, tokio::task::JoinHandle<()>) {
    let service = HubService::new(HubConfig {
        token: TOKEN.into(),
        allowed_remote_hosts: vec!["127.0.0.1".into()],
        public_url: Some("ws://live.example.test/v1/live".into()),
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
    (base, task)
}

fn ids(list: &Value, key: &str, field: &str) -> Vec<String> {
    list[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item[field].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn contract_schemas_templates_and_hubs() {
    let (base, task) = service().await;
    let client = HubClient::new(&base, TOKEN).unwrap();

    let contract = client.contract().await.unwrap();
    assert_eq!(contract["protocol"], HUB_PROTOCOL);
    assert!(client.contract_schema().await.unwrap().is_object());

    let schema = json!({"type":"object","properties":{"n":{"type":"integer"}}});
    client
        .create_schema(json!({"id":"typed.state","version":"1","schema":schema}))
        .await
        .unwrap();
    assert!(client
        .create_schema(json!({"id":"typed.state","version":"1","schema":schema}))
        .await
        .is_err());
    assert!(ids(&client.list_schemas().await.unwrap(), "schemas", "ref")
        .contains(&"typed.state@1".to_owned()));
    assert_eq!(
        client.get_schema("typed.state@1").await.unwrap()["schema"],
        schema
    );
    assert_eq!(
        client.delete_schema("typed.state@1").await.unwrap(),
        Value::Null
    );
    assert!(client.get_schema("typed.state@1").await.is_err());

    client
        .create_template(json!({"name":"room","mode":"internal","scopes":{"kind":"typed"}}))
        .await
        .unwrap();
    assert_eq!(
        client.get_template("room").await.unwrap()["scopes"]["kind"],
        "typed"
    );
    assert!(client
        .list_templates()
        .await
        .unwrap()
        .to_string()
        .contains("room"));

    client
        .register_hub(json!({"id":"typed","mode":"internal","scopes":{"kind":"typed"}}))
        .await
        .unwrap();
    client
        .register_hub(json!({"id":"from-template","template":"room"}))
        .await
        .unwrap();
    let hubs = ids(&client.list_hubs().await.unwrap(), "hubs", "id");
    assert!(hubs.contains(&"typed".to_owned()) && hubs.contains(&"from-template".to_owned()));
    assert_eq!(client.get_hub("typed").await.unwrap()["id"], "typed");
    assert_eq!(
        client
            .call_hub("typed", "upsertUser", vec![json!({"id":"alice"})])
            .await
            .unwrap(),
        json!(true)
    );
    assert_eq!(
        client
            .call_hub("typed", "getUser", vec![json!("alice")])
            .await
            .unwrap()["id"],
        "alice"
    );
    assert_eq!(
        client.delete_hub("from-template").await.unwrap(),
        Value::Null
    );
    assert!(client.get_hub("from-template").await.is_err());
    assert_eq!(client.delete_template("room").await.unwrap(), Value::Null);
    assert!(client.get_template("room").await.is_err());
    task.abort();
}

#[tokio::test]
async fn registry_import_links_and_link_credentials() {
    let (local_base, local_task) = service().await;
    let (remote_base, remote_task) = service().await;
    let local = HubClient::new(&local_base, TOKEN).unwrap();
    let remote = HubClient::new(&remote_base, TOKEN).unwrap();
    remote
        .register_hub(json!({"id":"far","mode":"internal"}))
        .await
        .unwrap();
    remote
        .call_hub(
            "far",
            "upsertServer",
            vec![json!({"id":"room","capacity":4})],
        )
        .await
        .unwrap();

    local
        .import_registry(json!({"address":remote_base,"token":TOKEN,"prefix":"mirror"}))
        .await
        .unwrap();

    let credential = remote.issue_link_credential("far").await.unwrap();
    let token = credential["token"].as_str().unwrap().to_owned();
    assert!(token.len() >= 24);
    let created = local
        .create_link(json!({
            "id":"to-far","address":remote_base,"remoteToken":token,
            "hubId":"far","userId":"bridge","serverId":"room"
        }))
        .await
        .unwrap();
    assert_eq!(created["id"], "to-far");
    assert!(
        !created.to_string().contains(&token),
        "the credential is never echoed"
    );
    assert_eq!(
        ids(&local.list_links().await.unwrap(), "links", "id"),
        ["to-far"]
    );
    assert_eq!(local.get_link("to-far").await.unwrap()["hubId"], "far");
    assert_eq!(local.delete_link("to-far").await.unwrap(), Value::Null);
    assert!(local.get_link("to-far").await.is_err());
    assert_eq!(
        remote.revoke_link_credentials("far").await.unwrap(),
        Value::Null
    );
    assert!(remote.revoke_link_credentials("nowhere").await.is_err());
    local_task.abort();
    remote_task.abort();
}
