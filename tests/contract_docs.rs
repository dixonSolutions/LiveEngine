#![cfg(feature = "host")]

use serde_json::{json, Value};

#[test]
fn contract_describes_live_registration_and_link_routes() {
    let mut schema: Value =
        serde_json::from_str(include_str!("../docs/hub-contract.schema.json")).unwrap();
    let routes = &schema["x-routes"];
    for (path, methods) in [
        ("/v1/hubs/{id}/link-credentials", &["POST", "DELETE"][..]),
        ("/v1/discover", &["POST"][..]),
        ("/v1/pair", &["POST"][..]),
        ("/v1/links/{id}", &["GET", "DELETE"][..]),
        ("/v1/live", &["GET (WebSocket)"][..]),
        ("/v1/live/registry", &["GET (WebSocket)"][..]),
    ] {
        for method in methods {
            assert!(
                routes[path].as_array().unwrap().contains(&json!(method)),
                "{method} {path}"
            );
        }
    }
    assert_eq!(
        schema["$defs"]["TicketRequest"]["properties"]["verified"]["type"],
        "string"
    );
    schema["$ref"] = json!("#/$defs/HubRegistration");
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .unwrap();
    assert!(
        validator.is_valid(&json!({"id":"demo", "mode":"internal", "channels": {
            "chat": {"delivery":"reliable", "maxRate": 10}
        }}))
    );
    assert!(
        !validator.is_valid(&json!({"id":"demo", "mode":"internal", "channels": {
            "chat": {"delivery":"invalid"}
        }}))
    );
}
