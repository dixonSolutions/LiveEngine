use liveengine::Engine;
use serde_json::{json, Value};

fn call(e: &mut Engine, method: &str, args: Vec<Value>) -> Value {
    e.dispatch(method, args).unwrap()
}

#[test]
fn matches_pairs_and_emits_lifecycle_events() {
    let mut e = Engine::new(3);
    assert_eq!(
        call(
            &mut e,
            "upsertServer",
            vec![json!({"id":"s","capacity":2,"minMembers":2})]
        ),
        json!(true)
    );
    call(
        &mut e,
        "upsertUsers",
        vec![json!([{"id":"a"},{"id":"b"},{"id":"c"}])],
    );
    let result = call(&mut e, "matchAll", vec![]);
    assert_eq!(result.as_array().unwrap().len(), 2);
    assert_eq!(
        call(&mut e, "getServer", vec![json!("s")])["status"],
        "full"
    );
    let events = call(&mut e, "drainEvents", vec![]);
    let kinds: Vec<_> = events
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "server_added",
            "server_available",
            "user_added",
            "user_added",
            "user_added",
            "paired",
            "paired",
            "server_full",
            "server_ready"
        ]
    );
    assert_eq!(call(&mut e, "leave", vec![json!("a")])["ok"], true);
    assert_eq!(
        call(&mut e, "getServer", vec![json!("s")])["status"],
        "open"
    );
}

#[test]
fn requirements_and_affinity_match() {
    let mut e = Engine::new(1);
    call(
        &mut e,
        "upsertServers",
        vec![json!([
            {"id":"near","scopes":{"rating":1510},"capacity":2},
            {"id":"far","scopes":{"rating":1800},"capacity":2}
        ])],
    );
    call(
        &mut e,
        "upsertUser",
        vec![
            json!({"id":"u","scopes":{"rating":1500},"requirements":{"rating":{"between":[1400,1600]}}}),
        ],
    );
    let got = call(
        &mut e,
        "findServersForUser",
        vec![json!("u"), json!({"affinity":["rating"]})],
    );
    assert_eq!(got[0]["id"], "near");
    assert_eq!(got[0]["score"], 10.0);
}

#[test]
fn generated_users_are_repeatable_and_events_drain() {
    let spec = json!({"idPrefix":"bot-","scopes":{"rating":{"type":"int","min":1,"max":9},"tags":{"type":"tags","values":["a","b","c"],"count":2}}});
    let mut a = Engine::new(99);
    let mut b = Engine::new(99);
    let ua = call(&mut a, "generateUsers", vec![json!(10), spec.clone()]);
    let ub = call(&mut b, "generateUsers", vec![json!(10), spec]);
    assert_eq!(ua, ub);
    assert_eq!(ua[0]["id"], "bot-1");
    assert_eq!(call(&mut a, "pendingEvents", vec![]), json!(10));
    assert_eq!(
        call(&mut a, "drainEvents", vec![])
            .as_array()
            .unwrap()
            .len(),
        10
    );
}

#[test]
fn mirror_advertise_status_removal_and_stats_methods_work() {
    let mut e = Engine::new(5);
    call(
        &mut e,
        "upsertServer",
        vec![json!({"id":"s","capacity":2,"requirements":{"level":{"gte":2}}})],
    );
    call(
        &mut e,
        "upsertUsers",
        vec![json!([
            {"id":"a","scopes":{"level":3}},
            {"id":"b","scopes":{"level":1}},
            {"id":"c","scopes":{"level":5}},
            {"id":"d","scopes":{"level":9},"status":"idle"}
        ])],
    );
    let matches = call(&mut e, "findUsersForServer", vec![json!("s")]);
    assert_eq!(
        matches
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["a", "c"]
    );
    assert_eq!(call(&mut e, "fits", vec![json!("b"), json!("s")]), false);
    assert_eq!(call(&mut e, "advertiseAll", vec![]), 2);
    assert_eq!(
        call(&mut e, "setUserStatus", vec![json!("d"), json!("waiting")]),
        true
    );
    assert_eq!(
        call(&mut e, "setServerStatus", vec![json!("s"), json!("closed")]),
        true
    );
    assert_eq!(
        call(&mut e, "findUsersForServer", vec![json!("s")]),
        json!([])
    );
    assert_eq!(
        call(&mut e, "setServerStatus", vec![json!("s"), json!("open")]),
        true
    );
    assert_eq!(
        call(&mut e, "join", vec![json!("a"), json!("s")])["ok"],
        true
    );
    assert_eq!(call(&mut e, "removeServer", vec![json!("s")]), true);
    assert_eq!(
        call(&mut e, "getUser", vec![json!("a")])["status"],
        "waiting"
    );
    let stats = call(&mut e, "stats", vec![]);
    assert_eq!(stats["users"], 4);
    assert_eq!(stats["servers"], 0);
    assert_eq!(call(&mut e, "clear", vec![]), Value::Null);
    assert_eq!(call(&mut e, "stats", vec![])["users"], 0);
}

#[test]
fn requirement_coercion_and_missing_key_rules_match_core_semantics() {
    assert!(liveengine::rules::evaluate(
        &json!({"rules":[{"key":"tags","op":"contains","value":1}]}),
        &json!({"tags":["1"]})
    ));
    let mut e = Engine::new(8);
    call(
        &mut e,
        "upsertServer",
        vec![json!({"id":"s","scopes":{"tags":["1"],"v":1}})],
    );
    call(
        &mut e,
        "upsertUsers",
        vec![json!([
            {"id":"tag","scopes":{"tags":["1"]},"requirements":{"tags":{"contains":1}}},
            {"id":"numeric","scopes":{"v":1},"requirements":{"v":{"in":[true]}}},
            {"id":"bool","scopes":{"v":true},"requirements":{"v":{"between":[1,1]}}},
            {"id":"missing-ne","requirements":{"absent":{"ne":"x"}}},
            {"id":"missing-nin","requirements":{"absent":{"nin":["x"]}}},
            {"id":"missing-not-contains","requirements":{"absent":{"notContains":"x"}}},
            {"id":"empty-any","requirements":{"rules":[],"match":"any"}}
        ])],
    );
    for id in [
        "tag",
        "numeric",
        "bool",
        "missing-ne",
        "missing-nin",
        "missing-not-contains",
        "empty-any",
    ] {
        assert_eq!(
            call(&mut e, "fits", vec![json!(id), json!("s")]),
            true,
            "{id}"
        );
    }
}

#[test]
fn generator_validation_is_error_safe_and_does_not_partially_mutate() {
    let mut e = Engine::new(12);
    for bad_spec in [
        json!({"scopes":{"x":{"type":"int","min":10,"max":1}}}),
        json!({"scopes":{"x":{"type":"float","min":2,"max":1}}}),
        json!({"scopes":{"x":{"type":"choice","values":[]}}}),
        json!({"scopes":{"x":{"type":"choice","values":["a","b"],"weights":[1,-1]}}}),
        json!({"scopes":{"x":{"type":"bool","p":2}}}),
        json!({"scopes":{"x":{"type":"int","min":"bad"}}}),
        json!({"scopes":{"first":{"type":"string"},"later":{"type":"wat"}}}),
    ] {
        assert!(e
            .dispatch("generateUsers", vec![json!(3), bad_spec])
            .is_err());
        assert_eq!(call(&mut e, "stats", vec![])["users"], 0);
        assert_eq!(call(&mut e, "pendingEvents", vec![]), 0);
    }
    assert_eq!(
        call(&mut e, "generateUsers", vec![json!(-100), json!({})]),
        json!([])
    );
    assert!(e
        .dispatch("generateUsers", vec![json!(100_001), json!({})])
        .is_err());
    assert_eq!(call(&mut e, "stats", vec![])["users"], 0);
    let first = call(&mut e, "generateUsers", vec![json!(1), json!({})]);
    assert_eq!(first[0]["id"], "user-1");
}

#[test]
fn upserting_paired_users_preserves_pairing_and_status_invariants() {
    let mut e = Engine::new(13);
    call(&mut e, "upsertServer", vec![json!({"id":"s","capacity":2})]);
    call(
        &mut e,
        "upsertUser",
        vec![json!({"id":"u","status":"paired"})],
    );
    assert_eq!(
        call(&mut e, "getUser", vec![json!("u")])["status"],
        "waiting"
    );
    assert_eq!(
        call(&mut e, "join", vec![json!("u"), json!("s")])["ok"],
        true
    );
    call(
        &mut e,
        "upsertUser",
        vec![json!({"id":"u","status":"idle","scopes":{"new":true}})],
    );
    let paired = call(&mut e, "getUser", vec![json!("u")]);
    assert_eq!(paired["status"], "paired");
    assert_eq!(paired["serverId"], "s");
    assert_eq!(paired["scopes"]["new"], true);
    assert_eq!(
        call(&mut e, "getServer", vec![json!("s")])["members"],
        json!(["u"])
    );
}

#[test]
fn generated_equal_float_ranges_and_empty_tag_pools_are_safe() {
    let mut e = Engine::new(14);
    let users = call(
        &mut e,
        "generateUsers",
        vec![
            json!(2),
            json!({"scopes":{
                "fixed":{"type":"float","min":2.5,"max":2.5},
                "tags":{"type":"tags","values":[],"count":u64::MAX}
            }}),
        ],
    );
    assert_eq!(users[0]["scopes"]["fixed"], 2.5);
    assert_eq!(users[0]["scopes"]["tags"], json!([]));
}

#[test]
fn first_strategy_respects_server_arrival_order_not_id_order() {
    let mut e = Engine::new(15);
    call(
        &mut e,
        "upsertServers",
        vec![json!([
            {"id":"z-first"},
            {"id":"a-second"}
        ])],
    );
    call(&mut e, "upsertUser", vec![json!({"id":"u"})]);
    let matches = call(
        &mut e,
        "findServersForUser",
        vec![json!("u"), json!({"limit":1})],
    );
    assert_eq!(matches[0]["id"], "z-first");

    call(
        &mut e,
        "upsertServer",
        vec![json!({"id":"full-defaults","status":"full","capacity":0})],
    );
    let s = call(&mut e, "getServer", vec![json!("full-defaults")]);
    assert_eq!(s["status"], "open");
    assert_eq!(s["capacity"], 1);
    assert_eq!(s["minMembers"], 0);
    assert_eq!(s["free"], 1);
}

#[test]
fn pair_peers_is_atomic_and_system_rooms_are_not_server_matches() {
    let mut e = Engine::new(42);
    call(&mut e, "upsertUser", vec![json!({"id":"taken"})]);
    call(&mut e, "upsertServer", vec![json!({"id":"real"})]);
    call(&mut e, "join", vec![json!("taken"), json!("real")]);
    let before = call(&mut e, "stats", vec![]);
    let events = call(&mut e, "pendingEvents", vec![]);
    assert!(e.dispatch("pairPeers", vec![json!({"room":"failed","capacity":2,"users":[{"id":"new","scopes":{}},{"id":"taken","scopes":{}}]})]).is_err());
    assert_eq!(call(&mut e, "stats", vec![]), before);
    assert_eq!(call(&mut e, "pendingEvents", vec![]), events);
    assert_eq!(call(&mut e, "getUser", vec![json!("new")]), Value::Null);
    assert_eq!(
        call(&mut e, "getServer", vec![json!("failed")]),
        Value::Null
    );
    call(
        &mut e,
        "pairPeers",
        vec![
            json!({"room":"room","capacity":3,"users":[{"id":"a","scopes":{}},{"id":"b","scopes":{}}]}),
        ],
    );
    call(&mut e, "upsertUser", vec![json!({"id":"other"})]);
    assert_eq!(
        call(&mut e, "findServersForUser", vec![json!("other")]),
        json!([])
    );
    assert_eq!(
        call(&mut e, "findUsersForServer", vec![json!("room")]),
        json!([])
    );
    assert_eq!(
        call(&mut e, "join", vec![json!("other"), json!("room")])["ok"],
        false
    );
    call(&mut e, "leave", vec![json!("a")]);
    assert!(!call(&mut e, "getServer", vec![json!("room")]).is_null());
    call(&mut e, "leave", vec![json!("b")]);
    assert!(call(&mut e, "getServer", vec![json!("room")]).is_null());
}

#[test]
fn pair_peers_capacity_is_atomic_and_uses_existing_room_limit() {
    let mut e = Engine::new(0);
    call(
        &mut e,
        "pairPeers",
        vec![
            json!({"room":"r","capacity":2,"users":[{"id":"a","scopes":{}},{"id":"b","scopes":{}}]}),
        ],
    );
    assert!(e.dispatch("pairPeers", vec![json!({"room":"r","capacity":64,"users":[{"id":"a","scopes":{}},{"id":"c","scopes":{}}]})]).is_err());
    assert_eq!(
        call(&mut e, "getServer", vec![json!("r")])["members"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(call(&mut e, "getUser", vec![json!("c")]), Value::Null);
}
