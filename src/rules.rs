use serde_json::{Map, Value};

pub fn parse_requirements(value: Option<&Value>) -> Result<Value, String> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(serde_json::json!({"rules": [], "match": "all"}));
    };
    let mut rules = Vec::new();
    let mut mode = "all";
    if let Some(arr) = value.as_array() {
        for rule in arr {
            rules.push(parse_rule(rule)?);
        }
    } else if let Some(obj) = value.as_object() {
        if let Some(arr) = obj.get("rules").and_then(Value::as_array) {
            for rule in arr {
                rules.push(parse_rule(rule)?);
            }
            if obj.get("match").and_then(Value::as_str) == Some("any") {
                mode = "any";
            }
        } else {
            for (key, val) in obj {
                if let Some(ops) = val.as_object() {
                    for (op, v) in ops {
                        rules.push(make_rule(key, op, v.clone())?);
                    }
                } else if val.is_array() {
                    rules.push(make_rule(key, "in", val.clone())?);
                } else {
                    rules.push(make_rule(key, "eq", val.clone())?);
                }
            }
        }
    } else {
        return Err("requirements must be an array or object".into());
    }
    Ok(serde_json::json!({"rules": rules, "match": mode}))
}

fn parse_rule(rule: &Value) -> Result<Value, String> {
    let obj = rule
        .as_object()
        .ok_or("requirement rule must be an object")?;
    let key = obj
        .get("key")
        .and_then(Value::as_str)
        .unwrap_or("undefined");
    let op = obj.get("op").and_then(Value::as_str).unwrap_or("eq");
    make_rule(key, op, obj.get("value").cloned().unwrap_or(Value::Null))
}

fn make_rule(key: &str, op: &str, value: Value) -> Result<Value, String> {
    let canonical = match op {
        "eq" | "==" => "eq",
        "ne" | "!=" => "ne",
        "lt" | "<" => "lt",
        "lte" | "<=" => "lte",
        "gt" | ">" => "gt",
        "gte" | ">=" => "gte",
        "in" => "in",
        "nin" => "nin",
        "between" => "between",
        "exists" => "exists",
        "missing" => "missing",
        "contains" => "contains",
        "notContains" => "notContains",
        "overlaps" => "overlaps",
        _ => return Err(format!("unknown requirement op: {op}")),
    };
    let list = matches!(canonical, "in" | "nin" | "between" | "overlaps");
    if list && !value.is_array() {
        return Err(format!("op '{op}' on '{key}' needs an array value"));
    }
    Ok(serde_json::json!({"key":key,"op":canonical,"value":value}))
}

pub fn evaluate(reqs: &Value, scopes: &Value) -> bool {
    let Some(rules) = reqs.get("rules").and_then(Value::as_array) else {
        return true;
    };
    if rules.is_empty() {
        return true;
    }
    let all = reqs.get("match").and_then(Value::as_str) != Some("any");
    if all {
        rules.iter().all(|r| eval_rule(r, scopes))
    } else {
        rules.iter().any(|r| eval_rule(r, scopes))
    }
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_bool().map(|b| if b { 1.0 } else { 0.0 }))
}
fn eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (num(a), num(b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}
fn has(v: &Value, needle: &Value) -> bool {
    if let Some(a) = v.as_array() {
        let needle = if let Some(s) = needle.as_str() {
            Some(s.to_owned())
        } else {
            num(needle).map(canonical_number)
        };
        needle.is_some_and(|needle| a.iter().any(|x| x.as_str() == Some(needle.as_str())))
    } else {
        eq(v, needle)
    }
}

fn canonical_number(n: f64) -> String {
    if n == 0.0 {
        return "0".into();
    }
    let mut s = format!("{n:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    s
}
fn eval_rule(r: &Value, scopes: &Value) -> bool {
    let key = r.get("key").and_then(Value::as_str).unwrap_or("");
    let op = r.get("op").and_then(Value::as_str).unwrap_or("eq");
    let want = r.get("value").unwrap_or(&Value::Null);
    let v = scopes.get(key).filter(|v| !v.is_null());
    if op == "exists" {
        return v.is_some();
    }
    if op == "missing" {
        return v.is_none();
    }
    let Some(v) = v else {
        return matches!(op, "ne" | "nin" | "notContains");
    };
    match op {
        "eq" => eq(v, want),
        "ne" => !eq(v, want),
        "lt" | "lte" | "gt" | "gte" => {
            if let (Some(a), Some(b)) = (num(v), num(want)) {
                match op {
                    "lt" => a < b,
                    "lte" => a <= b,
                    "gt" => a > b,
                    _ => a >= b,
                }
            } else if let (Some(a), Some(b)) = (v.as_str(), want.as_str()) {
                match op {
                    "lt" => a < b,
                    "lte" => a <= b,
                    "gt" => a > b,
                    _ => a >= b,
                }
            } else {
                false
            }
        }
        "in" | "nin" => {
            let hit = want
                .as_array()
                .map(|a| a.iter().any(|x| eq(v, x)))
                .unwrap_or(false);
            if op == "in" {
                hit
            } else {
                !hit
            }
        }
        "between" => want.as_array().filter(|a| a.len() >= 2).is_some_and(|a| {
            match (num(v), num(&a[0]), num(&a[1])) {
                (Some(n), Some(lo), Some(hi)) => n >= lo && n <= hi,
                _ => false,
            }
        }),
        "contains" => has(v, want),
        "notContains" => !has(v, want),
        "overlaps" => want
            .as_array()
            .map(|a| a.iter().any(|x| has(v, x)))
            .unwrap_or(false),
        _ => false,
    }
}

pub fn scopes(value: Option<&Value>) -> Value {
    let mut out = Map::new();
    if let Some(obj) = value.and_then(Value::as_object) {
        for (k, v) in obj {
            out.insert(
                k.clone(),
                if v.is_array() {
                    Value::Array(
                        v.as_array()
                            .unwrap()
                            .iter()
                            .map(|x| Value::String(js_string(x)))
                            .collect(),
                    )
                } else {
                    v.clone()
                },
            );
        }
    }
    Value::Object(out)
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n
            .as_f64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| n.to_string()),
        Value::String(s) => s.clone(),
        Value::Array(values) => values
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    js_string(v)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}
