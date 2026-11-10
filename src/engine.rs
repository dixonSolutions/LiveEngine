use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

use crate::rules;

#[derive(Clone)]
struct User {
    id: String,
    scopes: Value,
    req: Value,
    status: String,
    server: String,
    seq: u64,
}
#[derive(Clone)]
struct Server {
    id: String,
    scopes: Value,
    req: Value,
    status: String,
    capacity: u32,
    min: u32,
    members: Vec<String>,
    seq: u64,
    system: bool,
}
#[derive(Clone)]
struct Match {
    id: String,
    score: f64,
    load: usize,
}
type ParsedOptions = (usize, String, Vec<(String, f64)>);

fn generator_bound(value: Option<&Value>, default: f64, key: &str) -> Result<f64, String> {
    match value {
        None => Ok(default),
        Some(value) => value
            .as_f64()
            .ok_or_else(|| format!("generator '{key}' bounds must be numbers")),
    }
}

#[derive(Clone)]
pub struct Engine {
    users: BTreeMap<String, User>,
    servers: BTreeMap<String, Server>,
    server_eq: HashMap<String, Vec<(u64, String)>>,
    availability: BTreeMap<String, bool>,
    events: Vec<Value>,
    seq: u64,
    event_seq: u64,
    gen_counter: u64,
    joins: u64,
    leaves: u64,
    evaluated: u64,
    rng: ChaCha8Rng,
}

impl Engine {
    pub fn new(seed: u64) -> Self {
        Self {
            users: BTreeMap::new(),
            servers: BTreeMap::new(),
            server_eq: HashMap::new(),
            availability: BTreeMap::new(),
            events: vec![],
            seq: 0,
            event_seq: 0,
            gen_counter: 0,
            joins: 0,
            leaves: 0,
            evaluated: 0,
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }
    fn emit(&mut self, mut e: Value) {
        self.event_seq += 1;
        e["seq"] = json!(self.event_seq);
        self.events.push(e);
    }
    fn id_arg(args: &[Value], i: usize, what: &str) -> Result<String, String> {
        args.get(i)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(format!("{what} must be a string"))
    }
    fn req(v: Option<&Value>) -> Result<Value, String> {
        rules::parse_requirements(v)
    }
    fn make_user(&mut self, v: &Value) -> Result<User, String> {
        if !v.is_object() {
            return Err("user must be an object".into());
        }
        let id = v
            .get("id")
            .ok_or("user.id is required")?
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| v["id"].to_string());
        let status = v
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("waiting")
            .to_string();
        if !["waiting", "paired", "idle"].contains(&status.as_str()) {
            return Err("unknown user status".into());
        }
        Ok(User {
            id,
            scopes: rules::scopes(v.get("scopes")),
            req: Self::req(v.get("requirements"))?,
            status,
            server: String::new(),
            seq: 0,
        })
    }
    fn make_server(&mut self, v: &Value) -> Result<Server, String> {
        if !v.is_object() {
            return Err("server must be an object".into());
        }
        let id = v
            .get("id")
            .ok_or("server.id is required")?
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| v["id"].to_string());
        let status = v
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_string();
        if !["open", "full", "closed"].contains(&status.as_str()) {
            return Err("unknown server status".into());
        }
        Ok(Server {
            id,
            scopes: rules::scopes(v.get("scopes")),
            req: Self::req(v.get("requirements"))?,
            status,
            capacity: v
                .get("capacity")
                .and_then(Value::as_u64)
                .unwrap_or(1)
                .max(1)
                .min(u32::MAX as u64) as u32,
            min: v
                .get("minMembers")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as u32,
            members: vec![],
            seq: 0,
            system: false,
        })
    }
    fn user_value(u: &User) -> Value {
        json!({"id":u.id,"scopes":u.scopes,"requirements":u.req,"status":u.status,"serverId":if u.server.is_empty(){Value::Null}else{json!(u.server)},"seq":u.seq})
    }
    fn server_value(s: &Server) -> Value {
        json!({"id":s.id,"scopes":s.scopes,"requirements":s.req,"status":s.status,"capacity":s.capacity,"minMembers":s.min,"members":s.members,"free":(s.capacity as usize).saturating_sub(s.members.len()),"seq":s.seq,"system":s.system})
    }
    fn result(ok: bool, reason: &str, user: &str, server: &str) -> Value {
        json!({"ok":ok,"reason":if reason.is_empty(){Value::Null}else{json!(reason)},"userId":if user.is_empty(){Value::Null}else{json!(user)},"serverId":if server.is_empty(){Value::Null}else{json!(server)}})
    }
    fn event(&mut self, kind: &str, user: &str, server: &str, extra: Option<(&str, Value)>) {
        let mut e = json!({"type":kind});
        if !user.is_empty() {
            e["userId"] = json!(user)
        }
        if !server.is_empty() {
            e["serverId"] = json!(server)
        }
        if let Some((k, v)) = extra {
            e[k] = v
        }
        self.emit(e)
    }
    fn available(&self, s: &Server) -> bool {
        s.status == "open" && s.members.len() < (s.capacity as usize)
    }
    fn scope_key(key: &str, value: &Value) -> String {
        let normalized = value
            .as_f64()
            .or_else(|| value.as_bool().map(|b| if b { 1.0 } else { 0.0 }));
        format!(
            "{}={}",
            key,
            normalized
                .map(|n| format!("n:{n}"))
                .unwrap_or_else(|| format!("v:{value}"))
        )
    }
    fn index_server(&mut self, s: &Server, add: bool) {
        for (key, value) in s.scopes.as_object().into_iter().flatten() {
            let values: Vec<&Value> = if let Some(tags) = value.as_array() {
                tags.iter().collect()
            } else {
                vec![value]
            };
            for item in values {
                let idx = self
                    .server_eq
                    .entry(Self::scope_key(key, item))
                    .or_default();
                if add {
                    if !idx.iter().any(|(_, id)| id == &s.id) {
                        idx.push((s.seq, s.id.clone()));
                        idx.sort_by_key(|x| x.0);
                    }
                } else {
                    idx.retain(|(_, id)| id != &s.id);
                }
            }
        }
    }
    fn fits_inner(&mut self, uid: &str, sid: &str) -> bool {
        let (Some(u), Some(s)) = (self.users.get(uid).cloned(), self.servers.get(sid).cloned())
        else {
            return false;
        };
        self.evaluated += 1;
        !s.system
            && self.available(&s)
            && rules::evaluate(&s.req, &u.scopes)
            && rules::evaluate(&u.req, &s.scopes)
    }
    fn options(v: Option<&Value>) -> Result<ParsedOptions, String> {
        let Some(v) = v.filter(|v| !v.is_null()) else {
            return Ok((10, "first".into(), vec![]));
        };
        if !v.is_object() {
            return Err("options must be an object".into());
        }
        let limit = v
            .get("limit")
            .and_then(Value::as_f64)
            .map(|x| {
                if x <= 0.0 {
                    0
                } else {
                    x.trunc().min(usize::MAX as f64) as usize
                }
            })
            .unwrap_or(10);
        let strategy = v
            .get("strategy")
            .and_then(Value::as_str)
            .unwrap_or("first")
            .to_string();
        if !["first", "random", "leastLoaded", "mostLoaded", "affinity"]
            .contains(&strategy.as_str())
        {
            return Err("unknown strategy".into());
        }
        let mut aff = vec![];
        if let Some(a) = v.get("affinity") {
            if let Some(arr) = a.as_array() {
                for el in arr {
                    if let Some(k) = el.as_str() {
                        aff.push((k.to_string(), 1.0))
                    } else if let Some(o) = el.as_object() {
                        if let Some(k) = o.get("key").and_then(Value::as_str) {
                            aff.push((
                                k.to_string(),
                                o.get("weight").and_then(Value::as_f64).unwrap_or(1.0),
                            ))
                        }
                    }
                }
            } else if let Some(o) = a.as_object() {
                for (k, w) in o {
                    aff.push((k.clone(), w.as_f64().unwrap_or(0.0)))
                }
            }
        }
        let strat = if !aff.is_empty() && v.get("strategy").is_none() {
            "affinity".into()
        } else {
            strategy
        };
        Ok((limit, strat, aff))
    }
    fn affinity(mine: &Value, theirs: &Value, aff: &[(String, f64)]) -> f64 {
        aff.iter()
            .map(|(k, w)| match (mine.get(k), theirs.get(k)) {
                (Some(a), Some(b)) => match (
                    a.as_f64()
                        .or_else(|| a.as_bool().map(|x| if x { 1. } else { 0. })),
                    b.as_f64()
                        .or_else(|| b.as_bool().map(|x| if x { 1. } else { 0. })),
                ) {
                    (Some(x), Some(y)) => w * (x - y).abs(),
                    _ => w * if a == b { 0. } else { 1e6 },
                },
                _ => w * 1e6,
            })
            .sum()
    }
    fn matches_for(&mut self, user_mode: bool, id: &str, opts: &ParsedOptions) -> Vec<Match> {
        let (limit, strategy, aff) = opts;
        let mut out = vec![];
        if user_mode {
            let Some(u) = self.users.get(id).cloned() else {
                return out;
            };
            let eq_rule = if u.req.get("match").and_then(Value::as_str) == Some("any") {
                None
            } else {
                u.req.get("rules").and_then(Value::as_array).and_then(|rs| {
                    rs.iter()
                        .find(|r| r.get("op").and_then(Value::as_str) == Some("eq"))
                })
            };
            // The equality index is already ordered by server arrival. Avoid cloning
            // and sorting it for every user when only the first match is needed.
            if *strategy == "first" && *limit == 1 {
                if let Some(r) = eq_rule {
                    let key = Self::scope_key(r["key"].as_str().unwrap_or(""), &r["value"]);
                    if let Some(ids) = self.server_eq.get(&key) {
                        for (_, sid) in ids {
                            let Some(s) = self.servers.get(sid) else {
                                continue;
                            };
                            if s.system || !self.available(s) {
                                continue;
                            }
                            self.evaluated += 1;
                            if rules::evaluate(&s.req, &u.scopes)
                                && rules::evaluate(&u.req, &s.scopes)
                            {
                                return vec![Match {
                                    id: sid.clone(),
                                    score: s.seq as f64,
                                    load: s.members.len(),
                                }];
                            }
                        }
                    }
                    return out;
                }
            }
            let mut ids: Vec<_> = if let Some(r) = eq_rule {
                let key = Self::scope_key(r["key"].as_str().unwrap_or(""), &r["value"]);
                self.server_eq.get(&key).cloned().unwrap_or_default()
            } else {
                self.servers
                    .values()
                    .map(|s| (s.seq, s.id.clone()))
                    .collect()
            };
            ids.sort_by_key(|x| x.0);
            ids.retain(|(_, sid)| {
                self.servers
                    .get(sid)
                    .map(|s| self.available(s))
                    .unwrap_or(false)
            });
            for (_, sid) in ids {
                if self.fits_inner(id, &sid) {
                    let s = &self.servers[&sid];
                    out.push(Match {
                        id: sid,
                        score: s.seq as f64,
                        load: s.members.len(),
                    });
                    if *strategy == "first" && *limit > 0 && out.len() >= *limit {
                        break;
                    }
                }
            }
            for m in &mut out {
                match strategy.as_str() {
                    "leastLoaded" => m.score = m.load as f64,
                    "mostLoaded" => m.score = -(m.load as f64),
                    "affinity" => {
                        m.score = Self::affinity(&u.scopes, &self.servers[&m.id].scopes, aff)
                    }
                    _ => {}
                }
            }
        } else {
            let Some(s) = self.servers.get(id).cloned() else {
                return out;
            };
            if s.system || !self.available(&s) {
                return out;
            }
            let mut ids: Vec<_> = self
                .users
                .values()
                .filter(|u| u.status == "waiting")
                .map(|u| (u.seq, u.id.clone()))
                .collect();
            ids.sort_by_key(|x| x.0);
            for (_, uid) in ids {
                if self.fits_inner(&uid, id) {
                    let score = self.users[&uid].seq as f64;
                    out.push(Match {
                        id: uid,
                        score,
                        load: 0,
                    });
                    if *strategy == "first" && *limit > 0 && out.len() >= *limit {
                        break;
                    }
                }
            }
            for m in &mut out {
                if *strategy == "affinity" {
                    m.score = Self::affinity(&s.scopes, &self.users[&m.id].scopes, aff)
                }
            }
        }
        if strategy == "random" {
            out.shuffle(&mut self.rng);
            for (i, m) in out.iter_mut().enumerate() {
                m.score = i as f64
            }
        } else if strategy != "first" {
            out.sort_by(|a, b| a.score.total_cmp(&b.score));
        }
        if *limit > 0 {
            out.truncate(*limit)
        }
        out
    }
    fn mvalues(ms: Vec<Match>) -> Value {
        Value::Array(
            ms.into_iter()
                .map(|m| json!({"id":m.id,"score":m.score,"load":m.load}))
                .collect(),
        )
    }
    fn refresh(&mut self, sid: &str) {
        let Some(mut s) = self.servers.get(sid).cloned() else {
            return;
        };
        if s.status == "full" && s.members.len() < s.capacity as usize {
            s.status = "open".into()
        }
        if s.status == "open" && s.members.len() >= s.capacity as usize {
            s.status = "full".into()
        }
        let after = s.status.clone();
        let now = after == "open" && s.members.len() < (s.capacity as usize);
        self.servers.insert(sid.into(), s);
        let was = self.availability.insert(sid.into(), now).unwrap_or(false);
        if was && !now {
            self.event(
                if after == "full" {
                    "server_full"
                } else {
                    "server_unavailable"
                },
                "",
                sid,
                None,
            )
        } else if !was && now {
            self.event("server_available", "", sid, None)
        }
    }
    fn join_inner(&mut self, uid: &str, sid: &str) -> Value {
        let Some(u) = self.users.get(uid).cloned() else {
            return Self::result(false, "user_not_found", uid, sid);
        };
        let Some(s) = self.servers.get(sid).cloned() else {
            return Self::result(false, "server_not_found", uid, sid);
        };
        if u.status == "paired" {
            return Self::result(false, "already_paired", uid, &u.server);
        }
        if !self.available(&s) {
            return Self::result(
                false,
                if s.status == "closed" {
                    "closed"
                } else {
                    "full"
                },
                uid,
                sid,
            );
        }
        if !s.system && !self.fits_inner(uid, sid) {
            return Self::result(false, "no_fit", uid, sid);
        }
        let s = self.servers.get_mut(sid).unwrap();
        s.members.push(uid.into());
        let members = s.members.clone();
        let n = s.members.len();
        let min = s.min;
        self.users.get_mut(uid).unwrap().status = "paired".into();
        self.users.get_mut(uid).unwrap().server = sid.into();
        self.joins += 1;
        self.event("paired", uid, sid, None);
        self.refresh(sid);
        if min > 0 && n == min as usize {
            self.event("server_ready", "", sid, Some(("ids", json!(members))))
        }
        Self::result(true, "", uid, sid)
    }
    fn leave_inner(&mut self, uid: &str, reason: &str) -> Value {
        let Some(u) = self.users.get(uid).cloned() else {
            return Self::result(false, "user_not_found", uid, "");
        };
        if u.status != "paired" {
            return Self::result(false, "not_paired", uid, "");
        }
        let sid = u.server.clone();
        if let Some(s) = self.servers.get_mut(&sid) {
            s.members.retain(|x| x != uid);
            self.leaves += 1;
            self.event("unpaired", uid, &sid, Some(("reason", json!(reason))));
            self.refresh(&sid)
        }
        let u = self.users.get_mut(uid).unwrap();
        u.status = "waiting".into();
        u.server.clear();
        if self
            .servers
            .get(&sid)
            .is_some_and(|s| s.system && s.members.is_empty())
        {
            self.servers.remove(&sid);
            self.availability.remove(&sid);
            self.event("server_removed", "", &sid, None);
        }
        Self::result(true, "", uid, &sid)
    }

    pub fn dispatch(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String> {
        let a = &args;
        let x = |i| a.get(i);
        Ok(match method {
            "pairPeers" => {
                let spec = x(0)
                    .and_then(Value::as_object)
                    .ok_or("pairPeers requires an object")?;
                let room = spec
                    .get("room")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or("room must be a nonempty string")?;
                let capacity = spec
                    .get("capacity")
                    .and_then(Value::as_u64)
                    .filter(|n| *n > 0 && *n <= u32::MAX as u64)
                    .ok_or("invalid capacity")? as u32;
                let users = spec
                    .get("users")
                    .and_then(Value::as_array)
                    .filter(|u| !u.is_empty())
                    .ok_or("users must be a nonempty array")?;
                let mut ids = std::collections::HashSet::new();
                for user in users {
                    let id = user
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or("user id must be a nonempty string")?;
                    if !user.get("scopes").is_some_and(Value::is_object) || !ids.insert(id) {
                        return Err("invalid or duplicate peer".into());
                    }
                    if self
                        .users
                        .get(id)
                        .is_some_and(|u| u.status == "paired" && u.server != room)
                    {
                        return Err("peer already paired".into());
                    }
                }
                if self.servers.get(room).is_some_and(|s| !s.system) {
                    return Err("room already exists".into());
                }
                if let Some(server) = self.servers.get(room) {
                    let needed = users
                        .iter()
                        .filter(|user| {
                            let id = user["id"].as_str().unwrap();
                            !server.members.iter().any(|member| member == id)
                        })
                        .count();
                    if server.members.len().saturating_add(needed) > server.capacity as usize {
                        return Err("room at capacity".into());
                    }
                } else if users.len() > capacity as usize {
                    return Err("room at capacity".into());
                }
                let mut next = self.clone();
                if !next.servers.contains_key(room) {
                    let mut server = next.make_server(&json!({"id":room,"capacity":capacity}))?;
                    server.system = true;
                    next.seq += 1;
                    server.seq = next.seq;
                    next.servers.insert(room.into(), server);
                    next.event("server_added", "", room, None);
                    next.refresh(room);
                }
                for user in users {
                    let id = user["id"].as_str().unwrap();
                    if next
                        .users
                        .get(id)
                        .is_some_and(|u| u.server == room && u.status == "paired")
                    {
                        continue;
                    }
                    if !next.users.contains_key(id) {
                        next.dispatch("upsertUser", vec![user.clone()])?;
                    } else {
                        next.users.get_mut(id).unwrap().scopes = user["scopes"].clone();
                        next.event("user_updated", id, "", None);
                    }
                    if next.join_inner(id, room)["ok"] != true {
                        return Err("peer could not join room".into());
                    }
                }
                *self = next;
                json!({"ok":true,"serverId":room})
            }
            "upsertUser" => {
                let mut u = self.make_user(x(0).unwrap_or(&Value::Null))?;
                if let Some(old) = self.users.get(&u.id) {
                    u.seq = old.seq;
                    u.server = old.server.clone();
                    if old.status == "paired" || u.status == "paired" {
                        u.status = old.status.clone()
                    }
                    let id = u.id.clone();
                    let server = u.server.clone();
                    self.users.insert(id.clone(), u);
                    self.event("user_updated", &id, &server, None);
                    json!(false)
                } else {
                    if u.status == "paired" {
                        u.status = "waiting".into();
                    }
                    self.seq += 1;
                    u.seq = self.seq;
                    let id = u.id.clone();
                    self.users.insert(id.clone(), u);
                    self.event("user_added", &id, "", None);
                    json!(true)
                }
            }
            "upsertUsers" => {
                let arr = x(0)
                    .and_then(Value::as_array)
                    .ok_or("expected an array of users")?;
                let mut n = 0;
                for v in arr {
                    if self.dispatch("upsertUser", vec![v.clone()])?.as_bool() == Some(true) {
                        n += 1
                    }
                }
                json!(n)
            }
            "removeUser" => {
                let id = Self::id_arg(a, 0, "id")?;
                if let Some(u) = self.users.get(&id).cloned() {
                    if u.status == "paired" {
                        self.leave_inner(&id, "user_removed");
                    }
                    self.users.remove(&id);
                    self.event("user_removed", &id, "", None);
                    json!(true)
                } else {
                    json!(false)
                }
            }
            "getUser" => {
                let id = Self::id_arg(a, 0, "id")?;
                self.users
                    .get(&id)
                    .map(Self::user_value)
                    .unwrap_or(Value::Null)
            }
            "listUsers" => {
                let st = x(0).and_then(Value::as_str);
                if let Some(s) = st {
                    if !["waiting", "paired", "idle"].contains(&s) {
                        return Err("unknown user status".into());
                    }
                }
                let mut us: Vec<_> = self
                    .users
                    .values()
                    .filter(|u| st.map(|s| s == u.status).unwrap_or(true))
                    .cloned()
                    .collect();
                us.sort_by_key(|u| u.seq);
                json!(us.iter().map(Self::user_value).collect::<Vec<_>>())
            }
            "setUserStatus" => {
                let id = Self::id_arg(a, 0, "id")?;
                let st = Self::id_arg(a, 1, "status")?;
                if !["waiting", "paired", "idle"].contains(&st.as_str()) {
                    return Err("unknown user status".into());
                }
                if !self.users.contains_key(&id) {
                    json!(false)
                } else if st == "paired" {
                    json!(self.users[&id].status == "paired")
                } else {
                    if self.users[&id].status == "paired" {
                        self.leave_inner(&id, "status_change");
                    }
                    self.users.get_mut(&id).unwrap().status = st;
                    self.event("user_updated", &id, "", None);
                    json!(true)
                }
            }
            "upsertServer" => {
                let mut s = self.make_server(x(0).unwrap_or(&Value::Null))?;
                if let Some(old) = self.servers.get(&s.id).cloned() {
                    if old.system {
                        return Err("cannot overwrite a system room".into());
                    }
                    self.index_server(&old, false);
                    s.seq = old.seq;
                    s.members = old.members.clone();
                    if s.status == "full" {
                        s.status = old.status.clone()
                    }
                    let id = s.id.clone();
                    self.servers.insert(id.clone(), s);
                    self.index_server(&self.servers[&id].clone(), true);
                    self.event("server_updated", "", &id, None);
                    self.refresh(&id);
                    json!(false)
                } else {
                    if s.status == "full" {
                        s.status = "open".into()
                    }
                    self.seq += 1;
                    s.seq = self.seq;
                    let id = s.id.clone();
                    self.servers.insert(id.clone(), s);
                    self.index_server(&self.servers[&id].clone(), true);
                    self.event("server_added", "", &id, None);
                    self.refresh(&id);
                    json!(true)
                }
            }
            "upsertServers" => {
                let arr = x(0)
                    .and_then(Value::as_array)
                    .ok_or("expected an array of servers")?;
                let mut n = 0;
                for v in arr {
                    if self.dispatch("upsertServer", vec![v.clone()])?.as_bool() == Some(true) {
                        n += 1
                    }
                }
                json!(n)
            }
            "removeServer" => {
                let id = Self::id_arg(a, 0, "id")?;
                if let Some(s) = self.servers.get(&id).cloned() {
                    for uid in &s.members {
                        self.leave_inner(uid, "server_removed");
                    }
                    if self.servers.remove(&id).is_some() {
                        self.index_server(&s, false);
                        self.availability.remove(&id);
                        self.event("server_removed", "", &id, None);
                    }
                    json!(true)
                } else {
                    json!(false)
                }
            }
            "getServer" => {
                let id = Self::id_arg(a, 0, "id")?;
                self.servers
                    .get(&id)
                    .map(Self::server_value)
                    .unwrap_or(Value::Null)
            }
            "listServers" => {
                let st = x(0).and_then(Value::as_str);
                if let Some(s) = st {
                    if !["open", "full", "closed"].contains(&s) {
                        return Err("unknown server status".into());
                    }
                }
                let mut ss: Vec<_> = self
                    .servers
                    .values()
                    .filter(|s| st.map(|x| x == s.status).unwrap_or(true))
                    .cloned()
                    .collect();
                ss.sort_by_key(|s| s.seq);
                json!(ss.iter().map(Self::server_value).collect::<Vec<_>>())
            }
            "setServerStatus" => {
                let id = Self::id_arg(a, 0, "id")?;
                let st = Self::id_arg(a, 1, "status")?;
                if !["open", "full", "closed"].contains(&st.as_str()) {
                    return Err("unknown server status".into());
                }
                if !self.servers.contains_key(&id)
                    || (st == "full"
                        && self.servers[&id].members.len() < self.servers[&id].capacity as usize)
                {
                    json!(false)
                } else {
                    self.servers.get_mut(&id).unwrap().status = st;
                    self.event("server_updated", "", &id, None);
                    self.refresh(&id);
                    json!(true)
                }
            }
            "findServersForUser" => {
                let id = Self::id_arg(a, 0, "userId")?;
                let opt = Self::options(x(1))?;
                Self::mvalues(self.matches_for(true, &id, &opt))
            }
            "findUsersForServer" => {
                let id = Self::id_arg(a, 0, "serverId")?;
                let opt = Self::options(x(1))?;
                Self::mvalues(self.matches_for(false, &id, &opt))
            }
            "fits" => {
                let u = Self::id_arg(a, 0, "userId")?;
                let s = Self::id_arg(a, 1, "serverId")?;
                json!(self.fits_inner(&u, &s))
            }
            "advertise" => {
                let id = Self::id_arg(a, 0, "userId")?;
                let opt = Self::options(x(1))?;
                let ms = self.matches_for(true, &id, &opt);
                let n = ms.len();
                self.event(
                    "advertised",
                    &id,
                    "",
                    Some(("ids", json!(ms.iter().map(|m| &m.id).collect::<Vec<_>>()))),
                );
                json!(n)
            }
            "advertiseAll" => {
                let opt = Self::options(x(0))?;
                let mut us: Vec<_> = self
                    .users
                    .values()
                    .filter(|u| u.status == "waiting")
                    .map(|u| (u.seq, u.id.clone()))
                    .collect();
                us.sort_by_key(|x| x.0);
                let mut n = 0;
                for (_, id) in us {
                    let ms = self.matches_for(true, &id, &opt);
                    if !ms.is_empty() {
                        n += ms.len();
                        self.event(
                            "advertised",
                            &id,
                            "",
                            Some(("ids", json!(ms.iter().map(|m| &m.id).collect::<Vec<_>>()))),
                        );
                    }
                }
                json!(n)
            }
            "join" => {
                let u = Self::id_arg(a, 0, "userId")?;
                let s = Self::id_arg(a, 1, "serverId")?;
                if self.servers.get(&s).is_some_and(|s| s.system) {
                    Self::result(false, "no_fit", &u, &s)
                } else {
                    self.join_inner(&u, &s)
                }
            }
            "leave" => {
                let u = Self::id_arg(a, 0, "userId")?;
                self.leave_inner(&u, "left")
            }
            "autoJoin" => {
                let u = Self::id_arg(a, 0, "userId")?;
                let opt = Self::options(x(1))?;
                if !self.users.contains_key(&u) {
                    Self::result(false, "user_not_found", &u, "")
                } else if self.users[&u].status == "paired" {
                    Self::result(false, "already_paired", &u, &self.users[&u].server)
                } else {
                    let mut opt = opt;
                    opt.0 = 1;
                    let ms = self.matches_for(true, &u, &opt);
                    if ms.is_empty() {
                        Self::result(false, "no_fit", &u, "")
                    } else {
                        self.join_inner(&u, &ms[0].id)
                    }
                }
            }
            "matchAll" => {
                let mut opt = Self::options(x(0))?;
                opt.0 = 1;
                let mut us: Vec<_> = self
                    .users
                    .values()
                    .filter(|u| u.status == "waiting")
                    .map(|u| (u.seq, u.id.clone()))
                    .collect();
                us.sort_by_key(|x| x.0);
                let mut res = vec![];
                for (_, uid) in us {
                    let ms = self.matches_for(true, &uid, &opt);
                    if let Some(m) = ms.first() {
                        res.push(self.join_inner(&uid, &m.id));
                    }
                }
                json!(res)
            }
            "generateUsers" => {
                let count = x(0)
                    .and_then(Value::as_f64)
                    .ok_or("count must be a number")?;
                let count = if count < 0.0 {
                    0
                } else if !count.is_finite() || count > 100_000.0 {
                    return Err("count exceeds the maximum of 100000".into());
                } else {
                    count.trunc() as usize
                };
                let spec = x(1).cloned().unwrap_or(json!({}));
                self.generate(count, &spec)?
            }
            "drainEvents" => {
                let es = std::mem::take(&mut self.events);
                json!(es)
            }
            "pendingEvents" => json!(self.events.len()),
            "stats" => {
                let mut st = json!({"users":self.users.len(),"waiting":0,"paired":0,"idle":0,"servers":self.servers.len(),"open":0,"full":0,"closed":0,"pendingEvents":self.events.len(),"totalJoins":self.joins,"totalLeaves":self.leaves,"totalMatchesEvaluated":self.evaluated});
                for u in self.users.values() {
                    let k = match u.status.as_str() {
                        "waiting" => "waiting",
                        "paired" => "paired",
                        _ => "idle",
                    };
                    st[k] = json!(st[k].as_u64().unwrap_or(0) + 1)
                }
                for s in self.servers.values() {
                    let k = match s.status.as_str() {
                        "open" => "open",
                        "full" => "full",
                        _ => "closed",
                    };
                    st[k] = json!(st[k].as_u64().unwrap_or(0) + 1)
                }
                st
            }
            "clear" => {
                self.users.clear();
                self.servers.clear();
                self.server_eq.clear();
                self.availability.clear();
                self.events.clear();
                self.event("cleared", "", "", None);
                Value::Null
            }
            "reseed" => {
                let seed = x(0)
                    .and_then(Value::as_u64)
                    .ok_or("seed must be a number")?;
                self.rng = ChaCha8Rng::seed_from_u64(seed);
                Value::Null
            }
            _ => return Err(format!("unknown method: {method}")),
        })
    }

    fn generate(&mut self, count: usize, spec: &Value) -> Result<Value, String> {
        if !spec.is_null() && !spec.is_object() {
            return Err("generation spec must be an object".into());
        }
        let prefix = spec
            .get("idPrefix")
            .and_then(Value::as_str)
            .unwrap_or("user-");
        let req = Self::req(spec.get("requirements"))?;
        const MAX_GENERATED_BYTES: usize = 64 * 1024 * 1024;
        let per_user_budget = spec
            .to_string()
            .len()
            .saturating_add(prefix.len())
            .saturating_add(256);
        if count.saturating_mul(per_user_budget) > MAX_GENERATED_BYTES {
            return Err("generated users would exceed the 64 MiB output budget".into());
        }
        self.validate_generators(spec.get("scopes"))?;
        let mut made = vec![];
        for _ in 0..count {
            self.gen_counter += 1;
            let n = self.gen_counter;
            let mut scopes = Map::new();
            if let Some(gs) = spec.get("scopes").and_then(Value::as_object) {
                for (k, g) in gs {
                    let val = if let Some(a) = g.as_array() {
                        if a.is_empty() {
                            return Err(format!("generator for '{k}' has no choices"));
                        }
                        json!(a[self.rng.gen_range(0..a.len())]
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| a[0].to_string()))
                    } else if let Some(o) = g.as_object() {
                        let typ = o.get("type").and_then(Value::as_str).unwrap_or("int");
                        match typ {
                            "int" => {
                                let lo = o.get("min").and_then(Value::as_f64).unwrap_or(0.0).trunc()
                                    as i64;
                                let hi = o
                                    .get("max")
                                    .and_then(Value::as_f64)
                                    .unwrap_or(100.0)
                                    .trunc() as i64;
                                json!(self.rng.gen_range(lo..=hi))
                            }
                            "float" => {
                                let lo = o.get("min").and_then(Value::as_f64).unwrap_or(0.);
                                let hi = o.get("max").and_then(Value::as_f64).unwrap_or(100.);
                                json!(if lo == hi {
                                    lo
                                } else {
                                    self.rng.gen_range(lo..hi)
                                })
                            }
                            "bool" => {
                                let p = o
                                    .get("p")
                                    .or_else(|| o.get("probability"))
                                    .and_then(Value::as_f64)
                                    .unwrap_or(0.5);
                                json!(self.rng.gen_bool(p.clamp(0., 1.)))
                            }
                            "string" => json!(format!(
                                "{}{}",
                                o.get("prefix").and_then(Value::as_str).unwrap_or(""),
                                n
                            )),
                            "choice" | "tags" => {
                                let vals = o
                                    .get("values")
                                    .and_then(Value::as_array)
                                    .cloned()
                                    .unwrap_or_default();
                                if typ == "choice" {
                                    if vals.is_empty() {
                                        return Err(format!("generator for '{k}' has no choices"));
                                    }
                                    let i = self.weighted_idx(o.get("weights"), vals.len());
                                    json!(vals[i]
                                        .as_str()
                                        .map(str::to_owned)
                                        .unwrap_or_else(|| vals[i].to_string()))
                                } else {
                                    let mut vals: Vec<String> = vals
                                        .into_iter()
                                        .map(|v| {
                                            v.as_str()
                                                .map(str::to_owned)
                                                .unwrap_or_else(|| v.to_string())
                                        })
                                        .collect();
                                    vals.shuffle(&mut self.rng);
                                    vals.truncate(
                                        o.get("count")
                                            .and_then(Value::as_f64)
                                            .map(|n| {
                                                if n < 0.0 || n >= u32::MAX as f64 {
                                                    u32::MAX as usize
                                                } else {
                                                    n.trunc() as usize
                                                }
                                            })
                                            .unwrap_or(1),
                                    );
                                    json!(vals)
                                }
                            }
                            _ => return Err(format!("unknown generator type: {typ}")),
                        }
                    } else {
                        return Err(format!("generator for '{k}' must be an object or array"));
                    };
                    scopes.insert(k.clone(), val);
                }
            }
            let v = json!({"id":format!("{prefix}{n}"),"scopes":scopes,"requirements":req});
            let u = self.make_user(&v)?;
            self.dispatch("upsertUser", vec![Self::user_value(&u)])?;
            made.push(Self::user_value(self.users.get(&u.id).unwrap()));
        }
        Ok(json!(made))
    }

    fn validate_generators(&self, scopes: Option<&Value>) -> Result<(), String> {
        let Some(generators) = scopes.and_then(Value::as_object) else {
            return Ok(());
        };
        for (key, generator) in generators {
            let Some(object) = generator.as_object() else {
                if let Some(values) = generator.as_array() {
                    if values.is_empty() {
                        return Err(format!("generator for '{key}' has no choices"));
                    }
                    continue;
                }
                return Err(format!("generator for '{key}' must be an object or array"));
            };
            let kind = object.get("type").and_then(Value::as_str).unwrap_or("int");
            match kind {
                "int" => {
                    let lo = generator_bound(object.get("min"), 0.0, key)?.trunc();
                    let hi = generator_bound(object.get("max"), 100.0, key)?.trunc();
                    const I64_LIMIT: f64 = 9_223_372_036_854_775_808.0;
                    if lo < i64::MIN as f64 || hi >= I64_LIMIT || lo > hi {
                        return Err(format!("invalid integer range for generator '{key}'"));
                    }
                }
                "float" => {
                    let lo = generator_bound(object.get("min"), 0.0, key)?;
                    let hi = generator_bound(object.get("max"), 100.0, key)?;
                    if !lo.is_finite() || !hi.is_finite() || lo > hi {
                        return Err(format!("invalid float range for generator '{key}'"));
                    }
                }
                "bool" => {
                    let probability = generator_bound(
                        object.get("p").or_else(|| object.get("probability")),
                        0.5,
                        key,
                    )?;
                    if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
                        return Err(format!("invalid probability for generator '{key}'"));
                    }
                }
                "string" => {}
                "choice" | "tags" => {
                    let values = object.get("values").and_then(Value::as_array);
                    if kind == "choice" && values.is_none_or(Vec::is_empty) {
                        return Err(format!("generator for '{key}' has no choices"));
                    }
                    if let Some(weights) = object.get("weights").and_then(Value::as_array) {
                        if weights
                            .iter()
                            .any(|w| w.as_f64().is_none_or(|n| !n.is_finite() || n < 0.0))
                        {
                            return Err(format!("invalid choice weights for generator '{key}'"));
                        }
                        if values.is_some_and(|vs| weights.len() == vs.len())
                            && weights
                                .iter()
                                .filter_map(Value::as_f64)
                                .sum::<f64>()
                                .is_infinite()
                        {
                            return Err(format!("invalid choice weights for generator '{key}'"));
                        }
                    }
                }
                _ => return Err(format!("unknown generator type: {kind}")),
            }
        }
        Ok(())
    }
    fn weighted_idx(&mut self, weights: Option<&Value>, len: usize) -> usize {
        let Some(ws) = weights.and_then(Value::as_array).filter(|x| x.len() == len) else {
            return self.rng.gen_range(0..len);
        };
        let sum: f64 = ws.iter().map(|x| x.as_f64().unwrap_or(0.).max(0.)).sum();
        if sum <= 0. {
            return self.rng.gen_range(0..len);
        }
        let mut r = self.rng.gen_range(0.0..sum);
        for (i, w) in ws.iter().enumerate() {
            r -= w.as_f64().unwrap_or(0.).max(0.);
            if r < 0. {
                return i;
            }
        }
        len - 1
    }
}

/// Every method name [`Engine::dispatch`] accepts, in the order the typed methods below list them.
pub const DISPATCH_METHODS: &[&str] = &[
    "pairPeers",
    "upsertUser",
    "upsertUsers",
    "removeUser",
    "getUser",
    "listUsers",
    "setUserStatus",
    "upsertServer",
    "upsertServers",
    "removeServer",
    "getServer",
    "listServers",
    "setServerStatus",
    "findServersForUser",
    "findUsersForServer",
    "fits",
    "advertise",
    "advertiseAll",
    "join",
    "leave",
    "autoJoin",
    "matchAll",
    "generateUsers",
    "drainEvents",
    "pendingEvents",
    "stats",
    "clear",
    "reseed",
];

fn expect_bool(value: Value) -> Result<bool, String> {
    value.as_bool().ok_or_else(|| "expected a boolean".into())
}
fn expect_count(value: Value) -> Result<usize, String> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| "expected a count".into())
}
fn expect_list(value: Value) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(items) => Ok(items),
        _ => Err("expected an array".into()),
    }
}
fn optional(value: Value) -> Option<Value> {
    (!value.is_null()).then_some(value)
}
fn opt_arg(value: Option<Value>) -> Value {
    value.unwrap_or(Value::Null)
}

/// Typed methods, one per [`Engine::dispatch`] method and named like it in snake case. Each is a
/// thin wrapper over `dispatch` with the same validation, errors and events. Users, servers,
/// options and results keep their user-defined JSON shapes, so they are [`Value`]s, as in the
/// Node SDK's `LiveEngine` methods.
impl Engine {
    fn call(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String> {
        self.dispatch(method, args)
    }

    /// Seat `spec.users` (`[{id, scopes}]`) together in room `spec.room` with `spec.capacity`,
    /// all or nothing. Returns `{ok: true, serverId}`. Pairing in the hub uses this.
    pub fn pair_peers(&mut self, spec: Value) -> Result<Value, String> {
        self.call("pairPeers", vec![spec])
    }
    /// Add or replace a user (`{id, scopes?, requirements?, status?}`). True when it was added.
    pub fn upsert_user(&mut self, user: Value) -> Result<bool, String> {
        self.call("upsertUser", vec![user]).and_then(expect_bool)
    }
    /// Add or replace several users. Returns how many were added rather than replaced.
    pub fn upsert_users(&mut self, users: Vec<Value>) -> Result<usize, String> {
        self.call("upsertUsers", vec![Value::Array(users)])
            .and_then(expect_count)
    }
    /// Remove a user, leaving its room first. False when there was no such user.
    pub fn remove_user(&mut self, id: &str) -> Result<bool, String> {
        self.call("removeUser", vec![json!(id)])
            .and_then(expect_bool)
    }
    /// The user `id`, or `None`.
    pub fn get_user(&mut self, id: &str) -> Result<Option<Value>, String> {
        self.call("getUser", vec![json!(id)]).map(optional)
    }
    /// Users in insertion order, optionally only those with `status` (`waiting`, `paired`, `idle`).
    pub fn list_users(&mut self, status: Option<&str>) -> Result<Vec<Value>, String> {
        let args = status.map(|s| vec![json!(s)]).unwrap_or_default();
        self.call("listUsers", args).and_then(expect_list)
    }
    /// Set a user's status. Setting `paired` only reports whether it already is.
    pub fn set_user_status(&mut self, id: &str, status: &str) -> Result<bool, String> {
        self.call("setUserStatus", vec![json!(id), json!(status)])
            .and_then(expect_bool)
    }
    /// Add or replace a server (`{id, capacity?, minMembers?, scopes?, requirements?, status?}`).
    /// True when it was added.
    pub fn upsert_server(&mut self, server: Value) -> Result<bool, String> {
        self.call("upsertServer", vec![server])
            .and_then(expect_bool)
    }
    /// Add or replace several servers. Returns how many were added rather than replaced.
    pub fn upsert_servers(&mut self, servers: Vec<Value>) -> Result<usize, String> {
        self.call("upsertServers", vec![Value::Array(servers)])
            .and_then(expect_count)
    }
    /// Remove a server and unseat its members. False when there was no such server.
    pub fn remove_server(&mut self, id: &str) -> Result<bool, String> {
        self.call("removeServer", vec![json!(id)])
            .and_then(expect_bool)
    }
    /// The server `id`, or `None`.
    pub fn get_server(&mut self, id: &str) -> Result<Option<Value>, String> {
        self.call("getServer", vec![json!(id)]).map(optional)
    }
    /// Servers in insertion order, optionally only those with `status` (`open`, `full`, `closed`).
    pub fn list_servers(&mut self, status: Option<&str>) -> Result<Vec<Value>, String> {
        let args = status.map(|s| vec![json!(s)]).unwrap_or_default();
        self.call("listServers", args).and_then(expect_list)
    }
    /// Set a server's status. `full` is refused (false) while it has free seats.
    pub fn set_server_status(&mut self, id: &str, status: &str) -> Result<bool, String> {
        self.call("setServerStatus", vec![json!(id), json!(status)])
            .and_then(expect_bool)
    }
    /// Servers `user_id` fits, best first, as `{id, score}`. `options` is
    /// `{limit?, strategy?, affinity?}` or `None` for the defaults.
    pub fn find_servers_for_user(
        &mut self,
        user_id: &str,
        options: Option<Value>,
    ) -> Result<Vec<Value>, String> {
        self.call("findServersForUser", vec![json!(user_id), opt_arg(options)])
            .and_then(expect_list)
    }
    /// Users that fit `server_id`, best first, as `{id, score}`.
    pub fn find_users_for_server(
        &mut self,
        server_id: &str,
        options: Option<Value>,
    ) -> Result<Vec<Value>, String> {
        self.call(
            "findUsersForServer",
            vec![json!(server_id), opt_arg(options)],
        )
        .and_then(expect_list)
    }
    /// Whether `user_id` and `server_id` satisfy each other's requirements and the server has room.
    pub fn fits(&mut self, user_id: &str, server_id: &str) -> Result<bool, String> {
        self.call("fits", vec![json!(user_id), json!(server_id)])
            .and_then(expect_bool)
    }
    /// Emit an `advertised` event with the servers `user_id` fits. Returns how many.
    pub fn advertise(&mut self, user_id: &str, options: Option<Value>) -> Result<usize, String> {
        self.call("advertise", vec![json!(user_id), opt_arg(options)])
            .and_then(expect_count)
    }
    /// Advertise for every waiting user. Returns the total number of matches advertised.
    pub fn advertise_all(&mut self, options: Option<Value>) -> Result<usize, String> {
        self.call("advertiseAll", vec![opt_arg(options)])
            .and_then(expect_count)
    }
    /// Seat `user_id` in `server_id`. Returns `{ok, reason?, userId, serverId}`.
    pub fn join(&mut self, user_id: &str, server_id: &str) -> Result<Value, String> {
        self.call("join", vec![json!(user_id), json!(server_id)])
    }
    /// Unseat `user_id`. Returns `{ok, reason?, userId, serverId}`.
    pub fn leave(&mut self, user_id: &str) -> Result<Value, String> {
        self.call("leave", vec![json!(user_id)])
    }
    /// Seat `user_id` in its best fitting server. Returns `{ok, reason?, userId, serverId}`.
    pub fn auto_join(&mut self, user_id: &str, options: Option<Value>) -> Result<Value, String> {
        self.call("autoJoin", vec![json!(user_id), opt_arg(options)])
    }
    /// Seat every waiting user that fits somewhere, oldest first. Returns one result per seat.
    pub fn match_all(&mut self, options: Option<Value>) -> Result<Vec<Value>, String> {
        self.call("matchAll", vec![opt_arg(options)])
            .and_then(expect_list)
    }
    /// Create `count` users (at most 100000) from a generation `spec`, or `None` for plain users.
    pub fn generate_users(
        &mut self,
        count: usize,
        spec: Option<Value>,
    ) -> Result<Vec<Value>, String> {
        self.call("generateUsers", vec![json!(count), opt_arg(spec)])
            .and_then(expect_list)
    }
    /// Take every queued event, oldest first.
    pub fn drain_events(&mut self) -> Result<Vec<Value>, String> {
        self.call("drainEvents", vec![]).and_then(expect_list)
    }
    /// How many events are queued.
    pub fn pending_events(&mut self) -> Result<usize, String> {
        self.call("pendingEvents", vec![]).and_then(expect_count)
    }
    /// Counts of users and servers by status, queued events and lifetime totals.
    pub fn stats(&mut self) -> Result<Value, String> {
        self.call("stats", vec![])
    }
    /// Remove every user, server and queued event, then emit `cleared`.
    pub fn clear(&mut self) -> Result<(), String> {
        self.call("clear", vec![]).map(|_| ())
    }
    /// Restart the random generator from `seed`.
    pub fn reseed(&mut self, seed: u64) -> Result<(), String> {
        self.call("reseed", vec![json!(seed)]).map(|_| ())
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;

    /// The method names `dispatch` matches on, read from this file's source.
    fn dispatched() -> Vec<String> {
        let source = include_str!("engine.rs");
        let start = source.find("pub fn dispatch(").unwrap();
        let end = start + source[start..].find("unknown method").unwrap();
        source[start..end]
            .lines()
            .filter_map(|line| {
                let rest = line.strip_prefix("            \"")?;
                let (name, tail) = rest.split_once('"')?;
                tail.trim_start().starts_with("=>").then(|| name.to_owned())
            })
            .collect()
    }

    #[test]
    fn every_dispatch_method_has_a_typed_wrapper() {
        let names = dispatched();
        assert_eq!(names.len(), 28, "{names:?}");
        let listed: Vec<String> = DISPATCH_METHODS.iter().map(|m| m.to_string()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        let mut listed_sorted = listed.clone();
        listed_sorted.sort();
        assert_eq!(
            sorted, listed_sorted,
            "DISPATCH_METHODS drifted from dispatch"
        );
        let source = include_str!("engine.rs");
        // Whitespace is dropped so a call that rustfmt splits over lines still matches.
        let typed: String = source[source.find("fn call(&mut self, method").unwrap()..]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        for name in &names {
            assert!(
                typed.contains(&format!("self.call(\"{name}\"")),
                "no typed wrapper for {name}"
            );
        }
    }

    #[test]
    fn typed_methods_match_dispatch() {
        let mut e = Engine::new(7);
        assert!(e
            .upsert_server(json!({"id":"s","capacity":2,"minMembers":2,"scopes":{"g":"x"}}))
            .unwrap());
        assert_eq!(e.upsert_servers(vec![json!({"id":"t"})]).unwrap(), 1);
        assert!(e.upsert_user(json!({"id":"a","scopes":{"g":"x"}})).unwrap());
        assert_eq!(
            e.upsert_users(vec![json!({"id":"b"}), json!({"id":"a"})])
                .unwrap(),
            1
        );
        assert_eq!(e.get_user("a").unwrap().unwrap()["id"], "a");
        assert!(e.get_user("nobody").unwrap().is_none());
        assert_eq!(e.list_users(None).unwrap().len(), 2);
        assert_eq!(e.list_users(Some("paired")).unwrap().len(), 0);
        assert!(e.list_users(Some("bogus")).is_err());
        assert!(e.fits("a", "s").unwrap());
        assert!(!e.find_servers_for_user("a", None).unwrap().is_empty());
        assert!(!e
            .find_users_for_server("s", Some(json!({"limit":1})))
            .unwrap()
            .is_empty());
        assert!(e.advertise("a", None).unwrap() >= 1);
        assert!(e.advertise_all(None).unwrap() >= 1);
        assert_eq!(e.join("a", "s").unwrap()["ok"], true);
        assert_eq!(e.leave("a").unwrap()["ok"], true);
        assert_eq!(e.auto_join("a", None).unwrap()["ok"], true);
        assert!(!e.match_all(None).unwrap().is_empty());
        assert!(e.set_user_status("b", "idle").unwrap());
        assert!(e.set_server_status("t", "closed").unwrap());
        assert_eq!(e.list_servers(Some("closed")).unwrap().len(), 1);
        assert!(e.get_server("s").unwrap().is_some());
        assert_eq!(e.generate_users(3, None).unwrap().len(), 3);
        assert!(e.pending_events().unwrap() > 0);
        assert!(!e.drain_events().unwrap().is_empty());
        assert_eq!(e.pending_events().unwrap(), 0);
        assert_eq!(e.stats().unwrap()["users"], 5);
        assert!(e.remove_user("b").unwrap());
        assert!(e.remove_server("t").unwrap());
        assert_eq!(
            e.pair_peers(json!({"room":"r","capacity":2,"users":[{"id":"p","scopes":{}},{"id":"q","scopes":{}}]}))
                .unwrap()["serverId"],
            "r"
        );
        e.reseed(1).unwrap();
        e.clear().unwrap();
        assert_eq!(e.stats().unwrap()["users"], 0);
    }
}
