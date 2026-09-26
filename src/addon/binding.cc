// N-API binding for LiveEngine (node-addon-api). All conversion between JS
// objects and the core types lives here; the core has no Node dependency.
#include <napi.h>

#include "../core/engine.hpp"

using namespace liveengine;

namespace {

// ------------------------------------------------------------ JS -> core ----

ScopeValue toScopeValue(const Napi::Value& v) {
    if (v.IsNumber()) return v.As<Napi::Number>().DoubleValue();
    if (v.IsBoolean()) return v.As<Napi::Boolean>().Value();
    if (v.IsString()) return v.As<Napi::String>().Utf8Value();
    if (v.IsArray()) {
        auto arr = v.As<Napi::Array>();
        std::vector<std::string> out;
        out.reserve(arr.Length());
        for (uint32_t i = 0; i < arr.Length(); ++i) out.push_back(arr.Get(i).ToString().Utf8Value());
        return out;
    }
    return std::monostate{};
}

Scopes toScopes(const Napi::Value& v) {
    Scopes out;
    if (!v.IsObject() || v.IsArray()) return out;
    auto obj = v.As<Napi::Object>();
    auto keys = obj.GetPropertyNames();
    for (uint32_t i = 0; i < keys.Length(); ++i) {
        std::string k = keys.Get(i).ToString().Utf8Value();
        out[k] = toScopeValue(obj.Get(k));
    }
    return out;
}

bool needsList(Op op) { return op == Op::In || op == Op::Nin || op == Op::Between || op == Op::Overlaps; }

Requirement toRule(Napi::Env env, const std::string& key, const std::string& opName, const Napi::Value& value) {
    auto op = parseOp(opName);
    if (!op) throw Napi::TypeError::New(env, "unknown requirement op: " + opName);
    Requirement r; r.key = key; r.op = *op;
    if (needsList(*op)) {
        if (!value.IsArray()) throw Napi::TypeError::New(env, "op '" + opName + "' on '" + key + "' needs an array value");
        auto arr = value.As<Napi::Array>();
        for (uint32_t i = 0; i < arr.Length(); ++i) r.values.push_back(toScopeValue(arr.Get(i)));
    } else {
        r.value = toScopeValue(value);
    }
    return r;
}

// Accepts:  [{key, op, value}]  |  {rules: [...], match: 'all'|'any'}  |  {key: scalar | {op: value, ...}}
RequirementSet toRequirements(Napi::Env env, const Napi::Value& v) {
    RequirementSet set;
    if (v.IsUndefined() || v.IsNull()) return set;
    auto rulesFromArray = [&](const Napi::Array& arr) {
        for (uint32_t i = 0; i < arr.Length(); ++i) {
            auto o = arr.Get(i);
            if (!o.IsObject()) throw Napi::TypeError::New(env, "requirement rule must be an object");
            auto rule = o.As<Napi::Object>();
            std::string key = rule.Get("key").ToString().Utf8Value();
            std::string op = rule.Has("op") ? rule.Get("op").ToString().Utf8Value() : "eq";
            set.rules.push_back(toRule(env, key, op, rule.Get("value")));
        }
    };
    if (v.IsArray()) { rulesFromArray(v.As<Napi::Array>()); return set; }
    if (!v.IsObject()) throw Napi::TypeError::New(env, "requirements must be an array or object");
    auto obj = v.As<Napi::Object>();
    if (obj.Has("rules") && obj.Get("rules").IsArray()) {
        rulesFromArray(obj.Get("rules").As<Napi::Array>());
        if (obj.Has("match")) set.matchAll = obj.Get("match").ToString().Utf8Value() != "any";
        return set;
    }
    auto keys = obj.GetPropertyNames();
    for (uint32_t i = 0; i < keys.Length(); ++i) {
        std::string key = keys.Get(i).ToString().Utf8Value();
        auto val = obj.Get(key);
        if (val.IsObject() && !val.IsArray()) {
            auto ops = val.As<Napi::Object>();
            auto opKeys = ops.GetPropertyNames();
            for (uint32_t j = 0; j < opKeys.Length(); ++j) {
                std::string op = opKeys.Get(j).ToString().Utf8Value();
                set.rules.push_back(toRule(env, key, op, ops.Get(op)));
            }
        } else if (val.IsArray()) {
            set.rules.push_back(toRule(env, key, "in", val));
        } else {
            set.rules.push_back(toRule(env, key, "eq", val));
        }
    }
    return set;
}

User toUser(Napi::Env env, const Napi::Value& v) {
    if (!v.IsObject()) throw Napi::TypeError::New(env, "user must be an object");
    auto o = v.As<Napi::Object>();
    User u;
    if (!o.Has("id")) throw Napi::TypeError::New(env, "user.id is required");
    u.id = o.Get("id").ToString().Utf8Value();
    u.scopes = toScopes(o.Get("scopes"));
    u.requirements = toRequirements(env, o.Get("requirements"));
    if (o.Has("status") && o.Get("status").IsString()) {
        auto st = parseUserStatus(o.Get("status").As<Napi::String>().Utf8Value());
        if (!st) throw Napi::TypeError::New(env, "unknown user status");
        u.status = *st;
    }
    return u;
}

Server toServer(Napi::Env env, const Napi::Value& v) {
    if (!v.IsObject()) throw Napi::TypeError::New(env, "server must be an object");
    auto o = v.As<Napi::Object>();
    Server s;
    if (!o.Has("id")) throw Napi::TypeError::New(env, "server.id is required");
    s.id = o.Get("id").ToString().Utf8Value();
    s.scopes = toScopes(o.Get("scopes"));
    s.requirements = toRequirements(env, o.Get("requirements"));
    if (o.Has("capacity") && o.Get("capacity").IsNumber()) s.capacity = o.Get("capacity").As<Napi::Number>().Uint32Value();
    if (o.Has("minMembers") && o.Get("minMembers").IsNumber()) s.minMembers = o.Get("minMembers").As<Napi::Number>().Uint32Value();
    if (o.Has("status") && o.Get("status").IsString()) {
        auto st = parseServerStatus(o.Get("status").As<Napi::String>().Utf8Value());
        if (!st) throw Napi::TypeError::New(env, "unknown server status");
        s.status = *st;
    }
    return s;
}

MatchOptions toMatchOptions(Napi::Env env, const Napi::Value& v) {
    MatchOptions o;
    if (v.IsUndefined() || v.IsNull()) return o;
    if (!v.IsObject()) throw Napi::TypeError::New(env, "options must be an object");
    auto obj = v.As<Napi::Object>();
    if (obj.Has("limit") && obj.Get("limit").IsNumber()) o.limit = static_cast<size_t>(obj.Get("limit").As<Napi::Number>().Int64Value());
    if (obj.Has("strategy") && obj.Get("strategy").IsString()) {
        auto st = parseStrategy(obj.Get("strategy").As<Napi::String>().Utf8Value());
        if (!st) throw Napi::TypeError::New(env, "unknown strategy");
        o.strategy = *st;
    }
    if (obj.Has("affinity")) {
        auto a = obj.Get("affinity");
        if (a.IsArray()) {
            auto arr = a.As<Napi::Array>();
            for (uint32_t i = 0; i < arr.Length(); ++i) {
                auto e = arr.Get(i);
                if (e.IsString()) o.affinity.push_back({e.As<Napi::String>().Utf8Value(), 1.0});
                else if (e.IsObject()) {
                    auto eo = e.As<Napi::Object>();
                    Affinity af; af.key = eo.Get("key").ToString().Utf8Value();
                    if (eo.Has("weight") && eo.Get("weight").IsNumber()) af.weight = eo.Get("weight").As<Napi::Number>().DoubleValue();
                    o.affinity.push_back(af);
                }
            }
        } else if (a.IsObject()) {   // { key: weight }
            auto ao = a.As<Napi::Object>();
            auto keys = ao.GetPropertyNames();
            for (uint32_t i = 0; i < keys.Length(); ++i) {
                std::string k = keys.Get(i).ToString().Utf8Value();
                o.affinity.push_back({k, ao.Get(k).ToNumber().DoubleValue()});
            }
        }
        if (!o.affinity.empty() && !(obj.Has("strategy"))) o.strategy = Strategy::Affinity;
    }
    return o;
}

GenerationSpec toGenerationSpec(Napi::Env env, const Napi::Value& v) {
    GenerationSpec spec;
    if (v.IsUndefined() || v.IsNull()) return spec;
    if (!v.IsObject()) throw Napi::TypeError::New(env, "generation spec must be an object");
    auto obj = v.As<Napi::Object>();
    if (obj.Has("idPrefix")) spec.idPrefix = obj.Get("idPrefix").ToString().Utf8Value();
    spec.requirements = toRequirements(env, obj.Get("requirements"));
    if (obj.Has("scopes") && obj.Get("scopes").IsObject()) {
        auto sc = obj.Get("scopes").As<Napi::Object>();
        auto keys = sc.GetPropertyNames();
        for (uint32_t i = 0; i < keys.Length(); ++i) {
            std::string key = keys.Get(i).ToString().Utf8Value();
            auto gv = sc.Get(key);
            ScopeGenerator g;
            using K = ScopeGenerator::Kind;
            if (gv.IsArray()) {                       // shorthand: array → choice
                g.kind = K::Choice;
                auto arr = gv.As<Napi::Array>();
                for (uint32_t j = 0; j < arr.Length(); ++j) g.choices.push_back(arr.Get(j).ToString().Utf8Value());
            } else if (gv.IsObject()) {
                auto go = gv.As<Napi::Object>();
                std::string type = go.Has("type") ? go.Get("type").ToString().Utf8Value() : "int";
                if (type == "int") g.kind = K::Int; else if (type == "float") g.kind = K::Float;
                else if (type == "choice") g.kind = K::Choice; else if (type == "bool") g.kind = K::Bool;
                else if (type == "string") g.kind = K::String; else if (type == "tags") g.kind = K::Tags;
                else throw Napi::TypeError::New(env, "unknown generator type: " + type);
                if (go.Has("min")) g.min = go.Get("min").ToNumber().DoubleValue();
                if (go.Has("max")) g.max = go.Get("max").ToNumber().DoubleValue();
                if (go.Has("p")) g.probability = go.Get("p").ToNumber().DoubleValue();
                if (go.Has("probability")) g.probability = go.Get("probability").ToNumber().DoubleValue();
                if (go.Has("prefix")) g.prefix = go.Get("prefix").ToString().Utf8Value();
                if (go.Has("count")) g.count = go.Get("count").ToNumber().Uint32Value();
                if (go.Has("values") && go.Get("values").IsArray()) {
                    auto arr = go.Get("values").As<Napi::Array>();
                    for (uint32_t j = 0; j < arr.Length(); ++j) g.choices.push_back(arr.Get(j).ToString().Utf8Value());
                }
                if (go.Has("weights") && go.Get("weights").IsArray()) {
                    auto arr = go.Get("weights").As<Napi::Array>();
                    for (uint32_t j = 0; j < arr.Length(); ++j) g.weights.push_back(arr.Get(j).ToNumber().DoubleValue());
                }
            } else {
                throw Napi::TypeError::New(env, "generator for '" + key + "' must be an object or array");
            }
            spec.scopes[key] = g;
        }
    }
    return spec;
}

// ------------------------------------------------------------ core -> JS ----

Napi::Value fromScopeValue(Napi::Env env, const ScopeValue& v) {
    struct V {
        Napi::Env env;
        Napi::Value operator()(std::monostate) const { return env.Null(); }
        Napi::Value operator()(double d) const { return Napi::Number::New(env, d); }
        Napi::Value operator()(bool b) const { return Napi::Boolean::New(env, b); }
        Napi::Value operator()(const std::string& s) const { return Napi::String::New(env, s); }
        Napi::Value operator()(const std::vector<std::string>& l) const {
            auto arr = Napi::Array::New(env, l.size());
            for (size_t i = 0; i < l.size(); ++i) arr.Set(static_cast<uint32_t>(i), Napi::String::New(env, l[i]));
            return arr;
        }
    };
    return std::visit(V{env}, v);
}

Napi::Object fromScopes(Napi::Env env, const Scopes& s) {
    auto o = Napi::Object::New(env);
    for (auto& [k, v] : s) o.Set(k, fromScopeValue(env, v));
    return o;
}

Napi::Object fromRequirements(Napi::Env env, const RequirementSet& set) {
    auto o = Napi::Object::New(env);
    auto rules = Napi::Array::New(env, set.rules.size());
    for (size_t i = 0; i < set.rules.size(); ++i) {
        auto& r = set.rules[i];
        auto ro = Napi::Object::New(env);
        ro.Set("key", r.key);
        ro.Set("op", opName(r.op));
        if (needsList(r.op)) {
            auto arr = Napi::Array::New(env, r.values.size());
            for (size_t j = 0; j < r.values.size(); ++j) arr.Set(static_cast<uint32_t>(j), fromScopeValue(env, r.values[j]));
            ro.Set("value", arr);
        } else {
            ro.Set("value", fromScopeValue(env, r.value));
        }
        rules.Set(static_cast<uint32_t>(i), ro);
    }
    o.Set("rules", rules);
    o.Set("match", set.matchAll ? "all" : "any");
    return o;
}

Napi::Array fromIds(Napi::Env env, const std::vector<std::string>& ids) {
    auto arr = Napi::Array::New(env, ids.size());
    for (size_t i = 0; i < ids.size(); ++i) arr.Set(static_cast<uint32_t>(i), Napi::String::New(env, ids[i]));
    return arr;
}

Napi::Object fromUser(Napi::Env env, const User& u) {
    auto o = Napi::Object::New(env);
    o.Set("id", u.id);
    o.Set("scopes", fromScopes(env, u.scopes));
    o.Set("requirements", fromRequirements(env, u.requirements));
    o.Set("status", userStatusName(u.status));
    o.Set("serverId", u.serverId.empty() ? env.Null() : Napi::String::New(env, u.serverId));
    o.Set("seq", Napi::Number::New(env, static_cast<double>(u.seq)));
    return o;
}

Napi::Object fromServer(Napi::Env env, const Server& s) {
    auto o = Napi::Object::New(env);
    o.Set("id", s.id);
    o.Set("scopes", fromScopes(env, s.scopes));
    o.Set("requirements", fromRequirements(env, s.requirements));
    o.Set("status", serverStatusName(s.status));
    o.Set("capacity", s.capacity);
    o.Set("minMembers", s.minMembers);
    o.Set("members", fromIds(env, s.members));
    o.Set("free", Napi::Number::New(env, s.members.size() < s.capacity ? double(s.capacity - s.members.size()) : 0.0));
    o.Set("seq", Napi::Number::New(env, static_cast<double>(s.seq)));
    return o;
}

Napi::Array fromMatches(Napi::Env env, const std::vector<Match>& ms) {
    auto arr = Napi::Array::New(env, ms.size());
    for (size_t i = 0; i < ms.size(); ++i) {
        auto o = Napi::Object::New(env);
        o.Set("id", ms[i].id);
        o.Set("score", ms[i].score);
        o.Set("load", Napi::Number::New(env, static_cast<double>(ms[i].load)));
        arr.Set(static_cast<uint32_t>(i), o);
    }
    return arr;
}

Napi::Object fromResult(Napi::Env env, const Result& r) {
    auto o = Napi::Object::New(env);
    o.Set("ok", r.ok);
    o.Set("reason", r.reason.empty() ? env.Null() : Napi::String::New(env, r.reason));
    o.Set("userId", r.userId.empty() ? env.Null() : Napi::String::New(env, r.userId));
    o.Set("serverId", r.serverId.empty() ? env.Null() : Napi::String::New(env, r.serverId));
    return o;
}

Napi::Object fromEvent(Napi::Env env, const Event& e) {
    auto o = Napi::Object::New(env);
    o.Set("type", e.type);
    o.Set("seq", Napi::Number::New(env, static_cast<double>(e.seq)));
    if (!e.userId.empty()) o.Set("userId", e.userId);
    if (!e.serverId.empty()) o.Set("serverId", e.serverId);
    if (!e.ids.empty()) o.Set("ids", fromIds(env, e.ids));
    if (!e.reason.empty()) o.Set("reason", e.reason);
    return o;
}

// ---------------------------------------------------------------- wrapper ----

class NativeEngine : public Napi::ObjectWrap<NativeEngine> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports) {
        Napi::Function f = DefineClass(env, "NativeEngine", {
            InstanceMethod("upsertUser", &NativeEngine::UpsertUser),
            InstanceMethod("upsertUsers", &NativeEngine::UpsertUsers),
            InstanceMethod("removeUser", &NativeEngine::RemoveUser),
            InstanceMethod("getUser", &NativeEngine::GetUser),
            InstanceMethod("listUsers", &NativeEngine::ListUsers),
            InstanceMethod("setUserStatus", &NativeEngine::SetUserStatus),
            InstanceMethod("upsertServer", &NativeEngine::UpsertServer),
            InstanceMethod("upsertServers", &NativeEngine::UpsertServers),
            InstanceMethod("removeServer", &NativeEngine::RemoveServer),
            InstanceMethod("getServer", &NativeEngine::GetServer),
            InstanceMethod("listServers", &NativeEngine::ListServers),
            InstanceMethod("setServerStatus", &NativeEngine::SetServerStatus),
            InstanceMethod("findServersForUser", &NativeEngine::FindServersForUser),
            InstanceMethod("findUsersForServer", &NativeEngine::FindUsersForServer),
            InstanceMethod("fits", &NativeEngine::Fits),
            InstanceMethod("advertise", &NativeEngine::Advertise),
            InstanceMethod("advertiseAll", &NativeEngine::AdvertiseAll),
            InstanceMethod("join", &NativeEngine::Join),
            InstanceMethod("leave", &NativeEngine::Leave),
            InstanceMethod("autoJoin", &NativeEngine::AutoJoin),
            InstanceMethod("matchAll", &NativeEngine::MatchAll),
            InstanceMethod("generateUsers", &NativeEngine::GenerateUsers),
            InstanceMethod("drainEvents", &NativeEngine::DrainEvents),
            InstanceMethod("pendingEvents", &NativeEngine::PendingEvents),
            InstanceMethod("stats", &NativeEngine::GetStats),
            InstanceMethod("clear", &NativeEngine::Clear),
            InstanceMethod("reseed", &NativeEngine::Reseed),
        });
        exports.Set("NativeEngine", f);
        return exports;
    }

    explicit NativeEngine(const Napi::CallbackInfo& info)
        : Napi::ObjectWrap<NativeEngine>(info),
          engine_(info.Length() > 0 && info[0].IsNumber() ? static_cast<uint64_t>(info[0].As<Napi::Number>().Int64Value()) : 0) {}

private:
    Engine engine_;

    static std::string str(const Napi::CallbackInfo& info, size_t i, const char* what) {
        if (info.Length() <= i || !info[i].IsString()) throw Napi::TypeError::New(info.Env(), std::string(what) + " must be a string");
        return info[i].As<Napi::String>().Utf8Value();
    }
    static Napi::Value arg(const Napi::CallbackInfo& info, size_t i) { return info.Length() > i ? info[i] : info.Env().Undefined(); }

    Napi::Value UpsertUser(const Napi::CallbackInfo& info) { return Napi::Boolean::New(info.Env(), engine_.upsertUser(toUser(info.Env(), arg(info, 0)))); }
    Napi::Value UpsertUsers(const Napi::CallbackInfo& info) {
        if (!arg(info, 0).IsArray()) throw Napi::TypeError::New(info.Env(), "expected an array of users");
        auto arr = info[0].As<Napi::Array>();
        uint32_t created = 0;
        for (uint32_t i = 0; i < arr.Length(); ++i) if (engine_.upsertUser(toUser(info.Env(), arr.Get(i)))) ++created;
        return Napi::Number::New(info.Env(), created);
    }
    Napi::Value RemoveUser(const Napi::CallbackInfo& info) { return Napi::Boolean::New(info.Env(), engine_.removeUser(str(info, 0, "id"))); }
    Napi::Value GetUser(const Napi::CallbackInfo& info) {
        auto u = engine_.getUser(str(info, 0, "id"));
        return u ? fromUser(info.Env(), *u) : info.Env().Null();
    }
    Napi::Value ListUsers(const Napi::CallbackInfo& info) {
        std::optional<UserStatus> st;
        if (arg(info, 0).IsString()) { st = parseUserStatus(info[0].As<Napi::String>().Utf8Value()); if (!st) throw Napi::TypeError::New(info.Env(), "unknown user status"); }
        auto users = engine_.listUsers(st);
        auto arr = Napi::Array::New(info.Env(), users.size());
        for (size_t i = 0; i < users.size(); ++i) arr.Set(static_cast<uint32_t>(i), fromUser(info.Env(), users[i]));
        return arr;
    }
    Napi::Value SetUserStatus(const Napi::CallbackInfo& info) {
        auto st = parseUserStatus(str(info, 1, "status"));
        if (!st) throw Napi::TypeError::New(info.Env(), "unknown user status");
        return Napi::Boolean::New(info.Env(), engine_.setUserStatus(str(info, 0, "id"), *st));
    }

    Napi::Value UpsertServer(const Napi::CallbackInfo& info) { return Napi::Boolean::New(info.Env(), engine_.upsertServer(toServer(info.Env(), arg(info, 0)))); }
    Napi::Value UpsertServers(const Napi::CallbackInfo& info) {
        if (!arg(info, 0).IsArray()) throw Napi::TypeError::New(info.Env(), "expected an array of servers");
        auto arr = info[0].As<Napi::Array>();
        uint32_t created = 0;
        for (uint32_t i = 0; i < arr.Length(); ++i) if (engine_.upsertServer(toServer(info.Env(), arr.Get(i)))) ++created;
        return Napi::Number::New(info.Env(), created);
    }
    Napi::Value RemoveServer(const Napi::CallbackInfo& info) { return Napi::Boolean::New(info.Env(), engine_.removeServer(str(info, 0, "id"))); }
    Napi::Value GetServer(const Napi::CallbackInfo& info) {
        auto s = engine_.getServer(str(info, 0, "id"));
        return s ? fromServer(info.Env(), *s) : info.Env().Null();
    }
    Napi::Value ListServers(const Napi::CallbackInfo& info) {
        std::optional<ServerStatus> st;
        if (arg(info, 0).IsString()) { st = parseServerStatus(info[0].As<Napi::String>().Utf8Value()); if (!st) throw Napi::TypeError::New(info.Env(), "unknown server status"); }
        auto servers = engine_.listServers(st);
        auto arr = Napi::Array::New(info.Env(), servers.size());
        for (size_t i = 0; i < servers.size(); ++i) arr.Set(static_cast<uint32_t>(i), fromServer(info.Env(), servers[i]));
        return arr;
    }
    Napi::Value SetServerStatus(const Napi::CallbackInfo& info) {
        auto st = parseServerStatus(str(info, 1, "status"));
        if (!st) throw Napi::TypeError::New(info.Env(), "unknown server status");
        return Napi::Boolean::New(info.Env(), engine_.setServerStatus(str(info, 0, "id"), *st));
    }

    Napi::Value FindServersForUser(const Napi::CallbackInfo& info) {
        return fromMatches(info.Env(), engine_.findServersForUser(str(info, 0, "userId"), toMatchOptions(info.Env(), arg(info, 1))));
    }
    Napi::Value FindUsersForServer(const Napi::CallbackInfo& info) {
        return fromMatches(info.Env(), engine_.findUsersForServer(str(info, 0, "serverId"), toMatchOptions(info.Env(), arg(info, 1))));
    }
    Napi::Value Fits(const Napi::CallbackInfo& info) { return Napi::Boolean::New(info.Env(), engine_.fits(str(info, 0, "userId"), str(info, 1, "serverId"))); }
    Napi::Value Advertise(const Napi::CallbackInfo& info) {
        return Napi::Number::New(info.Env(), static_cast<double>(engine_.advertise(str(info, 0, "userId"), toMatchOptions(info.Env(), arg(info, 1)))));
    }
    Napi::Value AdvertiseAll(const Napi::CallbackInfo& info) {
        return Napi::Number::New(info.Env(), static_cast<double>(engine_.advertiseAll(toMatchOptions(info.Env(), arg(info, 0)))));
    }
    Napi::Value Join(const Napi::CallbackInfo& info) { return fromResult(info.Env(), engine_.join(str(info, 0, "userId"), str(info, 1, "serverId"))); }
    Napi::Value Leave(const Napi::CallbackInfo& info) { return fromResult(info.Env(), engine_.leave(str(info, 0, "userId"))); }
    Napi::Value AutoJoin(const Napi::CallbackInfo& info) { return fromResult(info.Env(), engine_.autoJoin(str(info, 0, "userId"), toMatchOptions(info.Env(), arg(info, 1)))); }
    Napi::Value MatchAll(const Napi::CallbackInfo& info) {
        auto results = engine_.matchAll(toMatchOptions(info.Env(), arg(info, 0)));
        auto arr = Napi::Array::New(info.Env(), results.size());
        for (size_t i = 0; i < results.size(); ++i) arr.Set(static_cast<uint32_t>(i), fromResult(info.Env(), results[i]));
        return arr;
    }
    Napi::Value GenerateUsers(const Napi::CallbackInfo& info) {
        if (!arg(info, 0).IsNumber()) throw Napi::TypeError::New(info.Env(), "count must be a number");
        auto n = info[0].As<Napi::Number>().Int64Value();
        if (n < 0) n = 0;
        auto users = engine_.generateUsers(static_cast<size_t>(n), toGenerationSpec(info.Env(), arg(info, 1)));
        auto arr = Napi::Array::New(info.Env(), users.size());
        for (size_t i = 0; i < users.size(); ++i) arr.Set(static_cast<uint32_t>(i), fromUser(info.Env(), users[i]));
        return arr;
    }
    Napi::Value DrainEvents(const Napi::CallbackInfo& info) {
        auto events = engine_.drainEvents();
        auto arr = Napi::Array::New(info.Env(), events.size());
        for (size_t i = 0; i < events.size(); ++i) arr.Set(static_cast<uint32_t>(i), fromEvent(info.Env(), events[i]));
        return arr;
    }
    Napi::Value PendingEvents(const Napi::CallbackInfo& info) { return Napi::Number::New(info.Env(), static_cast<double>(engine_.pendingEvents())); }
    Napi::Value GetStats(const Napi::CallbackInfo& info) {
        auto st = engine_.stats();
        auto o = Napi::Object::New(info.Env());
        auto num = [&](const char* k, uint64_t v) { o.Set(k, Napi::Number::New(info.Env(), static_cast<double>(v))); };
        num("users", st.users); num("waiting", st.waiting); num("paired", st.paired); num("idle", st.idle);
        num("servers", st.servers); num("open", st.open); num("full", st.full); num("closed", st.closed);
        num("pendingEvents", st.pendingEvents); num("totalJoins", st.totalJoins); num("totalLeaves", st.totalLeaves);
        num("totalMatchesEvaluated", st.totalMatchesEvaluated);
        return o;
    }
    Napi::Value Clear(const Napi::CallbackInfo& info) { engine_.clear(); return info.Env().Undefined(); }
    Napi::Value Reseed(const Napi::CallbackInfo& info) {
        if (!arg(info, 0).IsNumber()) throw Napi::TypeError::New(info.Env(), "seed must be a number");
        engine_.reseed(static_cast<uint64_t>(info[0].As<Napi::Number>().Int64Value()));
        return info.Env().Undefined();
    }
};

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
    exports.Set("version", "0.1.0");
    return NativeEngine::Init(env, exports);
}

}  // namespace

NODE_API_MODULE(liveengine, InitAll)
