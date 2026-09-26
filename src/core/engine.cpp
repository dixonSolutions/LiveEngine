#include "engine.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <sstream>

namespace liveengine {

// ------------------------------------------------------------------ names ----

const char* opName(Op op) {
    switch (op) {
        case Op::Eq: return "eq";           case Op::Ne: return "ne";
        case Op::Lt: return "lt";           case Op::Lte: return "lte";
        case Op::Gt: return "gt";           case Op::Gte: return "gte";
        case Op::In: return "in";           case Op::Nin: return "nin";
        case Op::Between: return "between"; case Op::Exists: return "exists";
        case Op::Missing: return "missing"; case Op::Contains: return "contains";
        case Op::NotContains: return "notContains"; case Op::Overlaps: return "overlaps";
    }
    return "eq";
}

std::optional<Op> parseOp(const std::string& n) {
    static const std::unordered_map<std::string, Op> table = {
        {"eq", Op::Eq}, {"==", Op::Eq}, {"ne", Op::Ne}, {"!=", Op::Ne},
        {"lt", Op::Lt}, {"<", Op::Lt}, {"lte", Op::Lte}, {"<=", Op::Lte},
        {"gt", Op::Gt}, {">", Op::Gt}, {"gte", Op::Gte}, {">=", Op::Gte},
        {"in", Op::In}, {"nin", Op::Nin}, {"between", Op::Between},
        {"exists", Op::Exists}, {"missing", Op::Missing},
        {"contains", Op::Contains}, {"notContains", Op::NotContains}, {"overlaps", Op::Overlaps},
    };
    auto it = table.find(n);
    if (it == table.end()) return std::nullopt;
    return it->second;
}

const char* userStatusName(UserStatus s) {
    switch (s) { case UserStatus::Waiting: return "waiting"; case UserStatus::Paired: return "paired"; case UserStatus::Idle: return "idle"; }
    return "waiting";
}
const char* serverStatusName(ServerStatus s) {
    switch (s) { case ServerStatus::Open: return "open"; case ServerStatus::Full: return "full"; case ServerStatus::Closed: return "closed"; }
    return "open";
}
std::optional<UserStatus> parseUserStatus(const std::string& s) {
    if (s == "waiting") return UserStatus::Waiting;
    if (s == "paired") return UserStatus::Paired;
    if (s == "idle") return UserStatus::Idle;
    return std::nullopt;
}
std::optional<ServerStatus> parseServerStatus(const std::string& s) {
    if (s == "open") return ServerStatus::Open;
    if (s == "full") return ServerStatus::Full;
    if (s == "closed") return ServerStatus::Closed;
    return std::nullopt;
}
const char* strategyName(Strategy s) {
    switch (s) {
        case Strategy::First: return "first"; case Strategy::Random: return "random";
        case Strategy::LeastLoaded: return "leastLoaded"; case Strategy::MostLoaded: return "mostLoaded";
        case Strategy::Affinity: return "affinity";
    }
    return "first";
}
std::optional<Strategy> parseStrategy(const std::string& s) {
    if (s == "first") return Strategy::First;
    if (s == "random") return Strategy::Random;
    if (s == "leastLoaded") return Strategy::LeastLoaded;
    if (s == "mostLoaded") return Strategy::MostLoaded;
    if (s == "affinity") return Strategy::Affinity;
    return std::nullopt;
}

// ----------------------------------------------------------------- values ----

std::string canonical(const ScopeValue& v) {
    struct V {
        std::string operator()(std::monostate) const { return "_"; }
        std::string operator()(double d) const {
            std::ostringstream os; os.precision(17); os << "n:" << d; return os.str();
        }
        std::string operator()(bool b) const { return b ? "b:1" : "b:0"; }
        std::string operator()(const std::string& s) const { return "s:" + s; }
        std::string operator()(const std::vector<std::string>& l) const {
            std::string out = "l:"; for (auto& s : l) { out += s; out += '\x1f'; } return out;
        }
    };
    return std::visit(V{}, v);
}

static std::optional<double> asNumber(const ScopeValue& v) {
    if (auto d = std::get_if<double>(&v)) return *d;
    if (auto b = std::get_if<bool>(&v)) return *b ? 1.0 : 0.0;
    return std::nullopt;
}

static bool scalarEq(const ScopeValue& a, const ScopeValue& b) {
    if (a.index() == b.index()) return canonical(a) == canonical(b);
    auto na = asNumber(a), nb = asNumber(b);
    return na && nb && *na == *nb;
}

static bool listHas(const ScopeValue& list, const ScopeValue& needle) {
    auto l = std::get_if<std::vector<std::string>>(&list);
    if (!l) return scalarEq(list, needle);  // scalar scope: contains == eq
    if (auto s = std::get_if<std::string>(&needle)) return std::find(l->begin(), l->end(), *s) != l->end();
    auto n = asNumber(needle);
    if (!n) return false;
    return std::find(l->begin(), l->end(), canonical(needle).substr(2)) != l->end();
}

bool evaluate(const Requirement& r, const Scopes& scopes) {
    auto it = scopes.find(r.key);
    const bool present = it != scopes.end() && !std::holds_alternative<std::monostate>(it->second);
    if (r.op == Op::Exists) return present;
    if (r.op == Op::Missing) return !present;
    if (!present) return r.op == Op::Ne || r.op == Op::Nin || r.op == Op::NotContains;
    const ScopeValue& v = it->second;
    switch (r.op) {
        case Op::Eq: return scalarEq(v, r.value);
        case Op::Ne: return !scalarEq(v, r.value);
        case Op::Lt: case Op::Lte: case Op::Gt: case Op::Gte: {
            auto a = asNumber(v), b = asNumber(r.value);
            if (!a || !b) {
                auto sa = std::get_if<std::string>(&v); auto sb = std::get_if<std::string>(&r.value);
                if (!sa || !sb) return false;
                int c = sa->compare(*sb);
                return r.op == Op::Lt ? c < 0 : r.op == Op::Lte ? c <= 0 : r.op == Op::Gt ? c > 0 : c >= 0;
            }
            return r.op == Op::Lt ? *a < *b : r.op == Op::Lte ? *a <= *b : r.op == Op::Gt ? *a > *b : *a >= *b;
        }
        case Op::In: case Op::Nin: {
            bool found = false;
            for (auto& x : r.values) if (scalarEq(v, x)) { found = true; break; }
            return r.op == Op::In ? found : !found;
        }
        case Op::Between: {
            if (r.values.size() < 2) return false;
            auto a = asNumber(v), lo = asNumber(r.values[0]), hi = asNumber(r.values[1]);
            return a && lo && hi && *a >= *lo && *a <= *hi;
        }
        case Op::Contains: return listHas(v, r.value);
        case Op::NotContains: return !listHas(v, r.value);
        case Op::Overlaps: {
            for (auto& x : r.values) if (listHas(v, x)) return true;
            return false;
        }
        default: return false;
    }
}

bool satisfies(const RequirementSet& set, const Scopes& scopes) {
    if (set.rules.empty()) return true;
    if (set.matchAll) {
        for (auto& r : set.rules) if (!evaluate(r, scopes)) return false;
        return true;
    }
    for (auto& r : set.rules) if (evaluate(r, scopes)) return true;
    return false;
}

// ------------------------------------------------------------------ index ----

void Engine::Index::add(uint64_t seq, const std::string& id, const Scopes& scopes) {
    for (auto& [k, v] : scopes) {
        if (auto l = std::get_if<std::vector<std::string>>(&v)) {
            for (auto& s : *l) eq[k]["s:" + s][seq] = id;
            continue;
        }
        if (std::holds_alternative<std::monostate>(v)) continue;
        eq[k][canonical(v)][seq] = id;
        if (auto n = asNumber(v)) num[k][*n][seq] = id;
    }
}

void Engine::Index::remove(uint64_t seq, const Scopes& scopes) {
    for (auto& [k, v] : scopes) {
        auto ek = eq.find(k);
        if (ek != eq.end()) {
            auto erase = [&](const std::string& c) {
                auto b = ek->second.find(c);
                if (b == ek->second.end()) return;
                b->second.erase(seq);
                if (b->second.empty()) ek->second.erase(b);
            };
            if (auto l = std::get_if<std::vector<std::string>>(&v)) for (auto& s : *l) erase("s:" + s);
            else erase(canonical(v));
            if (ek->second.empty()) eq.erase(ek);
        }
        if (auto n = asNumber(v)) {
            auto nk = num.find(k);
            if (nk != num.end()) {
                auto b = nk->second.find(*n);
                if (b != nk->second.end()) { b->second.erase(seq); if (b->second.empty()) nk->second.erase(b); }
                if (nk->second.empty()) num.erase(nk);
            }
        }
    }
}

std::optional<std::vector<std::pair<uint64_t, std::string>>> Engine::Index::candidates(const RequirementSet& reqs) const {
    using Out = std::vector<std::pair<uint64_t, std::string>>;
    if (reqs.rules.empty() || !reqs.matchAll) return std::nullopt;
    // Prefer the smallest equality bucket; fall back to the tightest numeric range.
    const Bucket* best = nullptr;
    for (auto& r : reqs.rules) {
        if (r.op != Op::Eq && r.op != Op::Contains) continue;
        if (std::holds_alternative<std::vector<std::string>>(r.value)) continue;
        auto k = eq.find(r.key);
        if (k == eq.end()) return Out{};   // nobody has this key/value → no fit
        auto b = k->second.find(canonical(r.value));
        if (b == k->second.end()) return Out{};
        if (!best || b->second.size() < best->size()) best = &b->second;
    }
    if (best) return Out(best->begin(), best->end());

    for (auto& r : reqs.rules) {
        double lo = -std::numeric_limits<double>::infinity(), hi = std::numeric_limits<double>::infinity();
        bool loOpen = false, hiOpen = false;
        if (r.op == Op::Between && r.values.size() >= 2) {
            auto a = asNumber(r.values[0]), b = asNumber(r.values[1]);
            if (!a || !b) continue;
            lo = *a; hi = *b;
        } else if (r.op == Op::Lt || r.op == Op::Lte || r.op == Op::Gt || r.op == Op::Gte) {
            auto n = asNumber(r.value);
            if (!n) continue;
            if (r.op == Op::Lt) { hi = *n; hiOpen = true; } else if (r.op == Op::Lte) hi = *n;
            else if (r.op == Op::Gt) { lo = *n; loOpen = true; } else lo = *n;
        } else continue;
        auto k = num.find(r.key);
        if (k == num.end()) return Out{};
        Out out;
        auto it = loOpen ? k->second.upper_bound(lo) : k->second.lower_bound(lo);
        for (; it != k->second.end(); ++it) {
            if (hiOpen ? it->first >= hi : it->first > hi) break;
            out.insert(out.end(), it->second.begin(), it->second.end());
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    return std::nullopt;
}

// ----------------------------------------------------------------- engine ----

Engine::Engine(uint64_t seed) : rng_(seed ? seed : std::random_device{}()) {}

void Engine::reseed(uint64_t seed) { std::lock_guard<std::mutex> g(mu_); rng_.seed(seed); }

void Engine::emit(Event e) { e.seq = ++eventSeq_; events_.push_back(std::move(e)); }

void Engine::refreshServerAvailability(Server& s) {
    const bool wasOpen = openServers_.count(s.seq) > 0;
    if (s.status == ServerStatus::Full && s.members.size() < s.capacity) s.status = ServerStatus::Open;
    if (s.status == ServerStatus::Open && s.members.size() >= s.capacity) s.status = ServerStatus::Full;
    const bool nowOpen = s.hasRoom();
    if (nowOpen && !wasOpen) { openServers_[s.seq] = s.id; emit({"server_available", "", s.id}); }
    if (!nowOpen && wasOpen) {
        openServers_.erase(s.seq);
        emit({s.status == ServerStatus::Full ? "server_full" : "server_unavailable", "", s.id});
    }
}

// --- users

bool Engine::upsertUser(User user) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = users_.find(user.id);
    if (it == users_.end()) {
        user.seq = ++seq_;
        user.serverId.clear();
        if (user.status == UserStatus::Paired) user.status = UserStatus::Waiting;
        userIndex_.add(user.seq, user.id, user.scopes);
        if (user.status == UserStatus::Waiting) waiting_[user.seq] = user.id;
        const std::string id = user.id;
        users_.emplace(id, std::move(user));
        emit({"user_added", id});
        return true;
    }
    User& u = it->second;
    userIndex_.remove(u.seq, u.scopes);
    u.scopes = std::move(user.scopes);
    u.requirements = std::move(user.requirements);
    userIndex_.add(u.seq, u.id, u.scopes);
    if (u.status != UserStatus::Paired && user.status != UserStatus::Paired && user.status != u.status) {
        if (user.status == UserStatus::Waiting) waiting_[u.seq] = u.id; else waiting_.erase(u.seq);
        u.status = user.status;
    }
    emit({"user_updated", u.id, u.serverId});
    return false;
}

bool Engine::removeUser(const std::string& id) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = users_.find(id);
    if (it == users_.end()) return false;
    User& u = it->second;
    if (u.status == UserStatus::Paired) leaveUnlocked(u, "user_removed");
    waiting_.erase(u.seq);
    userIndex_.remove(u.seq, u.scopes);
    users_.erase(it);
    emit({"user_removed", id});
    return true;
}

std::optional<User> Engine::getUser(const std::string& id) const {
    std::lock_guard<std::mutex> g(mu_);
    auto it = users_.find(id);
    if (it == users_.end()) return std::nullopt;
    return it->second;
}

std::vector<User> Engine::listUsers(std::optional<UserStatus> status) const {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<User> out;
    for (auto& [_, u] : users_) if (!status || u.status == *status) out.push_back(u);
    std::sort(out.begin(), out.end(), [](const User& a, const User& b) { return a.seq < b.seq; });
    return out;
}

bool Engine::setUserStatus(const std::string& id, UserStatus status) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = users_.find(id);
    if (it == users_.end()) return false;
    User& u = it->second;
    if (status == UserStatus::Paired) return u.status == UserStatus::Paired;  // only join() pairs
    if (u.status == UserStatus::Paired) leaveUnlocked(u, "status_change");
    waiting_.erase(u.seq);
    u.status = status;
    if (status == UserStatus::Waiting) waiting_[u.seq] = u.id;
    emit({"user_updated", u.id});
    return true;
}

// --- servers

bool Engine::upsertServer(Server server) {
    std::lock_guard<std::mutex> g(mu_);
    if (server.capacity == 0) server.capacity = 1;
    auto it = servers_.find(server.id);
    if (it == servers_.end()) {
        server.seq = ++seq_;
        server.members.clear();
        if (server.status == ServerStatus::Full) server.status = ServerStatus::Open;
        serverIndex_.add(server.seq, server.id, server.scopes);
        auto& s = servers_.emplace(server.id, std::move(server)).first->second;
        emit({"server_added", "", s.id});
        refreshServerAvailability(s);
        return true;
    }
    Server& s = it->second;
    serverIndex_.remove(s.seq, s.scopes);
    s.scopes = std::move(server.scopes);
    s.requirements = std::move(server.requirements);
    s.capacity = server.capacity;
    s.minMembers = server.minMembers;
    if (server.status != ServerStatus::Full) s.status = server.status;
    serverIndex_.add(s.seq, s.id, s.scopes);
    emit({"server_updated", "", s.id});
    refreshServerAvailability(s);
    return false;
}

bool Engine::removeServer(const std::string& id) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = servers_.find(id);
    if (it == servers_.end()) return false;
    Server& s = it->second;
    for (auto& uid : std::vector<std::string>(s.members)) {
        auto u = users_.find(uid);
        if (u != users_.end()) leaveUnlocked(u->second, "server_removed");
    }
    openServers_.erase(s.seq);
    serverIndex_.remove(s.seq, s.scopes);
    servers_.erase(it);
    emit({"server_removed", "", id});
    return true;
}

std::optional<Server> Engine::getServer(const std::string& id) const {
    std::lock_guard<std::mutex> g(mu_);
    auto it = servers_.find(id);
    if (it == servers_.end()) return std::nullopt;
    return it->second;
}

std::vector<Server> Engine::listServers(std::optional<ServerStatus> status) const {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<Server> out;
    for (auto& [_, s] : servers_) if (!status || s.status == *status) out.push_back(s);
    std::sort(out.begin(), out.end(), [](const Server& a, const Server& b) { return a.seq < b.seq; });
    return out;
}

bool Engine::setServerStatus(const std::string& id, ServerStatus status) {
    std::lock_guard<std::mutex> g(mu_);
    auto it = servers_.find(id);
    if (it == servers_.end()) return false;
    Server& s = it->second;
    if (status == ServerStatus::Full && s.members.size() < s.capacity) return false;  // derived state
    s.status = status;
    emit({"server_updated", "", s.id});
    refreshServerAvailability(s);
    return true;
}

// --- matching

bool Engine::fitsUnlocked(const User& u, const Server& s) const {
    ++evaluated_;
    if (!s.hasRoom()) return false;
    if (!satisfies(s.requirements, u.scopes)) return false;
    if (!satisfies(u.requirements, s.scopes)) return false;
    return true;
}

bool Engine::fits(const std::string& userId, const std::string& serverId) const {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId); auto s = servers_.find(serverId);
    if (u == users_.end() || s == servers_.end()) return false;
    return fitsUnlocked(u->second, s->second);
}

void Engine::rank(std::vector<Match>& out, const MatchOptions& opts, const Scopes& mine,
                  const std::function<const Scopes&(const std::string&)>& theirs) const {
    auto affinityScore = [&](const std::string& id) {
        double d = 0;
        const Scopes& other = theirs(id);
        for (auto& a : opts.affinity) {
            auto x = mine.find(a.key); auto y = other.find(a.key);
            if (x == mine.end() || y == other.end()) { d += a.weight * 1e6; continue; }
            auto nx = asNumber(x->second), ny = asNumber(y->second);
            if (!nx || !ny) { d += a.weight * (canonical(x->second) == canonical(y->second) ? 0 : 1e6); continue; }
            d += a.weight * std::fabs(*nx - *ny);
        }
        return d;
    };
    switch (opts.strategy) {
        case Strategy::First: break;  // already in seq order
        case Strategy::Random: {
            std::shuffle(out.begin(), out.end(), rng_);
            for (size_t i = 0; i < out.size(); ++i) out[i].score = static_cast<double>(i);
            break;
        }
        case Strategy::LeastLoaded:
            for (auto& m : out) m.score = static_cast<double>(m.load);
            std::stable_sort(out.begin(), out.end(), [](const Match& a, const Match& b) { return a.score < b.score; });
            break;
        case Strategy::MostLoaded:
            for (auto& m : out) m.score = -static_cast<double>(m.load);
            std::stable_sort(out.begin(), out.end(), [](const Match& a, const Match& b) { return a.score < b.score; });
            break;
        case Strategy::Affinity:
            for (auto& m : out) m.score = affinityScore(m.id);
            std::stable_sort(out.begin(), out.end(), [](const Match& a, const Match& b) { return a.score < b.score; });
            break;
    }
    if (opts.limit && out.size() > opts.limit) out.resize(opts.limit);
}

std::vector<Match> Engine::serversForUnlocked(const User& u, const MatchOptions& opts) const {
    std::vector<Match> out;
    // First-fit can stop as soon as `limit` candidates are found because scans run in seq order.
    const size_t stopAt = (opts.strategy == Strategy::First && opts.limit) ? opts.limit : 0;
    auto consider = [&](const std::string& sid) {
        auto it = servers_.find(sid);
        if (it == servers_.end()) return false;
        const Server& s = it->second;
        if (fitsUnlocked(u, s)) out.push_back({s.id, static_cast<double>(s.seq), s.members.size()});
        return stopAt && out.size() >= stopAt;
    };
    auto cands = serverIndex_.candidates(u.requirements);
    if (cands) { for (auto& [seq, id] : *cands) { if (openServers_.count(seq) && consider(id)) break; } }
    else for (auto& [seq, id] : openServers_) if (consider(id)) break;
    rank(out, opts, u.scopes, [&](const std::string& id) -> const Scopes& { return servers_.at(id).scopes; });
    return out;
}

std::vector<Match> Engine::usersForUnlocked(const Server& s, const MatchOptions& opts) const {
    std::vector<Match> out;
    if (!s.hasRoom()) return out;
    const size_t stopAt = (opts.strategy == Strategy::First && opts.limit) ? opts.limit : 0;
    auto consider = [&](const std::string& uid) {
        auto it = users_.find(uid);
        if (it == users_.end() || it->second.status != UserStatus::Waiting) return false;
        const User& u = it->second;
        if (fitsUnlocked(u, s)) out.push_back({u.id, static_cast<double>(u.seq), 0});
        return stopAt && out.size() >= stopAt;
    };
    auto cands = userIndex_.candidates(s.requirements);
    if (cands) { for (auto& [seq, id] : *cands) { if (waiting_.count(seq) && consider(id)) break; } }
    else for (auto& [_, id] : waiting_) if (consider(id)) break;
    rank(out, opts, s.scopes, [&](const std::string& id) -> const Scopes& { return users_.at(id).scopes; });
    return out;
}

std::vector<Match> Engine::findServersForUser(const std::string& userId, const MatchOptions& opts) const {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId);
    if (u == users_.end()) return {};
    return serversForUnlocked(u->second, opts);
}

std::vector<Match> Engine::findUsersForServer(const std::string& serverId, const MatchOptions& opts) const {
    std::lock_guard<std::mutex> g(mu_);
    auto s = servers_.find(serverId);
    if (s == servers_.end()) return {};
    return usersForUnlocked(s->second, opts);
}

// --- advertising

size_t Engine::advertise(const std::string& userId, const MatchOptions& opts) {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId);
    if (u == users_.end()) return 0;
    auto matches = serversForUnlocked(u->second, opts);
    Event e{"advertised", userId};
    for (auto& m : matches) e.ids.push_back(m.id);
    emit(std::move(e));
    return matches.size();
}

size_t Engine::advertiseAll(const MatchOptions& opts) {
    std::lock_guard<std::mutex> g(mu_);
    size_t total = 0;
    for (auto& [_, uid] : waiting_) {
        auto u = users_.find(uid);
        if (u == users_.end()) continue;
        auto matches = serversForUnlocked(u->second, opts);
        if (matches.empty()) continue;
        Event e{"advertised", uid};
        for (auto& m : matches) e.ids.push_back(m.id);
        emit(std::move(e));
        total += matches.size();
    }
    return total;
}

// --- pairing

Result Engine::joinUnlocked(User& u, Server& s) {
    if (u.status == UserStatus::Paired) return {false, "already_paired", u.id, u.serverId};
    if (!s.hasRoom()) return {false, s.status == ServerStatus::Closed ? "closed" : "full", u.id, s.id};
    if (!fitsUnlocked(u, s)) return {false, "no_fit", u.id, s.id};
    s.members.push_back(u.id);
    u.status = UserStatus::Paired;
    u.serverId = s.id;
    waiting_.erase(u.seq);
    ++totalJoins_;
    emit({"paired", u.id, s.id});
    refreshServerAvailability(s);
    if (s.minMembers && s.members.size() == s.minMembers) {
        Event e{"server_ready", "", s.id};
        e.ids = s.members;
        emit(std::move(e));
    }
    return {true, "", u.id, s.id};
}

Result Engine::leaveUnlocked(User& u, const std::string& reason) {
    if (u.status != UserStatus::Paired) return {false, "not_paired", u.id};
    const std::string sid = u.serverId;
    auto it = servers_.find(sid);
    if (it != servers_.end()) {
        Server& s = it->second;
        s.members.erase(std::remove(s.members.begin(), s.members.end(), u.id), s.members.end());
        ++totalLeaves_;
        Event e{"unpaired", u.id, sid}; e.reason = reason; emit(std::move(e));
        refreshServerAvailability(s);
    }
    u.status = UserStatus::Waiting;
    u.serverId.clear();
    waiting_[u.seq] = u.id;
    return {true, "", u.id, sid};
}

Result Engine::join(const std::string& userId, const std::string& serverId) {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId); auto s = servers_.find(serverId);
    if (u == users_.end()) return {false, "user_not_found", userId, serverId};
    if (s == servers_.end()) return {false, "server_not_found", userId, serverId};
    return joinUnlocked(u->second, s->second);
}

Result Engine::leave(const std::string& userId) {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId);
    if (u == users_.end()) return {false, "user_not_found", userId};
    return leaveUnlocked(u->second, "left");
}

Result Engine::autoJoin(const std::string& userId, const MatchOptions& opts) {
    std::lock_guard<std::mutex> g(mu_);
    auto u = users_.find(userId);
    if (u == users_.end()) return {false, "user_not_found", userId};
    if (u->second.status == UserStatus::Paired) return {false, "already_paired", userId, u->second.serverId};
    MatchOptions o = opts; o.limit = 1;
    auto matches = serversForUnlocked(u->second, o);
    if (matches.empty()) return {false, "no_fit", userId};
    return joinUnlocked(u->second, servers_.at(matches.front().id));
}

std::vector<Result> Engine::matchAll(const MatchOptions& opts) {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<Result> out;
    MatchOptions o = opts; o.limit = 1;
    std::vector<std::string> queue;
    for (auto& [_, uid] : waiting_) queue.push_back(uid);
    for (auto& uid : queue) {
        auto u = users_.find(uid);
        if (u == users_.end() || u->second.status != UserStatus::Waiting) continue;
        auto matches = serversForUnlocked(u->second, o);
        if (matches.empty()) continue;
        out.push_back(joinUnlocked(u->second, servers_.at(matches.front().id)));
    }
    return out;
}

// --- random users

std::vector<User> Engine::generateUsers(size_t count, const GenerationSpec& spec) {
    std::vector<User> made;
    made.reserve(count);
    {
        std::lock_guard<std::mutex> g(mu_);
        for (size_t i = 0; i < count; ++i) {
            User u;
            u.id = spec.idPrefix + std::to_string(++genCounter_);
            u.requirements = spec.requirements;
            for (auto& [key, gen] : spec.scopes) {
                using K = ScopeGenerator::Kind;
                switch (gen.kind) {
                    case K::Int: {
                        std::uniform_int_distribution<long long> d(static_cast<long long>(gen.min), static_cast<long long>(gen.max));
                        u.scopes[key] = static_cast<double>(d(rng_)); break;
                    }
                    case K::Float: { std::uniform_real_distribution<double> d(gen.min, gen.max); u.scopes[key] = d(rng_); break; }
                    case K::Bool: { std::bernoulli_distribution d(gen.probability); u.scopes[key] = d(rng_); break; }
                    case K::String: u.scopes[key] = gen.prefix + std::to_string(genCounter_); break;
                    case K::Choice: {
                        if (gen.choices.empty()) break;
                        size_t idx;
                        if (gen.weights.size() == gen.choices.size()) {
                            std::discrete_distribution<size_t> d(gen.weights.begin(), gen.weights.end()); idx = d(rng_);
                        } else { std::uniform_int_distribution<size_t> d(0, gen.choices.size() - 1); idx = d(rng_); }
                        u.scopes[key] = gen.choices[idx]; break;
                    }
                    case K::Tags: {
                        std::vector<std::string> pool = gen.choices;
                        std::shuffle(pool.begin(), pool.end(), rng_);
                        if (pool.size() > gen.count) pool.resize(gen.count);
                        u.scopes[key] = pool; break;
                    }
                }
            }
            made.push_back(u);
        }
    }
    for (auto& u : made) upsertUser(u);
    for (auto& u : made) u = *getUser(u.id);
    return made;
}

// --- events / stats

std::vector<Event> Engine::drainEvents() {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<Event> out;
    out.swap(events_);
    return out;
}

size_t Engine::pendingEvents() const { std::lock_guard<std::mutex> g(mu_); return events_.size(); }

Stats Engine::stats() const {
    std::lock_guard<std::mutex> g(mu_);
    Stats st;
    st.users = users_.size();
    for (auto& [_, u] : users_) {
        if (u.status == UserStatus::Waiting) ++st.waiting; else if (u.status == UserStatus::Paired) ++st.paired; else ++st.idle;
    }
    st.servers = servers_.size();
    for (auto& [_, s] : servers_) {
        if (s.status == ServerStatus::Open) ++st.open; else if (s.status == ServerStatus::Full) ++st.full; else ++st.closed;
    }
    st.pendingEvents = events_.size();
    st.totalJoins = totalJoins_; st.totalLeaves = totalLeaves_; st.totalMatchesEvaluated = evaluated_;
    return st;
}

void Engine::clear() {
    std::lock_guard<std::mutex> g(mu_);
    users_.clear(); servers_.clear(); waiting_.clear(); openServers_.clear();
    userIndex_ = Index{}; serverIndex_ = Index{};
    events_.clear();
    emit({"cleared"});
}

}  // namespace liveengine
