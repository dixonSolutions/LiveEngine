// Standalone core tests: g++ -std=c++17 -I src/core src/core/engine.cpp tests/cpp/engine_test.cpp
#include "engine.hpp"
#include <cassert>
#include <cstdio>
#include <cstdlib>

using namespace liveengine;

static int failures = 0;
#define CHECK(cond) do { if (!(cond)) { std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); ++failures; } } while (0)

static Requirement req(const char* key, Op op, ScopeValue v) { Requirement r; r.key = key; r.op = op; r.value = std::move(v); return r; }
static Requirement reqList(const char* key, Op op, std::vector<ScopeValue> vs) { Requirement r; r.key = key; r.op = op; r.values = std::move(vs); return r; }

static void testEvaluate() {
    Scopes s{{"rating", 1500.0}, {"region", std::string("eu")}, {"premium", true}, {"tags", std::vector<std::string>{"fast", "ranked"}}};
    CHECK(evaluate(req("rating", Op::Eq, 1500.0), s));
    CHECK(!evaluate(req("rating", Op::Ne, 1500.0), s));
    CHECK(evaluate(req("rating", Op::Gte, 1500.0), s));
    CHECK(!evaluate(req("rating", Op::Gt, 1500.0), s));
    CHECK(evaluate(reqList("rating", Op::Between, {1000.0, 2000.0}), s));
    CHECK(!evaluate(reqList("rating", Op::Between, {1600.0, 2000.0}), s));
    CHECK(evaluate(reqList("region", Op::In, {std::string("us"), std::string("eu")}), s));
    CHECK(evaluate(reqList("region", Op::Nin, {std::string("us")}), s));
    CHECK(evaluate(req("premium", Op::Eq, true), s));
    CHECK(evaluate(req("tags", Op::Contains, std::string("ranked")), s));
    CHECK(!evaluate(req("tags", Op::Contains, std::string("casual")), s));
    CHECK(evaluate(reqList("tags", Op::Overlaps, {std::string("casual"), std::string("fast")}), s));
    CHECK(evaluate(req("region", Op::Exists, {}), s));
    CHECK(evaluate(req("nope", Op::Missing, {}), s));
    CHECK(evaluate(req("nope", Op::Ne, 1.0), s));       // absent key satisfies Ne
    CHECK(!evaluate(req("nope", Op::Eq, 1.0), s));
    RequirementSet any; any.matchAll = false;
    any.rules = {req("region", Op::Eq, std::string("us")), req("premium", Op::Eq, true)};
    CHECK(satisfies(any, s));
}

static void testPairing() {
    Engine e(42);
    Server chess; chess.id = "chess-1"; chess.capacity = 2; chess.minMembers = 2;
    chess.scopes = {{"game", std::string("chess")}, {"rating", 1500.0}};
    chess.requirements.rules = {reqList("rating", Op::Between, {1300.0, 1700.0}), req("game", Op::Eq, std::string("chess"))};
    CHECK(e.upsertServer(chess));
    Server go = chess; go.id = "go-1"; go.scopes["game"] = std::string("go"); go.requirements.rules[1].value = std::string("go");
    e.upsertServer(go);

    User a; a.id = "a"; a.scopes = {{"game", std::string("chess")}, {"rating", 1400.0}};
    a.requirements.rules = {req("game", Op::Eq, std::string("chess"))};
    User b = a; b.id = "b"; b.scopes["rating"] = 1650.0;
    User c = a; c.id = "c"; c.scopes["rating"] = 2200.0;   // out of band
    User d = a; d.id = "d";                                 // no room after a+b
    e.upsertUser(a); e.upsertUser(b); e.upsertUser(c); e.upsertUser(d);

    auto m = e.findServersForUser("a");
    CHECK(m.size() == 1 && m[0].id == "chess-1");
    CHECK(e.findServersForUser("c").empty());
    auto users = e.findUsersForServer("chess-1");
    CHECK(users.size() == 3 && users[0].id == "a" && users[1].id == "b" && users[2].id == "d");

    e.drainEvents();
    auto results = e.matchAll();
    CHECK(results.size() == 2);
    CHECK(e.getServer("chess-1")->status == ServerStatus::Full);
    CHECK(e.getUser("a")->status == UserStatus::Paired && e.getUser("d")->status == UserStatus::Waiting);
    bool ready = false, full = false;
    for (auto& ev : e.drainEvents()) { if (ev.type == "server_ready") { ready = true; CHECK(ev.ids.size() == 2); } if (ev.type == "server_full") full = true; }
    CHECK(ready && full);

    CHECK(e.join("d", "chess-1").reason == "full");
    CHECK(e.leave("a").ok);
    CHECK(e.getServer("chess-1")->status == ServerStatus::Open);
    CHECK(e.autoJoin("d").ok);
    CHECK(e.getUser("d")->serverId == "chess-1");
    CHECK(e.removeServer("chess-1"));
    CHECK(e.getUser("b")->status == UserStatus::Waiting);
    auto st = e.stats();
    CHECK(st.servers == 1 && st.users == 4 && st.waiting == 4 && st.totalJoins == 3);
}

static void testStrategiesAndIndex() {
    Engine e(7);
    for (int i = 0; i < 5; ++i) {
        Server s; s.id = "s" + std::to_string(i); s.capacity = 10;
        s.scopes = {{"region", std::string(i % 2 ? "eu" : "us")}, {"level", double(i)}};
        e.upsertServer(s);
    }
    User u; u.id = "u"; u.scopes = {{"level", 2.5}};
    u.requirements.rules = {req("region", Op::Eq, std::string("us"))};
    e.upsertUser(u);
    auto m = e.findServersForUser("u");       // index path: eq bucket "us" -> s0, s2, s4
    CHECK(m.size() == 3);
    MatchOptions aff; aff.strategy = Strategy::Affinity; aff.affinity = {{"level", 1.0}};
    m = e.findServersForUser("u", aff);
    CHECK(m.size() == 3 && m[0].id == "s2" && m[1].id == "s4" && m[2].id == "s0");  // 0.5 < 1.5 < 2.5 away
    User v; v.id = "v"; v.requirements.rules = {reqList("level", Op::Between, {1.0, 3.0})};
    e.upsertUser(v);
    m = e.findServersForUser("v");            // numeric index path
    CHECK(m.size() == 3);
    // update moves the server out of the bucket
    Server s2 = *e.getServer("s2"); s2.scopes["region"] = std::string("apac"); e.upsertServer(s2);
    m = e.findServersForUser("u");
    CHECK(m.size() == 2);
    MatchOptions ll; ll.strategy = Strategy::LeastLoaded;
    User w; w.id = "w"; e.upsertUser(w);
    CHECK(e.join("w", "s0").ok);
    m = e.findServersForUser("u", ll);
    CHECK(m[0].id == "s4" && m[0].load == 0);
}

static void testGeneration() {
    Engine e(1);
    GenerationSpec spec; spec.idPrefix = "bot-";
    ScopeGenerator rating; rating.kind = ScopeGenerator::Kind::Int; rating.min = 1000; rating.max = 2000;
    ScopeGenerator region; region.kind = ScopeGenerator::Kind::Choice; region.choices = {"eu", "us"};
    ScopeGenerator tags; tags.kind = ScopeGenerator::Kind::Tags; tags.choices = {"a", "b", "c"}; tags.count = 2;
    spec.scopes = {{"rating", rating}, {"region", region}, {"tags", tags}};
    auto made = e.generateUsers(50, spec);
    CHECK(made.size() == 50 && made[0].id == "bot-1" && made[49].id == "bot-50");
    for (auto& u : made) {
        double r = std::get<double>(u.scopes.at("rating"));
        CHECK(r >= 1000 && r <= 2000);
        CHECK(std::get<std::vector<std::string>>(u.scopes.at("tags")).size() == 2);
    }
    CHECK(e.stats().waiting == 50);
    Engine e2(1);
    auto again = e2.generateUsers(50, spec);
    CHECK(canonical(again[10].scopes.at("rating")) == canonical(made[10].scopes.at("rating")));  // seeded determinism
}

static void testScale() {
    Engine e(3);
    for (int i = 0; i < 2000; ++i) {
        Server s; s.id = "srv" + std::to_string(i); s.capacity = 4;
        s.scopes = {{"region", std::string(i % 4 == 0 ? "eu" : i % 4 == 1 ? "us" : i % 4 == 2 ? "apac" : "sa")}, {"tier", double(i % 10)}};
        s.requirements.rules = {reqList("tier", Op::Between, {double(i % 10) - 1, double(i % 10) + 1})};
        e.upsertServer(s);
    }
    GenerationSpec spec;
    ScopeGenerator region; region.kind = ScopeGenerator::Kind::Choice; region.choices = {"eu", "us", "apac", "sa"};
    ScopeGenerator tier; tier.kind = ScopeGenerator::Kind::Int; tier.min = 0; tier.max = 9;
    spec.scopes = {{"region", region}, {"tier", tier}};
    spec.requirements.rules = {req("region", Op::Eq, std::string("eu"))};
    e.generateUsers(5000, spec);
    auto res = e.matchAll();
    CHECK(res.size() == 2000);   // 500 eu servers * 4 capacity, 5000 users all demanding eu
    CHECK(e.stats().full == 500);
}

int main() {
    testEvaluate(); testPairing(); testStrategiesAndIndex(); testGeneration(); testScale();
    if (failures) { std::fprintf(stderr, "%d failure(s)\n", failures); return 1; }
    std::puts("core tests passed");
    return 0;
}
