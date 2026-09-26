// LiveEngine core — a dependency-free, agnostic live pairing engine.
//
// Two hubs: users (with scopes + requirements) and servers (with scopes,
// requirements, capacity, status). The engine answers one question quickly:
// "which servers fit this user right now?" (and the mirror: "which users fit
// this server?"). Everything else — advertising, joining, auto-pairing,
// random user generation — is built on that primitive.
//
// This header has no Node/N-API dependency so the core can be unit-tested and
// embedded on its own.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace liveengine {

// ---------------------------------------------------------------- values ----

// A scope value is user-defined data: number, bool, string, or list of strings (tags).
using ScopeValue = std::variant<std::monostate, double, bool, std::string, std::vector<std::string>>;
using Scopes = std::unordered_map<std::string, ScopeValue>;

// ---------------------------------------------------------- requirements ----

enum class Op {
    Eq, Ne, Lt, Lte, Gt, Gte,   // scalar comparisons
    In, Nin,                    // scalar in / not in list
    Between,                    // numeric [lo, hi] inclusive
    Exists, Missing,            // key presence
    Contains, NotContains,      // list scope contains value
    Overlaps,                   // list scope shares any element with list value
};

const char* opName(Op op);
std::optional<Op> parseOp(const std::string& name);

struct Requirement {
    std::string key;
    Op op = Op::Eq;
    ScopeValue value;                  // for Eq/Ne/Lt/.../Contains
    std::vector<ScopeValue> values;    // for In/Nin/Between/Overlaps
};

struct RequirementSet {
    std::vector<Requirement> rules;
    bool matchAll = true;              // true = AND, false = OR
    bool empty() const { return rules.empty(); }
};

// Evaluates a single rule against a scope map. Public so it can be unit tested.
bool evaluate(const Requirement& rule, const Scopes& scopes);
bool satisfies(const RequirementSet& set, const Scopes& scopes);

// ------------------------------------------------------------- entities ----

enum class UserStatus { Waiting, Paired, Idle };
enum class ServerStatus { Open, Full, Closed };

const char* userStatusName(UserStatus s);
const char* serverStatusName(ServerStatus s);
std::optional<UserStatus> parseUserStatus(const std::string& s);
std::optional<ServerStatus> parseServerStatus(const std::string& s);

struct User {
    std::string id;
    Scopes scopes;
    RequirementSet requirements;       // what the user demands of a server
    UserStatus status = UserStatus::Waiting;
    std::string serverId;              // set while Paired
    uint64_t seq = 0;                  // arrival order (FIFO fairness)
};

struct Server {
    std::string id;
    Scopes scopes;
    RequirementSet requirements;       // what the server demands of a user
    ServerStatus status = ServerStatus::Open;
    uint32_t capacity = 1;
    uint32_t minMembers = 0;           // emits server_ready once members >= minMembers (0 = never)
    std::vector<std::string> members;
    uint64_t seq = 0;

    bool hasRoom() const { return status == ServerStatus::Open && members.size() < capacity; }
};

// --------------------------------------------------------------- matching ----

enum class Strategy {
    First,        // lowest seq (oldest) candidate
    Random,       // uniform random among candidates
    LeastLoaded,  // fewest members first (spread load)
    MostLoaded,   // most members first (fill rooms fast)
    Affinity,     // smallest weighted numeric distance across `affinity` keys
};

const char* strategyName(Strategy s);
std::optional<Strategy> parseStrategy(const std::string& s);

struct Affinity {
    std::string key;                 // numeric scope key present on both sides
    double weight = 1.0;
};

struct MatchOptions {
    size_t limit = 10;               // 0 = unlimited
    Strategy strategy = Strategy::First;
    std::vector<Affinity> affinity;  // used by Strategy::Affinity (and as tie-break for others)
};

struct Match {
    std::string id;                  // server id (or user id for the mirror query)
    double score = 0;                // lower is better; strategy-dependent
    size_t load = 0;                 // server members (for server matches)
};

struct Result {
    bool ok = false;
    std::string reason;              // machine-readable: "not_found", "no_fit", "full", ...
    std::string userId;
    std::string serverId;

    Result() = default;
    Result(bool o, std::string r, std::string u = "", std::string s = "")
        : ok(o), reason(std::move(r)), userId(std::move(u)), serverId(std::move(s)) {}
};

// ----------------------------------------------------------------- events ----

struct Event {
    std::string type;                // user_added, user_updated, user_removed, server_added, ...
    std::string userId;
    std::string serverId;
    std::vector<std::string> ids;    // e.g. advertised server ids
    std::string reason;
    uint64_t seq = 0;

    Event() = default;
    Event(std::string t, std::string u = "", std::string s = "")
        : type(std::move(t)), userId(std::move(u)), serverId(std::move(s)) {}
};

// ------------------------------------------------------ random generation ----

struct ScopeGenerator {
    enum class Kind { Int, Float, Choice, Bool, String, Tags };
    Kind kind = Kind::Int;
    double min = 0, max = 100;                 // Int / Float
    std::vector<std::string> choices;          // Choice / Tags (pick `count` distinct)
    std::vector<double> weights;               // optional, Choice
    double probability = 0.5;                  // Bool
    std::string prefix;                        // String -> prefix + running number
    uint32_t count = 1;                        // Tags
};

struct GenerationSpec {
    std::string idPrefix = "user-";
    std::unordered_map<std::string, ScopeGenerator> scopes;
    RequirementSet requirements;               // copied onto each generated user
};

// ------------------------------------------------------------------ stats ----

struct Stats {
    size_t users = 0, waiting = 0, paired = 0, idle = 0;
    size_t servers = 0, open = 0, full = 0, closed = 0;
    size_t pendingEvents = 0;
    uint64_t totalJoins = 0, totalLeaves = 0, totalMatchesEvaluated = 0;
};

// ----------------------------------------------------------------- engine ----

class Engine {
public:
    explicit Engine(uint64_t seed = 0);

    // --- users
    bool upsertUser(User user);                        // true when created, false when updated
    bool removeUser(const std::string& id);
    std::optional<User> getUser(const std::string& id) const;
    std::vector<User> listUsers(std::optional<UserStatus> status = std::nullopt) const;
    bool setUserStatus(const std::string& id, UserStatus status);

    // --- servers
    bool upsertServer(Server server);
    bool removeServer(const std::string& id);          // members are set back to Waiting
    std::optional<Server> getServer(const std::string& id) const;
    std::vector<Server> listServers(std::optional<ServerStatus> status = std::nullopt) const;
    bool setServerStatus(const std::string& id, ServerStatus status);

    // --- matching (read-only)
    std::vector<Match> findServersForUser(const std::string& userId, const MatchOptions& opts = {}) const;
    std::vector<Match> findUsersForServer(const std::string& serverId, const MatchOptions& opts = {}) const;
    bool fits(const std::string& userId, const std::string& serverId) const;

    // --- advertising: compute fits for waiting users and emit "advertised" events
    size_t advertise(const std::string& userId, const MatchOptions& opts = {});
    size_t advertiseAll(const MatchOptions& opts = {});

    // --- pairing
    Result join(const std::string& userId, const std::string& serverId);
    Result leave(const std::string& userId);
    Result autoJoin(const std::string& userId, const MatchOptions& opts = {});
    std::vector<Result> matchAll(const MatchOptions& opts = {});   // FIFO over waiting users

    // --- random users
    std::vector<User> generateUsers(size_t count, const GenerationSpec& spec);

    // --- events
    std::vector<Event> drainEvents();
    size_t pendingEvents() const;

    // --- misc
    Stats stats() const;
    void clear();
    void reseed(uint64_t seed);

private:
    // Buckets are ordered by arrival seq so a first-fit scan can stop at `limit`.
    using Bucket = std::map<uint64_t, std::string>;
    // Equality index: key -> canonical value -> bucket.  Range index: key -> number -> bucket.
    struct Index {
        std::unordered_map<std::string, std::unordered_map<std::string, Bucket>> eq;
        std::unordered_map<std::string, std::map<double, Bucket>> num;
        void add(uint64_t seq, const std::string& id, const Scopes& scopes);
        void remove(uint64_t seq, const Scopes& scopes);
        // Returns the narrowest candidate set (seq order) implied by the rule set, or nullopt for "scan everything".
        std::optional<std::vector<std::pair<uint64_t, std::string>>> candidates(const RequirementSet& reqs) const;
    };

    mutable std::mutex mu_;
    std::unordered_map<std::string, User> users_;
    std::unordered_map<std::string, Server> servers_;
    std::map<uint64_t, std::string> waiting_;          // seq -> user id (FIFO)
    Bucket openServers_;                               // seq -> server id, servers with room
    Index userIndex_, serverIndex_;
    std::vector<Event> events_;
    uint64_t seq_ = 0;
    uint64_t eventSeq_ = 0;
    mutable std::mt19937_64 rng_;
    uint64_t genCounter_ = 0;
    uint64_t totalJoins_ = 0, totalLeaves_ = 0;
    mutable uint64_t evaluated_ = 0;

    // unlocked helpers
    void emit(Event e);
    void refreshServerAvailability(Server& s);
    bool fitsUnlocked(const User& u, const Server& s) const;
    std::vector<Match> serversForUnlocked(const User& u, const MatchOptions& opts) const;
    std::vector<Match> usersForUnlocked(const Server& s, const MatchOptions& opts) const;
    Result joinUnlocked(User& u, Server& s);
    Result leaveUnlocked(User& u, const std::string& reason);
    void rank(std::vector<Match>& out, const MatchOptions& opts, const Scopes& mine,
              const std::function<const Scopes&(const std::string&)>& theirs) const;
};

// Canonical string for a scalar scope value (used by the equality index and tests).
std::string canonical(const ScopeValue& v);

}  // namespace liveengine
