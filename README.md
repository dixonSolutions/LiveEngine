# LiveEngine

**A general-purpose live pairing engine.** C++17 core, packaged for Node.js through
[Node-API](https://nodejs.org/api/n-api.html) with the
[node-addon-api](https://github.com/nodejs/node-addon-api) wrapper.

LiveEngine knows nothing about your domain. You give it two live hubs:

| Hub | What it holds | What you define |
|---|---|---|
| **Users** | people, players, agents, clients, workers | `scopes` (any key → number / bool / string / tags) and optional `requirements` on a server |
| **Servers** | rooms, tables, matches, lobbies, hosts, shifts | `scopes`, `requirements` on a user, `capacity`, `minMembers`, `status` |

and it answers the only question that matters live: **which servers fit this user right
now — requirements both ways, and a free seat?** Fits are *advertised* back to the user hub as
events; users can then *join* a server (or be *auto-joined*), servers fill up and become
*ready*, seats free up when users leave. There is a seeded random user generator for load,
bots and simulations.

Because the model is agnostic it applies to game lobbies, tournament brackets, online chess
pairing, co-op sessions, support-agent routing, ride sharing — anything that pairs a stream of
arrivals with a pool of limited-capacity slots under user-defined rules.

```
                      requirements ⇄ scopes                         events
   ┌──────────┐      ┌──────────────────────────┐      ┌──────────┐    advertised
   │ user hub │ ───▶ │  LiveEngine (C++ core)   │ ◀─── │server hub│    paired / unpaired
   │  scopes  │      │  indexes · fit · rank    │      │ capacity │    server_ready
   │  reqs    │ ◀─── │  join · leave · matchAll │ ───▶ │ status   │    server_full / available
   └──────────┘      └──────────────────────────┘      └──────────┘
```

---

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Requirements DSL](#requirements-dsl)
- [Matching and ranking strategies](#matching-and-ranking-strategies)
- [Events](#events)
- [Random user generation](#random-user-generation)
- [API reference](#api-reference)
- [Scaling](#scaling)
- [Example appliance: the math game](#example-appliance-the-math-game)
- [Modelling recipes](#modelling-recipes) (chess, tournaments, game servers)
- [Using the C++ core directly](#using-the-c-core-directly)
- [Project layout, development, tests](#project-layout)
- [Roadmap](#roadmap)

---

## Install

```sh
npm install @dixonsolutions/liveengine
```

The addon is compiled on install with `node-gyp`, so you need a C++17 toolchain and Python 3:

| Platform | Toolchain |
|---|---|
| Linux | `build-essential` (g++ ≥ 9) |
| macOS | Xcode command line tools |
| Windows | Visual Studio Build Tools (C++ workload) |

Node.js ≥ 18. No runtime dependencies besides `node-addon-api` headers at build time.

## Quick start

```js
const { LiveEngine } = require('@dixonsolutions/liveengine');

const engine = new LiveEngine({ seed: 42 });

engine.on('advertised', ev => console.log(`${ev.userId} can join`, ev.ids));
engine.on('server_ready', ev => console.log(`${ev.serverId} can start with`, ev.ids));

// Servers: two chess tables with a rating band, two seats each, start when both are taken.
engine.upsertServers([
  { id: 'table-1500', capacity: 2, minMembers: 2,
    scopes: { game: 'chess', rating: 1500 },
    requirements: { game: 'chess', rating: { between: [1300, 1700] } } },
  { id: 'table-2000', capacity: 2, minMembers: 2,
    scopes: { game: 'chess', rating: 2000 },
    requirements: { game: 'chess', rating: { between: [1800, 2200] } } },
]);

// Users: join the hub, get the fitting servers advertised back immediately.
engine.joinHub({ id: 'ada', scopes: { game: 'chess', rating: 1450 } });
engine.joinHub({ id: 'bob', scopes: { game: 'chess', rating: 1620 }, requirements: { game: 'chess' } });

// Pair everyone who is waiting (FIFO, first fit) — 'paired' then 'server_ready' fire.
engine.matchAll();

console.log(engine.getServer('table-1500').members); // ['ada', 'bob']
console.log(engine.stats());                          // { users: 2, waiting: 0, paired: 2, servers: 2, full: 1, ... }
```

TypeScript users get full typings from `index.d.ts`.

## Concepts

**Scopes** are your data. Any key, with a value that is a number, a boolean, a string, or a
list of strings (tags). Both users and servers have scopes.

**Requirements** are predicates over the *other side's* scopes. A server's requirements are
evaluated against a user's scopes, and a user's requirements are evaluated against a server's
scopes. A pair *fits* when both requirement sets pass **and** the server has a free seat.

**Capacity** is the number of seats. A server is `open` while it has room, becomes `full`
automatically when the last seat is taken, and returns to `open` when someone leaves. Setting
it `closed` hides it from matching without dropping its members.

**`minMembers`** is the "can start" threshold. When a join brings the member count to exactly
`minMembers`, a `server_ready` event carries the member list — the natural trigger for
"start the game".

**Status of a user** is `waiting` (in the hub, eligible), `paired` (seated at `serverId`), or
`idle` (in the hub, not eligible). Only `join()`/`autoJoin()`/`matchAll()` can make a user
`paired`.

**Arrival order** is preserved: every user and server gets a monotonic `seq`. FIFO fairness in
`matchAll` and the `first` strategy come from it.

## Requirements DSL

Three interchangeable forms are accepted everywhere a `requirements` field appears.

```js
// 1. Shorthand object — scalar means eq, array means in, nested object means {op: value}
{ region: 'eu', mode: ['ranked', 'casual'], rating: { between: [1300, 1700] }, tags: { contains: 'fast' } }

// 2. Rule array
[{ key: 'region', op: 'eq', value: 'eu' }, { key: 'rating', op: 'gte', value: 1300 }]

// 3. Rule set with match mode ('all' = AND, default; 'any' = OR)
{ rules: [{ key: 'region', op: 'eq', value: 'us' }, { key: 'premium', op: 'eq', value: true }], match: 'any' }
```

| Op | Meaning | Value |
|---|---|---|
| `eq`, `ne` | equal / not equal (numbers and bools compare numerically) | scalar |
| `lt`, `lte`, `gt`, `gte` | numeric comparison (strings compare lexically) | scalar |
| `in`, `nin` | scalar is / is not one of | array |
| `between` | `lo ≤ x ≤ hi` | `[lo, hi]` |
| `exists`, `missing` | key present / absent | — |
| `contains`, `notContains` | tag list contains value (scalar scope: same as `eq`) | scalar |
| `overlaps` | tag list shares any element with | array |

A missing key fails every positive test and passes `ne`, `nin`, `notContains`, `missing`.
Unknown ops throw a `TypeError` at insert time, not silently at match time.

## Matching and ranking strategies

All read queries and pairing calls take a `MatchOptions` object:

```js
{ limit: 10, strategy: 'first' | 'random' | 'leastLoaded' | 'mostLoaded' | 'affinity', affinity: ['rating'] }
```

| Strategy | Order |
|---|---|
| `first` (default) | oldest server first (arrival `seq`). Stops scanning at `limit` — the cheapest path. |
| `random` | uniform shuffle of the fitting set (seeded) |
| `leastLoaded` | fewest members first — spread users across servers |
| `mostLoaded` | most members first — fill servers so they start sooner |
| `affinity` | smallest weighted numeric distance between user and server scopes on the given keys, e.g. rating closeness. `affinity: ['rating']`, `[{ key: 'rating', weight: 2 }]` or `{ rating: 2, latency: 0.1 }`. Supplying `affinity` without a `strategy` selects it. |

`limit: 0` means unlimited. Each match is `{ id, score, load }` where `score` is the strategy's
sort key (lower is better) and `load` is the server's member count.

## Events

Every mutating call on `LiveEngine` drains the native event queue and emits each event both by
type and as a catch-all `'event'`. Construct with `{ autoEmit: false }` to poll `drainEvents()`
yourself instead (cheaper for bulk work).

| Event | Fields | When |
|---|---|---|
| `user_added`, `user_updated`, `user_removed` | `userId` | hub changes |
| `server_added`, `server_updated`, `server_removed` | `serverId` | hub changes |
| `server_available` | `serverId` | a server gains a free seat (or is opened) |
| `server_full` | `serverId` | last seat taken |
| `server_unavailable` | `serverId` | closed while it still had room |
| `server_ready` | `serverId`, `ids` (members) | member count reaches `minMembers` |
| `advertised` | `userId`, `ids` (server ids in rank order) | `advertise*` / `joinHub` |
| `paired` | `userId`, `serverId` | a join succeeded |
| `unpaired` | `userId`, `serverId`, `reason` (`left`, `user_removed`, `server_removed`, `status_change`) | a seat freed |
| `cleared` | — | `clear()` |

Events carry a monotonic `seq` so consumers can detect gaps.

## Random user generation

`generateUsers(count, spec)` creates users from a per-key generator spec, upserts them and
returns them. It is deterministic for a given engine `seed`, which makes load tests and
simulations reproducible.

```js
engine.generateUsers(1000, {
  idPrefix: 'bot-',
  scopes: {
    rating:  { type: 'int',    min: 800, max: 2400 },
    latency: { type: 'float',  min: 5,   max: 120 },
    region:  ['eu', 'us', 'apac'],                          // shorthand for choice
    tier:    { type: 'choice', values: ['free', 'pro'], weights: [0.8, 0.2] },
    premium: { type: 'bool',   p: 0.3 },
    name:    { type: 'string', prefix: 'Bot ' },            // "Bot 1", "Bot 2", …
    tags:    { type: 'tags',   values: ['fast', 'ranked', 'voice', 'new'], count: 2 },
  },
  requirements: { game: 'chess' },                           // copied onto every generated user
});
```

## API reference

Everything below is available on both `LiveEngine` (EventEmitter wrapper, recommended) and the
raw `NativeEngine` class. Signatures are in [`index.d.ts`](index.d.ts).

### Users

| Method | Returns | Notes |
|---|---|---|
| `upsertUser({ id, scopes?, requirements?, status? })` | `boolean` created | updating keeps `seq` and pairing |
| `upsertUsers(users[])` | `number` created | one boundary crossing for bulk loads |
| `removeUser(id)` | `boolean` | frees the seat if paired |
| `getUser(id)` | `User \| null` | |
| `listUsers(status?)` | `User[]` in arrival order | |
| `setUserStatus(id, 'waiting' \| 'idle')` | `boolean` | leaving a server if paired |

### Servers

| Method | Returns | Notes |
|---|---|---|
| `upsertServer({ id, scopes?, requirements?, capacity?, minMembers?, status? })` | `boolean` created | `full` is derived and cannot be set |
| `upsertServers(servers[])` | `number` created | |
| `removeServer(id)` | `boolean` | members return to `waiting` with `unpaired` events |
| `getServer(id)` | `Server \| null` | includes `members`, `free` |
| `listServers(status?)` | `Server[]` | |
| `setServerStatus(id, 'open' \| 'closed')` | `boolean` | |

### Matching

| Method | Returns |
|---|---|
| `findServersForUser(userId, options?)` | `Match[]` — servers that fit, ranked |
| `findUsersForServer(serverId, options?)` | `Match[]` — waiting users that fit, ranked |
| `fits(userId, serverId)` | `boolean` |
| `advertise(userId, options?)` | number of servers advertised (emits `advertised`) |
| `advertiseAll(options?)` | total advertised across all waiting users (one event per user with ≥1 fit) |

### Pairing

| Method | Returns |
|---|---|
| `join(userId, serverId)` | `{ ok, reason, userId, serverId }` — reasons: `user_not_found`, `server_not_found`, `already_paired`, `full`, `closed`, `no_fit` |
| `leave(userId)` | `Result` — reason `not_paired` |
| `autoJoin(userId, options?)` | `Result` — joins the top-ranked fit |
| `matchAll(options?)` | `Result[]` — walks waiting users FIFO, auto-joins each |
| `pair(userId, options?)` *(wrapper only)* | `Result & { server }` |
| `joinHub(user, options?)` *(wrapper only)* | upsert + advertise in one call |
| `startAutoMatch(intervalMs?, options?)` / `stopAutoMatch()` *(wrapper only)* | timer-driven `matchAll` |

### Everything else

`generateUsers(count, spec)`, `drainEvents()`, `pendingEvents()`, `stats()`, `clear()`,
`reseed(seed)`.

`stats()` → `{ users, waiting, paired, idle, servers, open, full, closed, pendingEvents, totalJoins, totalLeaves, totalMatchesEvaluated }`.

## Scaling

The core is designed so that the cost of a query is proportional to the *candidates*, not to
the hub size:

- **Equality index** — for every scope key, `value → servers` (and `value → users`) buckets.
  A query whose requirements contain an `eq`/`contains` rule scans only the smallest bucket.
- **Numeric index** — ordered `number → ids` per key; `between`, `lt`, `lte`, `gt`, `gte`
  rules become range scans.
- **Seq-ordered buckets** — every bucket is ordered by arrival, so the default `first`
  strategy stops as soon as `limit` fits are found (`limit: 1` for `autoJoin`/`matchAll`).
- **Open-server set** — closed and full servers are never visited.
- **Bulk entry points** — `upsertUsers`, `upsertServers`, `generateUsers`, `matchAll`,
  `advertiseAll` do their loops in C++ with one JS boundary crossing.
- **Single mutex** — the engine is safe to share across threads in the C++ core; the Node
  binding is single-threaded like every N-API addon.

`npm run bench` (5 000 servers × 10 seats, 50 000 generated users, users demand
`region: eu` + `mode: ranked`, servers demand a tier band; ~86 % of users cannot fit anywhere,
which is the worst case) on a laptop, Node 22:

| Operation | Time |
|---|---|
| `upsertServers` × 5 000 | 34 ms |
| `generateUsers` × 50 000 | 420 ms |
| `findServersForUser` × 10 000, indexed `eq`, `limit: 5` | 290 ms (29 µs each) |
| `findServersForUser` × 10 000, `affinity` ranking | 1.5 s (full candidate ranking) |
| `matchAll` over 50 000 waiting users | 3.5 s (42 M fit evaluations, ~80 ns each) |

The remaining cost is users with **no** fit: proving nothing fits means evaluating every
candidate the index yields. An interval index over server-side requirements is on the roadmap
for that case.

## Example appliance: the math game

[`examples/math-game`](examples/math-game) is a complete Node.js server built on LiveEngine
with zero dependencies:

- **Users** are players (humans over Server-Sent Events, or bots from `generateUsers`) with a
  `level` scope: `easy` (+), `medium` (×, −), `hard` (× then ±).
- **Servers** are two-seat tables per level (`requirements: { level }`, `minMembers: 2`).
- A player joins the hub, gets fitting tables **advertised**, is seated at the fullest fitting
  table (`strategy: 'mostLoaded'`), or a new table is opened for their level.
- `server_ready` starts a 5-round game: the first correct answer wins the round, 12 s per
  round. `game_over` frees the seats, the table goes back to `open`, and waiting players get a
  fresh `advertised` event.
- Bots get a random `speedMs` and `accuracy` from the generator and answer accordingly.

```sh
npm run example          # http://localhost:3131
curl -XPOST localhost:3131/api/bots -H 'content-type: application/json' -d '{"count":6}'
```

| Endpoint | Purpose |
|---|---|
| `GET /` | browser client: join, see advertised tables, answer, watch the hub |
| `POST /api/join {name, level}` | add a player, advertise, auto-seat |
| `GET /api/events?userId=` | SSE stream: `advertised`, `seated`, `game_start`, `question`, `round_result`, `wrong`, `game_over`, … |
| `POST /api/answer {userId, answer}` | answer the current question |
| `POST /api/seat {userId, tableId?}` | sit at a specific table, or the best fit |
| `POST /api/bots {count, level?}` | generate and seat bots |
| `GET /api/state` | stats, tables, waiting players, running games |

[`test/math-game.test.js`](test/math-game.test.js) boots this server on a random port, joins a
human over HTTP, follows the SSE stream, adds a bot, plays all five rounds, and checks that the
seats are freed afterwards. It is the integration test for the whole stack.

## Modelling recipes

**Online chess (rating-band pairing).** Servers are boards with `capacity: 2, minMembers: 2`,
scopes `{ rating: <host rating>, timeControl }`, requirements `{ rating: { between: [r-150, r+150] } }`.
Pair with `{ affinity: ['rating'] }` so the closest rating wins when several boards fit. Create
a board when a player arrives and nothing fits (as the math game does), remove it when the game
ends.

**Tournament rounds.** Each pairing slot is a server with `capacity: 2` and requirements on
`{ bracket, round, notPlayed: { notContains: '<opponent>' } }` (tags on the user hold past
opponents). `findUsersForServer` gives the ordered candidate list for a seat; `server_ready`
publishes the pairing.

**Game servers with regions and modes.** Servers carry `{ region, mode, map, version }` and
`capacity: 64`. Users' requirements express their filters (`region: ['eu', 'us']`,
`mode: 'ranked'`, `version: { gte: 12 }`); the server's requirements guard entry
(`rank: { between: [...] }`, `banned: { ne: true }`). Use `leastLoaded` to spread or
`mostLoaded` to fill, and `startAutoMatch(200)` to pair continuously.

**Support routing.** Agents are servers (`capacity` = concurrent chats, `skills` tags);
customers are users with `requirements: { skills: { contains: 'billing' }, language: 'de' }`.

## Using the C++ core directly

[`src/core/engine.hpp`](src/core/engine.hpp) / [`engine.cpp`](src/core/engine.cpp) have no Node
dependency. Compile them into any C++17 program:

```cpp
#include "engine.hpp"
using namespace liveengine;

Engine e(42);
Server s; s.id = "room"; s.capacity = 4; s.minMembers = 2;
s.requirements.rules = {{"level", Op::Between, {}, {1.0, 3.0}}};
e.upsertServer(s);
User u; u.id = "ada"; u.scopes["level"] = 2.0;
e.upsertUser(u);
auto r = e.autoJoin("ada");          // r.ok == true, r.serverId == "room"
for (auto& ev : e.drainEvents()) { /* ev.type, ev.userId, ev.serverId, ev.ids */ }
```

`npm run test:cpp` builds and runs [`tests/cpp/engine_test.cpp`](tests/cpp/engine_test.cpp)
against the core alone.

## Project layout

```
LiveEngine/
├── src/core/engine.{hpp,cpp}   C++17 engine, no Node dependency
├── src/addon/binding.cc        node-addon-api binding (JS ⇄ core conversion)
├── lib/index.js                LiveEngine EventEmitter wrapper
├── index.d.ts                  TypeScript typings
├── binding.gyp                 node-gyp build
├── test/                       node:test suites (binding, end-to-end math game)
├── tests/cpp/                  standalone core tests
├── bench/bench.js              throughput benchmark
├── examples/math-game/         example appliance: game.js (logic), server.js (HTTP+SSE), public/index.html
└── .github/workflows/ci.yml    Linux / macOS / Windows × Node 18 / 20 / 22
```

### Development

```sh
npm install          # builds the addon
npm test             # JS + end-to-end tests
npm run test:cpp     # core tests without Node
npm run test:all
npm run bench [servers] [users]
npm run build:debug  # debug build of the addon
```

## Roadmap

- Interval index over server-side numeric requirements so "no fit" is answered without a scan.
- Group joins: seat a party of N together or not at all.
- Optional background matcher thread with `ThreadSafeFunction` event delivery.
- Snapshot / restore of both hubs.
- Prebuilt binaries via `prebuildify` so `npm install` needs no toolchain.

## License

[MIT](LICENSE) © dixonSolutions
