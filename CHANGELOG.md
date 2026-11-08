# Changelog

## Unreleased

- Revoke password-hub tickets and live connections on credential changes, bound concurrent login attempts, and upgrade password hashes on login.

- Add account and connection clients, browser sessions, and an optional administrator console.

- Add administrative live connection listing, kicks and account bans, including linked rooms.

- Add password-backed hub authentication using account sessions and role restrictions.

- Add local accounts, password login, role-based sessions and durable session storage.

- Remove API-created join and pair users when their seats are released, preserve seat tokens across page reloads, and derive seat routes from pair URLs with trailing slashes or queries.

- Pair rooms default to two peers; `target.capacity` can request a larger room up to `LIVEENGINE_MAX_ROOM_CAPACITY` (default 64). The Gogh Strike bridge defaults to 12.

- Protect browser hub API seats with private, hashed seat tokens; the live client handles renewal, heartbeat and leave.

- Check published TypeScript declarations and documentation samples in CI.

## 0.3.0 — 2026-11-06

- Add `createHubApi` (`@dixonsolutions/liveengine/hub-api`): a dependency-free Node handler that publishes schemas and channels, pairs players with `waitMs`, renews, heartbeats, issues watch tickets, tracks seats and reaps idle ones, with Origin checks, CORS, body and rate limits. A left player is now removed from the hub, so a cancelled search leaves no ghost candidate. Add `watchHubs` to `LiveClient` for a live hub list in a page, and `signal` to `HubClient.pair`.

- Gogh Strike: a Play online panel in the lobby, scenarios 9 to 14 in the end-to-end test, a self-contained game setup (`integrations/gogh-strike/setup.mjs`) and `npm run dev:gogh`.

- Restrict provider-hosted credentials to local registrations by default, add administrator-controlled remote opt-in, and enforce hosted hub quotas across descendants.

- Start on read-only filesystems: without a configured location the instance id stays in memory, with a warning.

- Validate live limits at startup, make the registry subscriber cap configurable, expose live error codes, and repair templates, clients, contract metadata and link service tickets.

- Add live registry and two-peer messaging to the user hub demo, and document live rooms, links, pairing, provider hosting and authentication.

- Queue concurrent calls to a hub in arrival order for up to `LIVEENGINE_CALL_QUEUE_MS` (default 1000, at most `LIVEENGINE_MAX_QUEUED_CALLS` waiting) instead of refusing them at once; key waiting `pair` peers by id and scopes.

- Add `waitMs` to `POST /v1/pair` and the `pair` route operation (`HubClient.pair`): a peer with no partner waits in a bounded per-namespace queue and is woken by the peer that pairs with it, with `408` on timeout. Bounded by `LIVEENGINE_MAX_PAIR_WAIT_MS` (default 30000) and `LIVEENGINE_MAX_PENDING_PAIRS`. The Gogh Strike bridge no longer polls.

- Expose the engine's system-room flag and use it when selecting peer rooms.

- Reject forged server event types and linked frames claiming a sender announced on another link.

- Add the dependency-free `LiveClient` (`@dixonsolutions/liveengine/live-client`), which connects from a pairing descriptor, retries and renews tickets, and opens WebRTC data channels with automatic fallback to the hub relay. `HubClient` gains `ticket`, `watchTicket`, `watchRegistry`, `discover`, `pair` and `renewal`, all accepting an optional credential. `pair` responses include `userId`. The Rust `HubClient` and the CLI gain `ticket`, `watch-ticket` and `watch`.

- Make `cargo clippy --all-targets -- -D warnings` clean.

- Add provider-hosted hubs with a hub-scoped credential, carry verified identity and claims into tickets, welcome and presence, verify a pairing credential once, and put the live transport behind a trait.

- Preserve inherited live channel policies and referenced schemas when issuing tickets through remote centrals.

- Add persistent ticket-authenticated service links with connection status and retry budgets.

- Pair peers atomically in system rooms, validate peer scopes against the user schema, and remove empty system rooms.

- Add authenticated, bounded discovery and engine-backed pairing of hubs and peers through nested registries.

- Speed up first-match searches that use an indexed equality requirement.

- Extend live registry trees across remote centrals with shared upstream subscriptions, reconnects, and loop protection.

- Add scoped live registry subscriptions for local hub trees.

- Add room-scoped WebRTC signalling and ephemeral coturn credentials in live welcomes.

- Enforce live channel schemas, rate and connection budgets, liveness, membership and reconnect presence.

- Add ticket-authenticated WebSocket room relay with presence and targeted delivery.

- Issue short-lived, signed, single-use live room tickets to paired users.

- Accept and validate channel declarations in hosted hub definitions and protect their referenced schemas from deletion.

- Roll back registry definition changes when writing the state file fails.

- Add a typed Rust hub specification for registration through the HTTP service path.

- Bound registry list and export responses, publish instance identities atomically, and prune expired bridge rate limits.

- Expose nested registry operations in the Rust client and CLI, document schemas and nesting, and support `LIVEENGINE_HUB_PATH` in the Gogh Strike bridge.

- Persist self-hosted registry definitions and refresh imported registries, marking missing entries stale.

- Add remote and local nested central routing, bounded tree discovery, loop detection, and inherited schemas.

- Add hub templates, idempotent creation, background expiry, and per-central child quotas for hosted hubs.

- Add a versioned schema registry and JSON Schema validation for hub scopes, users and servers, with schema transfer during registry imports.

- Release Gogh Strike matchmaking seats on menu return and page hide, and reclaim idle seats. Check API Origins and enforce the bridge body limit in bytes.

## 0.2.4 — 2026-11-04

- The release workflow now runs on every push to `main`. A version that is missing
  from npm or crates.io is built and published automatically; unchanged versions
  skip the build matrix and publication.
- Tag pushes still build the platform artifacts and create the GitHub release, while
  npm and crates.io publication remains idempotent.

## 0.2.3 — 2026-11-04

- Added native ping: an unauthenticated `/v1/ping` hub route, Rust `ping_hub`,
  `liveengine-hub ping`, and Node `HubClient.ping()` with samples, min/avg/max and jitter.
- Added the dependency-free user hub browser interface in `examples/user-hub/` with
  client connection, live room listing, matchmaking, demo rooms, and native ping rendering.
- Added Nitro and SvelteKit framework recipes and expanded the Next.js recipe with
  server listing, matchmaking, native ping, and a client page.
- Included the framework and user hub examples in the npm package.

## 0.2.2 — 2026-11-04

- Added the Mintlify developer documentation site in `docs-site/`, including the full
  Node and Rust API references, hub protocol reference, deployment guides, and a
  framework recipe section for Express, Fastify, NestJS, Next.js, Axum, and Actix Web.
- Added runnable framework examples in `examples/frameworks/` and
  `examples/axum-service.rs`, and linked the documentation from the README.
- Connected the repository to Mintlify so pushes to `main` deploy the docs.

## 0.2.1 — 2026-11-04

- Published 0.2.0 to npm and crates.io, then configured OIDC trusted publishers for
  `dixonSolutions/LiveEngine` and `release.yml` on both registries. This release is the
  first one published through those trusted publishers.
- Made the release workflow skip publication when the manifest version already exists,
  so a partially completed release can be retried safely.
- Included the Rust test suite in the Cargo package, removing the packaging warnings.

## 0.2.0 — 2026-11-04

- Replaced the C++ core and node-gyp addon with Rust and N-API, retaining the existing Node matching API.
- Added Cargo library/host packaging and npm native binaries, host executable and HTTP-only client.
- Added authenticated internal/external hub hosting, a live central registry and remote registry imports through a versioned DTO contract.
- Added custom scopes, optional scope validation, request/resource budgets, remote destination allowlists and independent remote credentials.
- Added standalone OCI/Docker hosting, embedding support, remote connectivity checks and an optional Gogh Strike matchmaking integration.
- Added GitHub Actions release plumbing for npm and crates.io trusted publishing.

## 0.1.0 — 2026-09-27

Initial release.

- C++17 core (`src/core`): user hub, server hub, requirement DSL (14 ops, all/any),
  bidirectional fit, capacity + `minMembers`, five ranking strategies, seq-ordered
  equality and numeric indexes with first-fit early exit, seeded random user generation,
  event queue.
- N-API binding (`src/addon`) via node-addon-api, `LiveEngine` EventEmitter wrapper,
  TypeScript typings.
- Math-game example appliance (`examples/math-game`): HTTP + SSE server, browser page, bots.
- Tests: standalone C++ core tests, JS binding tests, end-to-end game test.
