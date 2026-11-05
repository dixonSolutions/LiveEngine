# Changelog

## Unreleased

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
