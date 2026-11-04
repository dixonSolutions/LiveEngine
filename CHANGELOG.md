# Changelog

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
