# Changelog

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
