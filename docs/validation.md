# Validation record

Validation performed on 2026-11-04. This records observed checks; registry publication and trust configuration are separate account operations.

- Rust core replacement: existing Node engine and math-game regression suite passes. Core tests also cover FIFO ordering, rule coercion/missing scopes, invalid generators and membership invariants.
- Hub tests: authentication, custom embedded handler calls, scope validation, pre-mutation event budgets, remote credentials and redaction, live registry import, TTL and peer request limits.
- OCI container integration: separate central/remote containers with independent tokens successfully handshake, forward player matchmaking and import a live registry; unauthenticated access is denied. Executed locally with Podman using the Dockerfile; Docker execution is also configured in CI.
- Different-network test: [GitHub Actions run 41788025360](https://github.com/dixonSolutions/LiveEngine/actions/runs/41788025360) succeeded. A GitHub-hosted runner connected over HTTPS to the local service through a temporary Cloudflare tunnel, created a hub/room/player, paired the player, and deleted the test hub. The tunnel was stopped and its temporary repository secret removed after the check.
- Native ping: `/v1/ping` is unauthenticated and reports native processing time. Rust `ping_hub`, the `liveengine-hub ping` CLI, and the Node `HubClient.ping()` sampler all pass against a live hub.
- User hub interface: the dependency-free browser UI lists rooms, joins a match, leaves, creates demo rooms, and renders live native ping samples. CI covers the underlying endpoints; the interface was exercised against a local hub with a headless browser.
- Gogh Strike: cloned upstream, applied optional hub matchmaking lobby, passed its 168 upstream tests and build. A real central-to-remote join and headless browser play flow passed without page errors. The lobby uses `crypto.getRandomValues` for client IDs, so the same flow passes over plain HTTP on a LAN where `crypto.randomUUID` is unavailable. The game remains locally simulated; this check does not claim multiplayer game-state replication.

Other devices on the user's Tailscale network were offline during inspection. A physical tablet/phone test has not been performed. The successful external runner test proves connectivity from a separate network.
