# LiveEngine

Rust matchmaking and hub hosting for Node.js and Rust services. LiveEngine hosts internal hubs inside your API, runs standalone central registries, and connects independently hosted remote hubs through one authenticated HTTP contract.

Define users, servers, custom scopes and requirements at runtime. Register internal or external hubs while the service is running; import a remote registry without rebuilding DTOs or redeploying. Each internal matchmaking hub owns an isolated engine. External hubs own their engine or custom implementation at their own address.

- **Cargo:** `liveengine`, with an embeddable Rust library and `liveengine-hub` host.
- **npm:** `@dixonsolutions/liveengine`, with a Rust N-API engine, TypeScript definitions, HTTP client and host command.
- **Containers:** one image runs a central registry, an internal hub host, or an independently hosted remote service.
- **Custom hubs:** implement the versioned protocol externally or embed a Rust handler in your API.

The former C++/node-gyp implementation has been replaced by Rust. The existing `LiveEngine` and `NativeEngine` Node interfaces remain available. Seeded sequences are deterministic within the Rust implementation; random sequences are not byte-for-byte compatible with the previous C++ generator.

## Documentation

The complete developer guide, protocol reference and framework recipes live at **[liveengine.mintlify.site](https://liveengine.mintlify.site)**.

Runnable framework examples are in the repository:

- [Express](examples/frameworks/express-server.js)
- [Fastify](examples/frameworks/fastify-server.mjs)
- [NestJS](examples/frameworks/nestjs)
- [Next.js](examples/frameworks/nextjs)
- [Nitro](examples/frameworks/nitro)
- [SvelteKit](examples/frameworks/sveltekit)
- [User hub interface](examples/user-hub)
- [Axum](examples/axum-service.rs)
- [Actix Web](docs-site/frameworks/actix.mdx) recipe

## Install

```sh
npm install @dixonsolutions/liveengine
```

Node.js 22 or newer is required. Release tarballs are built with native binaries for Linux x64/arm64 (glibc), macOS x64/arm64, and Windows x64. Source checkouts and other platforms build with Rust 1.88+ and the platform linker. The HTTP-only client is available without loading a native addon:

```js
const { HubClient } = require('@dixonsolutions/liveengine/hubs');
```

```sh
cargo add liveengine
cargo install liveengine --bin liveengine-hub
```

For local development:

```sh
npm ci                  # builds the Rust Node addon
npm test
cargo test --locked
npm run build:hub       # standalone Rust host
```

## Local matchmaking

```js
const { LiveEngine } = require('@dixonsolutions/liveengine');
const engine = new LiveEngine({ seed: 42 });
engine.on('paired', event => console.log(event));
engine.upsertServer({
  id: 'room-au', capacity: 2, minMembers: 2,
  scopes: { game: 'chess', region: 'au', rating: 1500 },
  requirements: { rating: { between: [1300, 1700] } },
});
engine.upsertUser({
  id: 'player', scopes: { rating: 1450 },
  requirements: { game: 'chess', region: 'au' },
});
console.log(engine.pair('player'));
```

Matches must satisfy requirements in both directions and fit available capacity. Pairing updates user membership, server availability and the event queue in one engine operation. `minMembers` emits `server_ready` when that threshold is reached.

Scopes accept numbers, booleans, strings, string arrays and null. Requirements accept shorthand, arrays of rules, or `{ rules, match: 'all' | 'any' }`:

```js
const requirements = {
  region: ['au', 'nz'],
  rating: { between: [1200, 1800] },
  tags: { contains: 'ranked' },
};
```

Operators: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `nin`, `between`, `exists`, `missing`, `contains`, `notContains`, `overlaps`. Strategies: `first` (FIFO), `random`, `leastLoaded`, `mostLoaded`, and `affinity`. `affinity: ['rating']` ranks numeric closeness. Match queries default to 10 results; `limit: 0` means unlimited.

The API includes user/server upsert, bulk upsert, get/list/remove/status operations, `fits`, `findServersForUser`, `findUsersForServer`, `join`, `leave`, `autoJoin`, `matchAll`, `advertise`, `advertiseAll`, `generateUsers`, `stats`, `clear`, `reseed`, and `drainEvents`. The `LiveEngine` wrapper adds `pair`, `joinHub`, `startAutoMatch` and `stopAutoMatch`. `HubClient` adds `contract`, `list`, `register`, `get`, `remove`, `call`, `importRegistry`, and `ping`. See [TypeScript definitions](index.d.ts) for complete signatures.

`autoEmit: false` retains events for manual draining. For network hubs, explicitly drain events as part of your service's event delivery loop. Registry definitions can persist with `LIVEENGINE_STATE_FILE`; live users, rooms and memberships remain in memory and are lost on restart. The state file contains credentials, is created mode 0600 and must be protected along with its backups.

## Hosted and remote hubs

Run a host with a strong secret:

```sh
export LIVEENGINE_TOKEN="$(openssl rand -hex 32)"
export LIVEENGINE_BIND=0.0.0.0:8787
cargo run --locked --release --bin liveengine-hub
# npm-installed command: liveengine-hub
```

`liveengine-hub` is the npm and Cargo executable name. It defaults to `127.0.0.1:8787`; choose a LAN or Tailscale interface for another device. For Internet deployment, terminate HTTPS at your ingress and configure network access deliberately.

A central registry and an ordinary hub host use the same runtime. A service may host several internal hubs and route to several external ones simultaneously:

```js
const { HubClient } = require('@dixonsolutions/liveengine/hubs');
const central = new HubClient('http://127.0.0.1:8787', {
  token: process.env.LIVEENGINE_TOKEN,
});
await central.contract();
await central.register({
  id: 'ranked-au', mode: 'internal',
  scopes: { game: 'chess', region: 'au' },
  scopeSchema: {
    type: 'object', required: ['region'],
    properties: { region: { type: 'string', enum: ['au', 'nz'] } },
  },
});
await central.call('ranked-au', 'upsertServer', [
  { id: 'room-1', capacity: 2, scopes: { region: 'au' } },
]);
await central.call('ranked-au', 'upsertUser', [
  { id: 'alice', scopes: { region: 'au' } },
]);
const result = await central.call('ranked-au', 'autoJoin', ['alice']);
```

An external registration attaches an existing hub by address and hub ID. The central host must explicitly allow that remote hostname through `LIVEENGINE_ALLOWED_REMOTE_HOSTS`. Remote credentials are separate from central credentials and are omitted from discovery responses.

```js
await central.register({
  id: 'remote-ranked', mode: 'external',
  address: 'https://remote.example.net',
  remoteHubId: 'ranked-au',
  remoteToken: process.env.REMOTE_HUB_TOKEN,
  scopes: { region: 'au' }, ttlSeconds: 3600,
});
await central.call('remote-ranked', 'upsertUser', [
  { id: 'bob', scopes: { region: 'au' } },
]);

// Discover the remote registry now and attach its advertised hubs.
await central.importRegistry({
  address: 'https://remote.example.net',
  token: process.env.REMOTE_HUB_TOKEN,
  prefix: 'partner-',
});
```

Remote imports are snapshots by default; optional `refreshSeconds` enables periodic refresh and marks missing entries stale. An external hub must implement `liveengine.hub/v1`, pass a health/contract handshake and expose its declared remote hub. Forwarded calls use the same `{ method, args }` request and `{ result }` response as internal calls. Custom external methods can use arbitrary JSON arguments and results within configured limits.

## Native ping

The host exposes an unauthenticated `GET /v1/ping` latency probe. Node and Rust clients can sample it without a third-party ping binary:

```js
const { HubClient } = require('@dixonsolutions/liveengine/hubs');
const hub = new HubClient('http://127.0.0.1:8787', { token: process.env.LIVEENGINE_TOKEN });
const ping = await hub.ping({ samples: 5 });
console.log(ping.avgMs, ping.minMs, ping.maxMs, ping.jitterMs);
```

```sh
liveengine-hub ping http://127.0.0.1:8787 --samples 5
liveengine-hub ping http://127.0.0.1:8787 --samples 5 --json
```

The Rust API exposes the same measurement as `liveengine::hub::ping_hub`. The browser interface in [`examples/user-hub`](examples/user-hub) renders live ping samples alongside the room list.

## Contract and DTOs

[Hub protocol schema](docs/hub-contract.schema.json) defines registrations, discovery, calls, registry imports and errors. Internal and external modes use the same DTOs; external mode additionally requires its address and credential. Hub IDs use ASCII letters, digits, dots, underscores and hyphens (1–128 characters); the path segments `.` and `..` are rejected.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Liveness and protocol |
| `GET /v1/contract` | Versioned capability handshake |
| `GET /v1/contract/schema` | JSON schema for the hub contract |
| `GET /v1/hubs` | Discover currently attached hubs |
| `POST /v1/hubs` | Register a dynamic internal or external hub |
| `GET /v1/hubs/{id}` | Inspect a hub |
| `DELETE /v1/hubs/{id}` | Remove a registration |
| `POST /v1/hubs/{id}/call` | Exchange `{ method, args }` / `{ result }` |
| `POST /v1/registries/import` | Discover and attach a remote registry |

Send `Authorization: Bearer <token>` for data endpoints. `scopeSchema` supports object properties with primitive types and enums, required properties and `additionalProperties: false`; it is not a complete JSON Schema validator. Define application DTOs freely within the JSON protocol, and validate application-specific operations in your custom handler.

## Embed in an existing Rust API

The Cargo library exposes the same host as an Axum router:

```rust,no_run
use liveengine::hub::{HubConfig, HubService};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hubs = HubService::new(HubConfig::from_env()?)?;
    let app = axum::Router::new().merge(hubs.router());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8787").await?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}
```

Implement `HubHandler::call` to host custom methods and application DTOs in the same process, then use `HubService::register_internal(registration, handler).await`. HTTP registrations create matchmaking hubs; application code supplies custom handlers. The [custom hub example](examples/custom-hub.rs) implements `increment` and `read` with its own result shape:

```sh
cargo run --example custom-hub
# Package the same custom service as an independent container:
docker build -f Dockerfile.custom -t liveengine-counter .
docker run --rm -p 8787:8787 -e LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" liveengine-counter
```

Pass real peer connection information so rate limits apply to the socket peer. Proxy headers do not establish client identity. When nesting the router below a prefix, advertise that prefix as part of the external base address.

For an engine-only Rust dependency, disable hosting dependencies with `liveengine = { version = "0.2", default-features = false }`. The core is `Engine::new(seed)` with `dispatch(method, Vec<serde_json::Value>)` and has no Node dependency.

## Docker and Podman

```sh
cp .env.example .env
# Set independent random secrets in .env.
docker compose up --build -d
# Podman: podman build -t liveengine:local .
```

The Compose example hosts a central registry at loopback port 8787 and a remote host on the private container network. Register an internal hub on `remote`, then attach it to `central` with `address: 'http://remote:8787'`. These are runtime registrations; no hub IDs are hard-coded in the image.

```sh
docker build -t liveengine:local .
docker run --rm -p 8787:8787 \
  -e LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" liveengine:local
bash scripts/test-containers.sh docker liveengine:local
```

Containers run as a non-root user. For a standalone custom hub binary, build your Rust application with the library and copy it into a minimal image using the same pattern as [Dockerfile](Dockerfile).

## Limits and security

Hosts require a token of at least 24 bytes. Keep administrative tokens in server code or secret storage, and expose application-specific routes to untrusted game clients. The Gogh Strike bridge demonstrates this separation.

Defaults include a remote hostname allowlist, disabled HTTP redirects, outbound timeouts, bounded HTTP bodies/responses, bounded hub/entity/event counts and a peer request budget. Exact allowlist entries authorize the destination host, including private addresses when explicitly listed; only list hosts you control. Behind a shared reverse proxy, the socket peer budget is shared by its clients. TTL registrations expire in memory and can be re-registered after expiry.

See [hosting configuration](docs/hosting.md) for environment settings and deployment details. Authentication is administrative for this initial protocol: there is no per-player authorization, tenant isolation policy or durable storage layer supplied by the host.

## Gogh Strike integration

The integration clones [petergpt/gogh-strike](https://github.com/petergpt/gogh-strike), adds an optional lobby, and sends matchmaking requests through a server bridge to the same internal/external hub contract. See [integration instructions](integrations/gogh-strike/README.md).

Gogh Strike's gameplay is locally simulated. The integration validates registration, remote connectivity and matchmaking; it does not add multiplayer state synchronization.

## Verification and releases

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
npm ci
npm test
npm run build:hub
npm pack --dry-run
cargo package --locked
```

CI runs Rust and Node tests on Linux, macOS and Windows, and an independent central/remote container test on Linux. The [release workflow](.github/workflows/release.yml) also runs on pushes to `main`: when the manifest version is missing from npm or crates.io, it builds native addons and host binaries, publishes npm with GitHub OIDC/provenance, and publishes Cargo with crates.io trusted publishing. Unchanged versions skip the build. Tag pushes build the platform artifacts and create the GitHub release. Registry-side trust must be configured before an OIDC release can succeed. Observed test evidence is recorded in [validation](docs/validation.md). See [publishing](docs/publishing.md) for the bootstrap and exact workflow identity.

MIT licensed. See [LICENSE](LICENSE).

### Nested hubs and schemas

Register a local or remote `central` hub and use `HubClient.route(["region"], "register", {id: "game", mode: "internal"})` to create a child. `tree(depth)` lists branches, including errors. Depth defaults to 8 and is capped at 16; repeated instance IDs return 508. Use `LIVEENGINE_INSTANCE_ID` for a stable unique service identity.

Publish immutable JSON Schema 2020-12 definitions with `createSchema`, bind them using `schemaRef`, or supply inline `scopeSchema`, `userSchema`, and `serverSchema`. Children inherit the nearest schemas. Templates support on-demand creation, `ifAbsent`, TTLs and quotas. See [hosting](docs/hosting.md). The CLI can access all authenticated routes with `liveengine-hub request <url> <method> <route> [json-body]`, reading its token from `LIVEENGINE_TOKEN`.
