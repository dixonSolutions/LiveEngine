# Hub hosting

The `liveengine-hub` process owns its registry and all registered internal engines. An external registration routes to a remote process; removing that registration does not remove its remote hub. Runtime state is in memory and is intentionally not persisted by this version.

Set `LIVEENGINE_TOKEN` to an unpredictable secret of at least 24 bytes. Internal endpoints and remote endpoints share the `liveengine.hub/v1` protocol. Remote registrations require their own token; registry listing never returns credentials.

`GET /health` and `GET /v1/ping` are unauthenticated. Ping is a stateless latency probe: it returns protocol, server time, native processing time and uptime. Use `HubClient.ping()` or `liveengine-hub ping <url>` to sample it.

`LIVEENGINE_BIND` defaults to `127.0.0.1:8787`. For a LAN device, bind the appropriate interface and connect to the host's LAN address. For a different network, use a reachable HTTPS ingress or a private overlay such as Tailscale. A container test proves process/network isolation; it does not prove a physical device on another network connected.

`LIVEENGINE_ALLOWED_REMOTE_HOSTS` is a comma-separated allowlist for remote registrations and imports. Listing `remote` permits the Compose service hostname. Listing loopback/private hostnames is an explicit authorization to connect to them. Use exact hostnames you control. HTTP redirects are disabled; endpoint paths are appended to the registered base path.

Limits are configurable through `HubConfig` when embedding the router. The standalone host accepts:

| Environment variable | Default |
| --- | --- |
| `LIVEENGINE_MAX_HUBS` | 256 |
| `LIVEENGINE_MAX_BODY_BYTES` | 1048576 |
| `LIVEENGINE_MAX_RESPONSE_BYTES` | 4194304 |
| `LIVEENGINE_MAX_ARGS` | 256 |
| `LIVEENGINE_MAX_ENTITIES` | 100000 |
| `LIVEENGINE_MAX_EVENTS` | 100000 |
| `LIVEENGINE_MAX_GENERATE` | 10000 |
| `LIVEENGINE_MAX_INFLIGHT_CALLS` | 16 |
| `LIVEENGINE_RATE_LIMIT_REQUESTS` | 120 |
| `LIVEENGINE_RATE_LIMIT_WINDOW_SECONDS` | 60 |
| `LIVEENGINE_RATE_LIMIT_PEERS` | 4096 |
| `LIVEENGINE_SEED` | 0 |

Limits and windows must be positive. Internal handlers run on blocking workers so they do not stall the API runtime. Capacity remains occupied until the handler finishes, even if its HTTP request times out. A timeout does not roll back a handler that already started; reconcile state before retrying a mutation. Outbound timeout and maximum registration TTL can also be changed through Rust `HubConfig`. Request, response, hub, entity and event limits apply to the hosted service; direct local `Engine` use is controlled by the embedding application.

The host uses actual socket peers for request budgets. It ignores arbitrary `X-Forwarded-For` headers. A reverse proxy therefore shares its peer budget across clients; apply public-client limits at the proxy as well when required by your deployment.

Scope schemas use JSON Schema draft 2020-12. References must be local JSON pointers; remote references, schema IDs, anchor references and cycles are refused. Schemas are limited to 64 KiB, 32 levels and 2048 JSON nodes. A dynamically registered internal hub is a matchmaking engine. Custom internal handlers are compiled into the embedding Rust service, and custom external services implement the HTTP contract independently.

## Container connectivity check

```sh
docker build -t liveengine:local .
bash scripts/test-containers.sh docker liveengine:local
# Same OCI image/test with Podman:
bash scripts/test-containers.sh podman liveengine:local
```

The test starts separate central/remote containers with different secrets, performs the handshake and forwarding, imports the remote registry live, pairs a player and verifies unauthenticated access is denied. It removes its containers/network/secrets on exit.

## Different-network connectivity check

Run the host behind a reachable HTTPS address. Set a dedicated test token as the GitHub repository secret `LIVEENGINE_NETWORK_TEST_TOKEN`, then dispatch the **Cross-network connectivity** workflow with the address. The runner connects from GitHub's network, creates a short-lived isolated hub, registers a room and player, pairs them and cleans up. Remove the temporary test secret after the check.

For a device you control, the same test script works with:

```sh
LIVEENGINE_NETWORK_URL=https://your-hub.example \
LIVEENGINE_NETWORK_TEST_TOKEN="$LIVEENGINE_TOKEN" \
node scripts/test-network.js
```

Keep administrative credentials in the server-side bridge rather than browser game code. For production applications, implement player authentication and authorization in that application bridge.

The Compose example exposes central port 8787 on the chosen interface and remote management port 8788 only on loopback. It uses separate credentials from `.env`.

For hubs with a `scopeSchema`, user/server upserts are validated before dispatch. `generateUsers` is rejected for these hubs; generate locally and submit validated users with `upsertUsers`. The active peer budget table rejects new peers at capacity instead of evicting and resetting existing budgets.

## Schema registry

Authenticated `POST /v1/schemas` accepts `{ "id": "game", "version": "1", "schema": { "type": "object" } }`. `GET /v1/schemas` lists definitions, and `GET` or `DELETE /v1/schemas/game@1` retrieves or deletes one. Versions are immutable, and deletion fails while a hub references the version. `liveengine.default@1` is built in.

Hub registration accepts `schemaRef` instead of `scopeSchema`; `userSchema` and `serverSchema` optionally override the shared schema for the corresponding upserts. Registry imports transfer schema definitions and reject conflicting versions; `GET /v1/registry/export` includes definitions without credentials. Schema definitions remain in memory.
