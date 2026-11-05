# Hub hosting

The `liveengine-hub` process owns its registry and all registered internal engines. An external registration routes to a remote process; removing that registration does not remove its remote hub. Live room and player state is in memory; registry definitions can optionally be persisted.

Set `LIVEENGINE_TOKEN` to an unpredictable secret of at least 24 bytes. Internal endpoints and remote endpoints share the `liveengine.hub/v1` protocol. Remote registrations require their own token; registry listing never returns credentials.

`GET /health` and `GET /v1/ping` are unauthenticated. Ping is a stateless latency probe: it returns protocol, server time, native processing time and uptime. Use `HubClient.ping()` or `liveengine-hub ping <url>` to sample it.

`LIVEENGINE_BIND` defaults to `127.0.0.1:8787`. For a LAN device, bind the appropriate interface and connect to the host's LAN address. For a different network, use a reachable HTTPS ingress or a private overlay such as Tailscale. A container test proves process/network isolation; it does not prove a physical device on another network connected.

`LIVEENGINE_ALLOWED_REMOTE_HOSTS` is a comma-separated allowlist for remote registrations and imports. Listing `remote` permits the Compose service hostname. Listing loopback/private hostnames is an explicit authorization to connect to them. Use exact hostnames you control. HTTP redirects are disabled; endpoint paths are appended to the registered base path.

Limits are configurable through `HubConfig` when embedding the router. The standalone host accepts:

| Environment variable | Default |
| --- | --- |
| `LIVEENGINE_MAX_HUBS` | 256 |
| `LIVEENGINE_MAX_DEPTH` | 8 (maximum 16) |
| `LIVEENGINE_INSTANCE_ID` | generated and persisted |
| `LIVEENGINE_STATE_FILE` | unset (in-memory registry) |
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

Hub registration accepts `schemaRef` instead of `scopeSchema`; `userSchema` and `serverSchema` optionally override the shared schema for the corresponding upserts. Registry imports transfer schema definitions and reject conflicting versions; `GET /v1/registry/export` includes definitions without credentials. Schema definitions are durable when a state file is configured.

## On-demand hubs

Create a template with `POST /v1/templates`, for example `{"name":"game","mode":"internal","ttlSeconds":60}`. List templates with `GET /v1/templates`, retrieve or delete one at `/v1/templates/{name}`. Template credentials are write-only. Register with `{"id":"match-1","template":"game","ifAbsent":true}`; supplied fields override template defaults. A repeated `ifAbsent` registration returns the existing hub without changing it. The server removes expired hubs in the background. Template and hub registries are each bounded by `LIVEENGINE_MAX_HUBS`.

Local central hubs accept `maxHubs` to limit their immediate children, including on-demand registrations. The service-wide `LIVEENGINE_MAX_HUBS` budget also counts central hubs and their descendants. Use `POST /v1/route` with `{ "path": ["central-id"], "op": "register", "body": { "id": "match-1", "template": "game" } }` to create a child from a service template. Nested paths are limited by `LIVEENGINE_MAX_DEPTH` (default 8, maximum 16).


## Nested central hubs

Register `mode: "central"` to create a local namespace. Supplying `address` and `remoteToken` makes the central a remote service. `POST /v1/route` accepts `path`, `op` (`get`, `list`, `register`, `delete`, `call`, or `tree`), and `body`; each path element selects a central. Calls use `body.id`, `body.method`, and `body.args`. Empty paths target the current service. `GET /v1/tree?depth=3` recursively lists branches, including an `error` status for unreachable services. Discovery is bounded to 1024 nodes and the configured global hub limit. Flat routes continue to show immediate children.

Schemas are inherited from the nearest central and can be overridden by a child. Remote routing carries inherited schemas to the destination. Each service has a stable identity used in the `x-liveengine-trace` forwarding header; loops return HTTP 508. Set `LIVEENGINE_INSTANCE_ID` to a unique identifier per service. Otherwise the standalone service stores a generated identity under the XDG state directory, keyed by bind address; `LIVEENGINE_INSTANCE_ID_FILE` selects the file explicitly, and `LIVEENGINE_STATE_FILE` stores the identity in the registry snapshot. Embedded `HubService` instances should set `HubConfig.instance_id` when identity must survive restarts.

## Durable registry

Set `LIVEENGINE_STATE_FILE` to a writable file path to persist hub definitions, nested local centrals, schemas, templates, imports and the instance ID. Snapshots are written atomically and synced to disk. The file contains remote credentials in plaintext: protect its directory and backups. LiveEngine creates the file with mode `0600` and refuses to start if an existing file is group- or world-accessible or corrupt. Live rooms and player state remain in memory and are lost on restart. Embedded custom Rust handlers must be registered again by the host after restart; their restored definitions return unavailable until the handler is reattached. Without a state file, registry definitions remain in memory.

`GET /v1/registry/export` returns hub and schema definitions without credentials. `POST /v1/registries/import` accepts an optional `refreshSeconds` (1–86400); the service periodically re-syncs the source, retains entries no longer present with `stale: true`, and persists the refresh configuration and credential. A failed refresh leaves the previous registry intact. If a background expiry or refresh cannot write the state file, its definition changes are rolled back and retried on the next sweep; background writes are best-effort.

The Compose example provides three separate services and persistent volumes. After starting it, register `remote` as a remote central on `central`, then register `leaf` as a remote central on `remote` using their respective credentials and service addresses `http://remote:8787` and `http://leaf:8787`. Create child hubs on `leaf` to form a three-tier nest. Each volume contains sensitive credentials and should be secured and backed up accordingly.

The Rust `HubClient` exposes `route`, `tree`, `export_registry`, and a general `request` method. Node `HubClient` provides schema and template operations as well as nested routing. The CLI accesses any authenticated endpoint using `liveengine-hub request <url> <method> <route> [json-body]`; set `LIVEENGINE_TOKEN` in its environment.

### Live room tickets

Set `LIVEENGINE_PUBLIC_URL` to the direct live endpoint advertised by this service and optionally set `LIVEENGINE_TICKET_TTL_SECONDS` (default 60). An administrator can request `POST /v1/hubs/{id}/tickets` with `userId`, `serverId`, and optional `role` (`peer` or `service`); the user must already be paired in that server. The response contains `ticket`, `url`, and the Unix-seconds `expiresAt`. For nested hubs, use the `ticket` route operation with the hub `id` in the body. The owning service issues the ticket. Tickets are single-use and bound to the hub path and room. The signing key is stored in the private state file; used-ticket tracking is in memory and resets on restart.

Live room connections use `GET /v1/live` as a WebSocket endpoint. Send the
single-use ticket in the first JSON text frame as `{"type":"auth","ticket":"..."}`
within five seconds; do not put it in the URL. Set
`LIVEENGINE_LIVE_ALLOWED_ORIGINS` to a comma-separated list of exact browser
origins. Requests without an Origin header are accepted; requests with one must
match the list. A welcome message lists current peers, and presence messages
announce joins and leaves. Relay messages contain `v`, `type`, `channel`,
`seq`, and `payload`, with optional `to`; the server supplies `from`.

The live relay bounds per-connection traffic with `LIVEENGINE_LIVE_MAX_FRAME_BYTES`
(default 65536), `LIVEENGINE_LIVE_MESSAGES_PER_SECOND` (100),
`LIVEENGINE_LIVE_BYTES_PER_SECOND` (1048576), and `LIVEENGINE_LIVE_SEND_QUEUE`
(64 messages). `LIVEENGINE_LIVE_MAX_CONNECTIONS` (4096) and
`LIVEENGINE_LIVE_MAX_CONNECTIONS_PER_ROOM` (256) cap sockets globally and authenticated sockets per room.
`LIVEENGINE_LIVE_PING_SECONDS` (20), `LIVEENGINE_LIVE_IDLE_SECONDS` (60), and
`LIVEENGINE_LIVE_RESUME_MILLIS` (500) control liveness and presence. Values must
be positive integers. A full queue drops lossy messages; reliable delivery
closes the slow consumer. Reconnection requires a new ticket.

For WebRTC negotiation, send `signal.offer`, `signal.answer`, and `signal.ice`
using the normal envelope with a `to` peer in the same room. These opaque,
reliable signalling frames bypass application channel schemas, but share the
connection's message and byte rate limits. An invalid or absent target closes
the sender. `LIVEENGINE_SIGNAL_MAX_BYTES` limits each complete signalling frame
(default 16384); the live frame limit also applies. The hub carries signalling,
not media, and does not terminate WebRTC.

Set `LIVEENGINE_ICE_SERVERS` to a JSON array of browser ICE server objects, for
example `[{"urls":"stun:stun.example.org"},{"urls":"turn:turn.example.org"}]`.
The authenticated welcome frame contains this array as `iceServers`. With
`LIVEENGINE_TURN_SECRET`, TURN entries receive per-connection coturn REST
credentials: a username beginning with a Unix expiry one hour in the future,
and a base64 HMAC-SHA1 password. Configure coturn with the same secret and keep
it private. Browsers behind strict NAT may need a reachable TURN server.

### Live local registry subscriptions

`GET /v1/live/registry` upgrades to a WebSocket. Send an authentication frame
`{"type":"auth","token":"<admin bearer>"}` or `{"type":"auth","ticket":"<watch ticket>"}`,
then `{"path":[],"depth":3}`. The first response is a `snapshot`; subsequent
`added`, `removed`, `updated`, and `status` frames have increasing `seq` values.
Send `{"type":"resnapshot"}` after detecting a gap. Frames contain hub IDs,
modes, status, room and member counts, and `childrenLive`; they omit addresses
and credentials. `POST /v1/tickets/watch` issues a short-lived, single-use
read-only ticket for `{path,depth}`; the `watchTicket` route operation issues
one relative to its central. Tickets must be sent in the first frame, not URLs.

`LIVEENGINE_LIVE_TREE_DEPTH` defaults to 3 and is capped by
`LIVEENGINE_MAX_DEPTH`. A local central can set `liveDepth` within the maximum.
The response reports the clamped `effectiveDepth`. Nodes at the boundary have
`childrenLive:false`; use `/v1/tree` to fetch beyond it. This release streams
local trees; remote centrals appear as unreachable branches in this stream.
