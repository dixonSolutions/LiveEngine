# Hub hosting

The `liveengine-hub` process owns its registry and all registered internal engines. An external registration routes to a remote process; removing that registration does not remove its remote hub. Live room and player state is in memory; registry definitions can optionally be persisted.

Set `LIVEENGINE_TOKEN` to an unpredictable secret of at least 24 bytes. Internal endpoints and remote endpoints share the `liveengine.hub/v1` protocol. Remote registrations require their own token; registry listing never returns credentials.

`GET /health` and `GET /v1/ping` are unauthenticated. Ping is a stateless latency probe: it returns protocol, server time, native processing time and uptime. Use `HubClient.ping()` or `liveengine-hub ping <url>` to sample it.

`LIVEENGINE_BIND` defaults to `127.0.0.1:8787`. For a LAN device, bind the appropriate interface and connect to the host's LAN address. For a different network, use a reachable HTTPS ingress or a private overlay such as Tailscale. A container test proves process/network isolation; it does not prove a physical device on another network connected.

`LIVEENGINE_ALLOWED_REMOTE_HOSTS` is a comma-separated allowlist for remote registrations and imports. Listing `remote` permits the Compose service hostname. Listing loopback/private hostnames is an explicit authorization to connect to them. Wildcards such as `*.example.com` allow subdomains; use hostnames you control. HTTP redirects are disabled; endpoint paths are appended to the registered base path.

Limits are configurable through `HubConfig` when embedding the router. The standalone host accepts:

| Environment variable | Default |
| --- | --- |
| `LIVEENGINE_MAX_HUBS` | 256 |
| `LIVEENGINE_MAX_DEPTH` | 8 (maximum 16) |
| `LIVEENGINE_INSTANCE_ID` | generated and persisted; in memory with a warning if the default location is not writable |
| `LIVEENGINE_STATE_FILE` | unset (in-memory registry) |
| `LIVEENGINE_MAX_BODY_BYTES` | 1048576 |
| `LIVEENGINE_MAX_RESPONSE_BYTES` | 4194304 |
| `LIVEENGINE_MAX_ARGS` | 256 |
| `LIVEENGINE_MAX_ENTITIES` | 100000 |
| `LIVEENGINE_MAX_EVENTS` | 100000 |
| `LIVEENGINE_MAX_GENERATE` | 10000 |
| `LIVEENGINE_MAX_INFLIGHT_CALLS` | 16 |
| `LIVEENGINE_CALL_QUEUE_MS` | 1000 |
| `LIVEENGINE_MAX_QUEUED_CALLS` | 1024 |
| `LIVEENGINE_MAX_PAIR_WAIT_MS` | 30000 |
| `LIVEENGINE_MAX_PENDING_PAIRS` | 1024 per namespace |
| `LIVEENGINE_LIVE_REGISTRY_MAX_SUBSCRIBERS` | 256 |
| `LIVEENGINE_RATE_LIMIT_REQUESTS` | 120 |
| `LIVEENGINE_RATE_LIMIT_WINDOW_SECONDS` | 60 |
| `LIVEENGINE_RATE_LIMIT_PEERS` | 4096 |
| `LIVEENGINE_SEED` | 0 |

Limits and windows must be positive; malformed or zero values cause startup to fail. Live and link limits are capped at 16777216. Internal handlers run on blocking workers so they do not stall the API runtime. Capacity remains occupied until the handler finishes, even if its HTTP request times out. A timeout does not roll back a handler that already started; reconcile state before retrying a mutation. Outbound timeout and maximum registration TTL can also be changed through Rust `HubConfig`. Request, response, hub, entity and event limits apply to the hosted service; direct local `Engine` use is controlled by the embedding application.

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

Local central hubs accept `maxHubs` to limit their immediate children, including on-demand registrations; on provider-hosted hubs it limits all descendants. The service-wide `LIVEENGINE_MAX_HUBS` budget also counts central hubs and their descendants. Use `POST /v1/route` with `{ "path": ["central-id"], "op": "register", "body": { "id": "match-1", "template": "game" } }` to create a child from a service template. Nested paths are limited by `LIVEENGINE_MAX_DEPTH` (default 8, maximum 16).


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

### Pairing a named target

`POST /v1/pair` takes `target: {kind, requirements?, id?, capacity?}`. Capacity defaults to 2 (a plain pair is a pair), and must be an integer from 2 to `LIVEENGINE_MAX_ROOM_CAPACITY` (default 64). Full peer rooms are excluded; later peers join another room or wait. `createHubApi` passes its `roomCapacity` option (default 2), and the Gogh Strike bridge sets it from `GOGH_MATCH_CAPACITY` (default 12). Without `id`, the first candidate in id
order that matches `requirements` is used. With `id`, only the hub or peer of that id can match, so
two hubs that share scopes stay distinct. A non-string or empty `id` answers `400`; an `id` that
matches nothing answers `404`.

### Waiting for a partner

`POST /v1/pair` and the `pair` route operation accept `waitMs`, for a peer subject with a `peer` or
`any` target. A peer with no candidate yet stays queued, per namespace, and the next peer that pairs with it
wakes it with its own ticket; nothing polls. The wait ends with `408` after `waitMs`, capped by
`LIVEENGINE_MAX_PAIR_WAIT_MS` (`0` turns waiting off), and a request its caller drops leaves the queue at
once. A peer is queued once (`409` for a second request), at most `LIVEENGINE_MAX_PENDING_PAIRS` wait at a
time (`503` beyond that), and a match removes both peers from the queue in one step. The waiting peer must
already be a user of a hub, so that other peers can find it. A waiting `pair` gets the request timeout plus
its own wait, also when forwarded through a nest; every other route keeps the plain request timeout. A
waiting peer is known by its id and scopes, so one id can wait in two hubs of a service.

Calls to a hub queue in arrival order, for at most `LIVEENGINE_CALL_QUEUE_MS`, for the hub's turn and for one
of `LIVEENGINE_MAX_INFLIGHT_CALLS` workers; at most `LIVEENGINE_MAX_QUEUED_CALLS` calls queue at once. Past
that a call is refused as before (`409` handler busy, `503` capacity busy).

### Hub auth

A hub may carry `auth`, a verifier that resolves the credential a user presents
into an identity plus user-defined claims before any ticket is issued. Core never
interprets claims. Add `credential` to the ticket request (or to `subject.credential`
in `POST /v1/pair`, which checks it before changing membership). The verified
identity must equal `userId`, so a credential cannot mint a ticket for someone
else. Every failure returns the same `403 credential refused`. Hubs without `auth`
behave as before. Link credentials, which an administrator issues for a hub, are
not user credentials and skip this check. `auth` is not available on central hubs.

| `kind` | Definition | Credential |
| --- | --- | --- |
| `password` | `roles?` containing `user` and/or `admin` (default both) | an enabled account’s session token from the hub-owning service |
| `token` | `tokens: [{token, identity?, claims?}]`, each token 24 to 512 bytes | the token, compared in constant time |
| `hmac` | `secret` (32 to 512 bytes), `leewaySeconds?` | `base64url(payload JSON).base64url(HMAC-SHA256(secret, first segment))`; payload has `sub`, a required `exp` in Unix seconds, and any claims |
| `jwt` | `alg` of `HS256` (with `secret`) or `EdDSA` (with `publicKey`, 32 raw bytes as unpadded base64url); `issuer?`, `audience?`, `identityClaim?` (default `sub`), `leewaySeconds?` | a compact JWT with a required `exp` |
| `webhook` | `url`, `secret?` (sent as a bearer token), `timeoutMs?` (100 to 10000, default 3000) | opaque; forwarded to the webhook |
| `custom` | `name` of a verifier registered with `HubService::register_verifier` | opaque; given to the verifier |

For password hubs, `userId` must exactly match the account username. Claims include the verified account role; this role replaces any user-defined `role` claim. A peer pairing needs each peer’s session: each waiting peer supplies `subject.credential`, or a direct request supplies the other peer’s session as `target.credential`. Sessions are checked by the service that owns the hub, including through external hubs and remote centrals. Accounts and sessions are never exported.

The JWT algorithm comes only from the hub definition. A token whose `alg` header
differs, including `none`, is refused, and no other algorithm can be configured.
A webhook receives a POST of `{hub, path, userId, serverId, role, credential}` and must
answer `200` with `{"allow":true,"identity"?,"claims"?}` within the timeout. Redirects
are not followed, its host must be in `LIVEENGINE_ALLOWED_REMOTE_HOSTS`, and any other
status, body, timeout or error refuses the ticket. A compiled verifier implements
`liveengine::auth::AuthVerifier` and is registered on the service before the hubs that
name it are created; nested centrals share the registry. Secrets are persisted in the
state file like remote tokens and are never returned by any route or the export;
`GET /v1/hubs/{id}` shows only `auth: {"kind": ...}`.

A hub registered with `HubSpec::auth` (through `HubService::register` or the HTTP API) is
the only way to set `auth`. `HubRegistration`, and so `register_internal`, has no `auth`
field: a compiled handler added with `register_internal` accepts tickets without a
credential. The verified identity and claims travel with the connection: they are signed
into the ticket, and the welcome frame carries `identity` (`{identity, claims}`) for the
connecting peer and `peerIdentities` (keyed by peer id) for peers on the same hub, while a
`presence` join carries `identity` for the peer that joined. Claims stay opaque and are
limited to 2048 bytes serialized; a credential that resolves to more is refused with `413`.
Identities are not carried across hub links, and a ticket for a hub forwarded to another
service carries the identity only when that service verified the credential.
`POST /v1/pair` checks the credential once: the `authorize` route operation returns a
short-lived, single-use `receipt` bound to hub, room, user and role, and the ticket step
accepts it as `verified` instead of the credential, so a single-use credential works.

### Provider-hosted hubs

`POST /v1/hubs` (or the `register` route operation on a central) with `"hosting":"provider"`
and `"mode":"central"` runs the new hub on this service and returns a hub-scoped credential
once, as `credential`. Only its SHA-256 digest is stored, in the state file. In Rust use
`HubService::register_hosted`. The credential manages only that hub and its subtree, and
is default deny:

- It is accepted on `POST /v1/route` and nowhere else. Schemas, templates, links, export,
  import, `/v1/tree`, `/v1/hubs`, `/v1/hubs/{id}` and every other route answer `401`.
- The request `path` must start with the owning hub's full path. A sibling, a parent and the
  service root are refused. Path segments are matched exactly against hub ids, never
  normalised. The owner is found by walking local centrals only, so a credential is never
  forwarded to a remote central before it has matched inside the subtree.
- Only these operations run: `discover`, `pair`, `tree`, `register`, `list`, `get`,
  `delete`, `watchTicket`, `ticket`, `authorize`, `authPolicy` and `call`. Any other operation, a
  `register` that names a `template`, and the forwarding fields `inherited_schemas` and
  `inherited_channels` answer `403`.
- Digests are compared in constant time. Deleting the hub ends the credential; a hub
  created again under the same id gets a new one. A hosted hub's holder can create nested
  hosted hubs, whose credentials are narrower.

By default, a scoped credential can register internal hubs and local centrals, but external hubs,
remote centrals and webhook auth return `403`. An administrator can opt in at provider-hosted
creation with `"hostedPolicy":{"allowRemote":true}`. The policy persists and applies to the
subtree; scoped credentials cannot set it, and an administrator cannot widen a nested policy
beyond its parent. Nested hosted hubs inherit the parent policy when omitted. Outbound hosts
remain subject to the service allowlist. A provider-hosted `maxHubs` counts every descendant,
including nested centrals; nested limits cannot increase an ancestor's quota.

Live transport sits behind `liveengine::transport::LiveTransport`; the built-in one is the
axum WebSocket.

Live room connections use `GET /v1/live` as a WebSocket endpoint. Send the
single-use ticket in the first JSON text frame as `{"type":"auth","ticket":"..."}`
within five seconds; do not put it in the URL. Set
`LIVEENGINE_LIVE_ALLOWED_ORIGINS` to a comma-separated list of exact browser
origins. Requests without an Origin header are accepted; requests with one must
match the list. A welcome message lists current peers, and presence messages
announce joins and leaves. Relay messages contain `v`, `type`, `channel`,
`seq`, and `payload`, with optional `to`; the server supplies `from`.
Application frame types are user-defined except the reserved server types `presence`,
`welcome`, `error`, and `auth`, and the `link.` and `signal.` prefixes. The
three signalling types described below are permitted as targeted peer frames.
Linked frames are accepted only when their sender was announced on that link.

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

### Live registry subscriptions

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
`childrenLive:false`; use `/v1/tree` to fetch beyond it. Remote centrals share
upstream subscriptions for matching destinations and forwarding traces. Lost
connections mark their branch unreachable and reconnect with backoff while the
rest of the tree remains live. Forwarding traces reject loops.

### Service links

An administrator can create a persistent outbound link with `POST /v1/links`:
`{"id":"link-a","address":"https://remote.example","remoteToken":"...","hubId":"game","userId":"service-user","serverId":"room"}`.
`remoteToken` is a link credential, never the remote administrator token. The
remote administrator creates one with `POST /v1/hubs/{id}/link-credentials`,
which returns `{"token":"..."}` once; only its SHA-256 hash is kept, in the
remote state file. The credential is accepted only as the bearer for
`POST /v1/hubs/{id}/tickets` on that hub and only for role `service`
(role `peer` is refused with 403). Every other route still needs the
administrator token. `DELETE /v1/hubs/{id}/link-credentials` revokes every
credential for the hub. The service user must already be paired in the remote room. The remote host must
be on `LIVEENGINE_ALLOWED_REMOTE_HOSTS`. The initiating service requests a new
service-role ticket over the authenticated HTTP API for each connection, then
sends only that ticket in the WebSocket authentication frame. Use HTTPS for
remote connections. `GET /v1/links` and `GET /v1/links/{id}` report
`connecting`, `connected`, or `unreachable`; `DELETE /v1/links/{id}` or
`DELETE /v1/links?id=link-a` removes the definition. Reads omit the remote
address and credential. Definitions and credentials are stored in the private
state file, while connection status is rebuilt on startup. Links retry with
bounded exponential backoff and enforce per-link frame and inbound rate limits.
A link bridges room messages when both `localHubId` and `localRoom` are supplied.

Adding `localHubId` and `localRoom` bridges that local room to the remote room
over the same socket. Frames from peers on either side reach the other side
(targeted and `signal.*` frames go to the named peer), and presence is merged:
a linked peer appears as a `presence` join and in the welcome list. Bridged
frames are `link.frame` and `link.presence` messages with a `trace` of instance
ids; only role `service` sockets may send them, a frame whose trace holds the
receiving instance is dropped, and each side enforces its own channel and
schema rules, so a ring of links delivers each frame once. When a link drops,
peers known through it get a `presence` event `unreachable`, local peers keep
working, and presence is resent on reconnect. A link without `localHubId`
stays a plain connection. Each bridged frame carries a unique `id` stamped by the hub it entered; forwarding hubs keep it and drop repeats. A link socket has its own budget, `LIVEENGINE_LINK_MAX_MESSAGES_PER_SECOND` (default 2000) and `LIVEENGINE_LINK_MAX_BYTES_PER_SECOND` (default 16777216); over the message budget lossy frames are dropped and the link stays up. When a link drops, the remaining links are asked to announce presence again, so peers reachable another way reappear. `GET /v1/links` reports dropped and rejected frame
counts under `frames`.

The browser hub API returns a private `seatToken` when `pair` or legacy `join` seats a player. Repeat pair, renew, heartbeat and leave require it in the request body; missing or invalid tokens receive 403. The API stores only its SHA-256 hash. `connectLive` keeps the token internally and supplies `heartbeat()` and `leave()` methods. Abort a pending pair request to cancel it.

### Local accounts and sessions

The service token can create accounts with `POST /v1/accounts` using `username`,
`password`, `role` (`admin` or `user`), and optional `enabled` and `claims`.
Account names are case-insensitively unique. Administrators can list accounts
and get, patch, or delete `/v1/accounts/{username}`. A patch accepts `role`,
`enabled`, `claims`, and `password` for a reset. Passwords must contain 10–1024
bytes; account names contain 3–64 ASCII letters, digits, `.`, `_`, or `-`.

`POST /v1/sessions` accepts `username` and `password` and returns a bearer token,
account and Unix `expiresAt`. Use it with `GET /v1/sessions/me`,
`DELETE /v1/sessions`, and `POST /v1/accounts/me/password` (`current`, `next`).
A password change ends every session for that account. Disabling, deleting,
demoting, or resetting a password also ends its sessions. User sessions can
only read their own account, change their password and log out. Admin sessions
can administer the service. Keep session tokens secret.

`LIVEENGINE_PASSWORD_ITERATIONS` defaults to 310000 (minimum 100000),
`LIVEENGINE_SESSION_TTL_SECONDS` defaults to 43200, and
`LIVEENGINE_LOGIN_MAX_FAILURES` defaults to 5 in 15 minutes. Failed logins
are subject to per-name and per-address backoff; an address may hold ten times a
name's budget, because many people sign in through one hub API address. With `LIVEENGINE_STATE_FILE`,
accounts and hashed session tokens persist in the protected state file; they
are never included in registry exports.

### Managing live connections

An administrator can `GET /v1/connections` to list live sockets. Optional `hub`
(a slash-separated hub path), `room`, and `user` query parameters filter the
list. Each entry contains an opaque `id`, `hub` path, `room`, `user`, live
`role`, `transport`, Unix-seconds `connectedAt`, and `remote` flag. Linked
room peers are marked remote; credentials and network addresses are omitted.
`DELETE /v1/connections/{id}` sends the peer a `kicked` error and closes its
socket. A new ticket permits it to reconnect. Disabling an account ends its
sessions and closes its password-authenticated sockets with `banned`;
re-enabling it permits a fresh login. Linked socket termination is performed by
the service that owns the socket.

### Browser accounts and administration

`createHubApi({ ..., adminConsole: true })` enables an account and connection console at the API's `/admin` path. `POST /login`, `GET /me`, and `POST /logout` manage a browser session. Login sets an HttpOnly, SameSite=Strict cookie with an opaque identifier; the hub session remains on the API server. The cookie is Secure when served over HTTPS. An explicit hub session bearer token is also accepted. The session is passed to password-authenticated pair and renew operations. Browser sessions are held in memory and require a new sign-in after an API restart. Configure allowed origins and serve over HTTPS.
