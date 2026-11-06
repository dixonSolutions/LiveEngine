# Gogh Strike LiveEngine bridge

This integration connects the browser game to LiveEngine for real multiplayer. The browser talks only to the bridge; `LIVEENGINE_TOKEN` and the optional remote credential stay in the Node process. The lobby's **Find a LiveEngine match** option pairs the player with the bridge's hub through `HubClient.pair`, and the game then connects with `LiveClient`. Every browser in the room sees the others move and fire: each client keeps its own simulation (and its own bots), sends its player's state at 20 Hz on a lossy channel and its shots, hits and deaths on a reliable channel, and draws each remote player on an existing actor slot.

Responsibilities are split so the game holds no connection logic:

- `bridge.mjs` holds the administrator token. On first use it registers the game's schemas (`gogh-strike.state@1`, `gogh-strike.event@1`) and the hub's `state` (lossy) and `event` (reliable) channels, plus `auth` when `GOGH_HUB_AUTH` is set. It exposes thin routes over `HubClient.discover`, `pair` and `renewal`, and serves `lib/live-client.mjs` to the browser at `/liveengine/live-client.mjs`.
- The game's `src/liveengine-net.js` holds only the DTO and schema definitions, the channel table and the mapping to and from game state (plus a small interpolation of remote positions). The bridge imports its `LIVE_SCHEMAS` and `LIVE_CHANNELS` from the game checkout, so the game stays the one owner of its data. If the file is absent the bridge still serves the older seat-based `join` route but registers no schemas or channels.
- Pairing, tickets, retry with backoff, a fresh ticket on every reconnect and WebRTC with relay fallback live in `lib/live-client.mjs` (`connectLive`, `LiveClient`).

A remote player takes over the bot that holds the same team and role (or any free teammate); the bots then leave that actor alone. Bots on each client are local and do not hurt remote players: damage between players comes only from the shooter's `hit` event. Scores are tallied per client.

The upstream checkout used here is `https://github.com/petergpt/gogh-strike` at commit `2285e640097d9b12bcc42af72d5e8d0b37082f51`, cloned beside this repository at `../gogh-strike`. The game changes are captured in `gogh-strike.patch`. Apply it once to that clean upstream checkout; the command below also accepts a checkout where the patch is already applied:

```sh
patch="$PWD/integrations/gogh-strike/gogh-strike.patch"
if git -C ../gogh-strike apply --check "$patch"; then
  git -C ../gogh-strike apply "$patch"
else
  git -C ../gogh-strike apply --reverse --check "$patch"
fi
```

Run the game through the bridge from the LiveEngine repository root:

```sh
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN=your-local-token \
LIVEENGINE_HUB_ID=gogh-strike \
GOGH_STRIKE_ROOT=../gogh-strike \
node integrations/gogh-strike/bridge.mjs
```

Open `http://127.0.0.1:8967/` in two browsers (or two tabs: each tab gets its own player id), select **Find a LiveEngine match** in both, then press **Play**. The bridge checks `/v1/contract`, publishes the schemas, creates the configured internal hub with its channels if it is missing, and pairs each player into the hub's shared room (`pair:hub:<hub id>`, up to 64 players). Pairing targets the hub by id, so another hub with the same scopes cannot be matched by mistake. The hub must advertise a WebSocket address the browser can reach (`LIVEENGINE_PUBLIC_URL`) and accept the page's origin (`LIVEENGINE_LIVE_ALLOWED_ORIGINS`). Browsers behind strict NAT need TURN for WebRTC; relayed play works without it.

Bridge routes, all `POST` with an allowed Origin and JSON bodies:

| Route | Purpose |
| --- | --- |
| `/api/liveengine/pair` | `{userId, team, role, credential?}`: returns the descriptor `{link, room, userId, ticket, url, transports}` |
| `/api/liveengine/renew` | `{userId, credential?}`: a fresh descriptor for a player who paired; `404` for anyone else |
| `/api/liveengine/discover` | `{kind?, depth?, requirements?}`: candidates under the configured hub path, without scopes or secrets |
| `/api/liveengine/join`, `/leave`, `/heartbeat` | The seat routes: `leave` and `heartbeat` apply to paired players too; `join` is the older matchmaking-only call the game no longer uses |

A hub `4xx` answer (for example `403 credential refused`) reaches the browser with its status. Credentials are never stored by the bridge: a player pairing again presents it again. `GOGH_REQUEST_LIMIT` (default 10 per minute per client address, shared by all routes) can be raised for a LAN. Set `GOGH_HUB_AUTH` to the JSON of a hub `auth` verifier (see `docs/hosting.md`) to require a credential; the game reads one from `localStorage['gogh-strike-liveengine-credential']`. The hub is registered with its channels and `auth` only when the bridge creates it; an existing hub keeps its own settings, and external hubs own theirs.

Set `LIVEENGINE_HUB_PATH=region/games` to register and call the configured hub under those central IDs. Omit it for the root registry.

To target an external hub, set `LIVEENGINE_HUB_MODE=external`, `LIVEENGINE_REMOTE_ADDRESS`, `LIVEENGINE_REMOTE_HUB_ID`, and a separate `LIVEENGINE_REMOTE_TOKEN` (at least 24 characters). The bridge registers that external hub on the local service and passes the remote credential only in that server-side registration. The service must allow the remote host and use matching credentials on the remote service.

The bridge validates the Host header on every request and checks Origin on API requests when present; matchmaking mutations require an allowed Origin. It limits request bodies to 16 KiB and rate-limits matchmaking mutations. The game releases its seat on return to the menu or page hide and sends periodic heartbeats; the bridge reclaims seats after three minutes without a heartbeat. Restarting the bridge loses its in-memory seat tracking. It has no browser-facing token. The upstream game's own `npm start` does not include the proxy; use this bridge command so credentials are not placed in browser storage or URLs. Outbound service calls use `HubClient` with its five-second request timeout.

## Hosting on a network

By default the bridge binds to `127.0.0.1`. To serve the game on a trusted LAN, bind the LAN address and keep the Host/Origin allowlist narrow:

```sh
HOST=192.168.1.10 PORT=8967 \
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN=your-local-token \
LIVEENGINE_HUB_ID=gogh-strike \
GOGH_STRIKE_ROOT=../gogh-strike \
node integrations/gogh-strike/bridge.mjs
```

When `HOST` is a concrete address, `HOST:PORT` is allowed automatically alongside loopback. If you bind `0.0.0.0` or want multiple addresses, list them explicitly:

```sh
HOST=0.0.0.0 \
GOGH_ALLOWED_HOSTS=192.168.1.10:8967 \
GOGH_ALLOWED_ORIGINS=https://gogh.example.net \
node integrations/gogh-strike/bridge.mjs
```

The bridge has no player authentication. Expose it only on a trusted LAN, or put your own authenticated proxy in front of it. Do not publish it directly to the Internet.

Run the bridge tests without installing dependencies:

```sh
npm run test:integration
```

`bridge.test.mjs` uses a local fake HTTP hub and verifies the contract handshake, internal and external registration, matchmaking call sequence, and rejection of hostile Host and Origin headers. `bridge-live.test.mjs` starts a real `liveengine-hub` (`npm run build:hub` first) and real WebSockets and checks schema and channel registration, serving of `LiveClient`, two players pairing into one room and exchanging both channels, schema enforcement by the hub, renewal and reconnect, discovery, hub auth (good, missing, wrong and mismatched credentials) and that the administrator token never appears in a response. The game side is covered in the Gogh Strike repository by `tests/liveengine-net.test.js`.

## Service validation

`e2e.mjs` is an opt-in, replayable check against running LiveEngine HTTP services. It reads `LIVEENGINE_TOKEN` or `LIVEENGINE_TOKEN_FILE`. For an external-hub run, it also reads `LIVEENGINE_REMOTE_TOKEN` or `LIVEENGINE_REMOTE_TOKEN_FILE`, creates a unique internal hub on the remote service, registers an external alias on the central service, performs a real browser-bridge join, checks the remote user's paired state, and removes test hubs afterward. Hub IDs and room IDs are unique unless explicitly set through `LIVEENGINE_HUB_ID`, `LIVEENGINE_REMOTE_HUB_ID`, and `GOGH_MATCH_SERVER_ID`.

Example with the central and remote token files supplied by a local test session:

```sh
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN_FILE=/path/to/central-token \
LIVEENGINE_HUB_MODE=external \
LIVEENGINE_REMOTE_URL=http://127.0.0.1:8788 \
LIVEENGINE_REMOTE_TOKEN_FILE=/path/to/remote-token \
node integrations/gogh-strike/e2e.mjs
```

For the Compose services, set `LIVEENGINE_REMOTE_ADDRESS=http://remote:8787` as well: the test client reaches the remote through loopback port 8788, while the central container forwards through the container hostname.

For optional headless browser coverage, set `PLAYWRIGHT_PATH` to a Playwright `index.mjs` and optionally set `CHROMIUM_PATH` to the browser executable. The runner checks the lobby checkbox, starts a match, verifies the LiveEngine toast and browser user pairing, then saves `/tmp/liveengine-gogh-lobby.png` (override with `GOGH_SCREENSHOT`). It does not install Playwright into either project.

The lobby generates client IDs with `crypto.getRandomValues`, so matchmaking also works over plain HTTP on a LAN where `crypto.randomUUID` is unavailable. The id is kept per tab in `sessionStorage`.

Recorded for the earlier matchmaking-only version, on upstream commit `2285e640097d9b12bcc42af72d5e8d0b37082f51`. The multiplayer version is covered by the Node tests above and a scripted pairing against a real hub; it has not yet been run in two real browsers. `npm test` passed all 168 upstream tests, `npm run build` passed, and `node --test integrations/gogh-strike/bridge.test.mjs` passed. The live external-hub run reported `service join ok`, and optional Playwright reported `phase playing` with zero page errors; the screenshot was saved to `/tmp/liveengine-gogh-lobby.png`. A wrong local bearer returned HTTP 401 with a generic message and no token reflection. The test ran against local central and remote HTTP services, not a remote device.
