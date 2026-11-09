# Gogh Strike LiveEngine bridge

This integration connects the browser game to LiveEngine for real multiplayer. The browser talks only to the bridge; `LIVEENGINE_TOKEN` and the optional remote credential stay in the Node process. The lobby's **Find a LiveEngine match** option pairs the player with the bridge's hub through `HubClient.pair`, and the game then connects with `LiveClient`. Every browser in the room sees the others move and fire: each client keeps its own simulation (and its own bots), sends its player's state at 20 Hz on a lossy channel and its shots, hits and deaths on a reliable channel, and draws each remote player on an existing actor slot.

Responsibilities are split so the game holds no connection logic:

- `bridge.mjs` serves the game and mounts `lib/hub-api.mjs` (the generic hub API, which holds the administrator token). On first use it registers the game's schemas (`gogh-strike.state@1`, `gogh-strike.event@1`) and the hub's `state` (lossy) and `event` (reliable) channels, plus `auth` when `GOGH_HUB_AUTH` is set. It exposes thin routes over `HubClient.discover`, `pair` and `renewal`, and serves `lib/live-client.mjs` to the browser at `/liveengine/live-client.mjs`.
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
| `/api/liveengine/watch` | `{depth?}`: `{url, ticket, expiresAt, path, depth}`, a read-only registry watch ticket for the hub's namespace; the page opens `url` and sends `{type:'auth', ticket, path, depth}` as its first frame |
| `/api/liveengine/join`, `/leave`, `/heartbeat` | The seat routes: `leave` and `heartbeat` apply to paired players too; `join` is the older matchmaking-only call the game no longer uses |

A hub `4xx` answer (for example `403 credential refused`) reaches the browser with its status. Credentials are never stored by the bridge: a player pairing again presents it again. `GOGH_REQUEST_LIMIT` (default 10 per minute per client address, shared by all routes) can be raised for a LAN. Set `GOGH_HUB_AUTH` to the JSON of a hub `auth` verifier (see `docs/hosting.md`) to require a credential; the game reads one from `localStorage['gogh-strike-liveengine-credential']`. The hub is registered with its channels and `auth` only when the bridge creates it; an existing hub keeps its own settings, and external hubs own theirs.

With this bridge, every player joins the hub's shared room by default (a `peer-hub` pairing, relay only); set `GOGH_LIVE_LINK=peer` to pair players with each other. Gogh Strike's own `npm run live` pairs players with each other by default (`GOGH_LIVE_LINK=hub` for the shared room). When players are paired with each other, the first player's request is held (up to `GOGH_PEER_WAIT_MS`, default 30000, then `408`) until another player pairs with them, and both get a `peer-peer` descriptor, so `LiveClient` opens a WebRTC data channel and keeps the hub as signalling and fallback. The game asks `LiveClient` for `directBroadcast`, so its player state travels over the data channels. `LIVEENGINE_WATCH_URL` overrides the registry address given by `watch` (default: the hub's own address as a WebSocket URL).

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

## Running from the published package

`server-live.mjs` in the Gogh Strike checkout is the same server built only from the installed `@dixonsolutions/liveengine` package (`dependencies`: `^0.4.0`, locked to the exact version in `package-lock.json`). Nothing in it imports from a LiveEngine checkout. The patch adds it, the dependency and the `live` script.

```sh
cd ../gogh-strike
npm install
npm run live                    # loopback
npm run live -- --lan           # this machine's LAN address (never a Tailscale one)
npm run live -- --password      # players sign in; the admin console opens at /api/liveengine/admin
```

It starts the package's `liveengine-hub` binary on a free port, mounts `createHubApi` from `@dixonsolutions/liveengine/hub-api` with the schemas and channels from `src/liveengine-net.js`, serves the game and the package's live client, and prints the URLs. With `--password` it also creates an `admin` account and prints its generated password once; it is kept, with the hub's administrator token, in `.liveengine/keys.json` (gitignored and never served). Set `LIVEENGINE_URL` and `LIVEENGINE_TOKEN` to use an existing hub instead of starting one. It reads the same options as `bridge.mjs`: `PORT`, `HOST`, `LIVEENGINE_HUB_ID`, `LIVEENGINE_HUB_MODE`, `LIVEENGINE_HUB_PATH`, `LIVEENGINE_REMOTE_*`, `LIVEENGINE_WATCH_URL`, `GOGH_LIVE_LINK`, `GOGH_MATCH_CAPACITY`, `GOGH_ALLOWED_ORIGINS`, `GOGH_ALLOWED_HOSTS`, `GOGH_HUB_AUTH`, `GOGH_HUB_AUTH_PASSWORD=1` and the rest listed at the top of the file. The game's own `npm start` is unchanged.

`npm run dev:gogh` differs: it builds the debug hub from this LiveEngine checkout and runs `bridge.mjs` against `lib/`, so it tests unreleased changes. `npm run live` tests what is on npm. To run the end-to-end test against the package, use `GOGH_E2E_SERVER=package GOGH_STRIKE_ROOT=../gogh-strike npm run test:e2e`; it prints the package version the game resolved and fails every scenario it cannot run in that mode. The default, `GOGH_E2E_SERVER=bridge`, uses `bridge.mjs`. The test's own central hubs and Node `LiveClient` still come from this checkout in both modes.

## Local end-to-end test

`live-e2e.mjs` (`npm run test:e2e`, not part of `npm test`) is the real local test. It starts two `liveengine-hub` processes (build them with `npm run build:hub`), one bridge per scenario and headless Chromium, all on free loopback ports, and stops everything at the end, also on failure. It prints one `PASS` or `FAIL` line per scenario and exits non-zero if any fails; a scenario that cannot run counts as failed.

| # | Scenario |
| --- | --- |
| 1 | Central hub, a nested central, and a hub created on demand inside it with its own token auth: a wrong credential shows the refusal in the page and pairs nothing; the right one joins |
| 2 | Peer-peer over WebRTC: both pages report an open data channel, state frames arrive with `via: "direct"` and none by the relay, and each player sees the other move |
| 3 | The data channel is closed in the page mid-game: both sides fall back, and the frames and the movement continue over the relay |
| 4 | Peer-hub with a Node `LiveClient` as a service participant: relay only, no direct link; the page sees the service move and the service receives the page's state |
| 5 | Hub-hub: two services joined by a link, one player on each, who see each other move |
| 6 | Reconnect: one player's socket is cut and held down; the other sees it leave, then it returns with a fresh ticket and is seen again |
| 7 | Live hub list: a page opens the registry with a watch ticket and sees hubs, also a nested one, added and removed |
| 8 | Schema rejection: five malformed frames are each refused by the hub (it closes the sender, which reconnects), none reaches the other player, and play continues |

| 16 | Sign-in: an admin signs in on the front screen, opens the admin console and creates accounts; a wrong password and an unknown user are refused in the same plain words; a user cannot open the console (403, anonymous 401); the session cookie is HttpOnly and SameSite=Strict |
| 17 | Two signed-in users find a match peer to peer; each is shown by the other's verified username and role, the player ids are the usernames, frames travel directly, and an unsigned pair is refused |
| 18 | The admin kicks one player in the console: that player sees they were removed, does not reconnect by itself, is still signed in and rejoins |
| 19 | The admin disables one player: thrown out with the disabled message and signed out, refused at sign-in, back after re-enabling |
| 20 | A password changed in one tab signs the other tab out and closes its live connection; the old password is refused, the new one works |
| 21 | The admin resets a password in the console while the player is mid-match: the connection closes and the old session stops working |
| 22 | An external hub with password auth reached through the central: the accounts live on the remote service only and sign-in works end to end |
| 23 | Ten users signed in at once are matched into rooms, every pair exchanges data and each is shown under its own name |

Set `GOGH_HUB_AUTH_PASSWORD=1` on the bridge (or run `npm run dev:gogh -- --password`, which also creates an `admin` account and prints its generated password once) to make players sign in with an account on the hub's service and to serve the admin console at `/api/liveengine/admin`. The Play online panel then shows a sign-in form, "Signed in as NAME (role)", Sign out, Change password and, for admins, a link to the console. Scenarios 16 to 23 use it. `GOGH_E2E_SHOTS` (a directory) saves screenshots of the signed-out and signed-in panel and the console as `auth-*.png`.

Environment: `PLAYWRIGHT_PATH` (a Playwright module; `require('playwright')` is tried first), `CHROMIUM_PATH` (browser executable), `GOGH_STRIKE_ROOT` (the game checkout), `GOGH_E2E_CHROMIUM_ARGS` (replaces the Chromium flags) and `GOGH_E2E_ONLY` (for example `2,3`, a debugging filter: skipped scenarios print `SKIP` and the exit code is non-zero). Chromium runs with `--disable-features=WebRtcHideLocalIpsWithMdns --allow-loopback-in-peer-connection` so two contexts find each other's local candidates. Software GL draws the town at under one frame a second, which starves the page's WebRTC callbacks, so when `/dev/dri/renderD128` exists it also passes `--use-gl=angle --use-angle=vulkan --enable-features=Vulkan --ignore-gpu-blocklist --enable-gpu` to render on the GPU, still headless.

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

For optional headless browser coverage, set `PLAYWRIGHT_PATH` to a Playwright `index.mjs` and optionally set `CHROMIUM_PATH` to the browser executable. The runner checks the lobby checkbox, starts a match, verifies the LiveEngine toast and browser user pairing, then saves `liveengine-gogh-lobby.png` under `TMPDIR` (override with `GOGH_SCREENSHOT`). It does not install Playwright into either project.

The lobby generates client IDs with `crypto.getRandomValues`, so matchmaking also works over plain HTTP on a LAN where `crypto.randomUUID` is unavailable. The id is kept per tab in `sessionStorage`.

Recorded for the earlier matchmaking-only version, on upstream commit `2285e640097d9b12bcc42af72d5e8d0b37082f51`. The multiplayer version is covered by the Node tests above and a scripted pairing against a real hub; the local end-to-end test above runs it in headless browsers. `npm test` passed all 168 upstream tests, `npm run build` passed, and `node --test integrations/gogh-strike/bridge.test.mjs` passed. The live external-hub run reported `service join ok`, and optional Playwright reported `phase playing` with zero page errors; the screenshot was saved to `/tmp/liveengine-gogh-lobby.png`. A wrong local bearer returned HTTP 401 with a generic message and no token reflection. The test ran against local central and remote HTTP services, not a remote device.
