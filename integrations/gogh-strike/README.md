# Gogh Strike matchmaking bridge

This integration adds optional LiveEngine matchmaking to the browser game's lobby. The browser talks only to the loopback bridge; `LIVEENGINE_TOKEN` and the optional remote credential stay in the Node process. Successful matchmaking selects a hub room, then starts Gogh Strike's existing local match simulation. It does not replicate player movement, combat or match state between browsers.

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

Open `http://127.0.0.1:8967/`, select **Find a LiveEngine match**, then press **Play**. The bridge checks `/v1/contract`, creates the configured internal hub if it is missing, registers a 12-seat Gogh Strike room, upserts the player and calls `autoJoin`. Hub ids and server ids are configurable with `LIVEENGINE_HUB_ID` and `GOGH_MATCH_SERVER_ID`.

To target an external hub, set `LIVEENGINE_HUB_MODE=external`, `LIVEENGINE_REMOTE_ADDRESS`, `LIVEENGINE_REMOTE_HUB_ID`, and a separate `LIVEENGINE_REMOTE_TOKEN` (at least 24 characters). The bridge registers that external hub on the local service and passes the remote credential only in that server-side registration. The service must allow the remote host and use matching credentials on the remote service.

The bridge binds only to `127.0.0.1`, validates the Host and Origin headers on matchmaking requests, limits request bodies to 16 KiB, and rate-limits joins. It has no browser-facing token because it is intended for one local player. Do not expose it on a network interface. The upstream game's own `npm start` does not include the proxy; use this bridge command so credentials are not placed in browser storage or URLs. Outbound service calls use `HubClient` with its five-second request timeout.

Run the bridge adapter test without installing dependencies:

```sh
node --test integrations/gogh-strike/bridge.test.mjs
```

The test uses a local fake HTTP hub and verifies the contract handshake, internal and external registration, matchmaking call sequence, and rejection of hostile Host and Origin headers.

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

For optional headless browser coverage, set `PLAYWRIGHT_PATH` to a Playwright `index.mjs` and optionally set `CHROMIUM_PATH` to the browser executable. The runner checks the lobby checkbox, starts a match, verifies the LiveEngine toast and browser user pairing, then saves `/tmp/liveengine-gogh-lobby.png` (override with `GOGH_SCREENSHOT`). It does not install Playwright into either project.

Validated on upstream commit `2285e640097d9b12bcc42af72d5e8d0b37082f51`. `npm test` passed all 168 upstream tests, `npm run build` passed, and `node --test integrations/gogh-strike/bridge.test.mjs` passed. The live external-hub run reported `service join ok`, and optional Playwright reported `phase playing` with zero page errors; the screenshot was saved to `/tmp/liveengine-gogh-lobby.png`. A wrong local bearer returned HTTP 401 with a generic message and no token reflection. The test ran against local central and remote HTTP services, not a remote device.
