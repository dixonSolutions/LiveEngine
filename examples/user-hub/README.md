# User hub interface

A dependency-free browser interface for players:

- join with an ID and JSON scopes/requirements
- auto-match or pick an open room
- leave, refresh, and watch room state
- create a demo room
- sample native hub ping (min, average, max, jitter)

The Node server keeps `LIVEENGINE_TOKEN` server-side. The browser only talks to the loopback bridge.

## Run

Start a hub, or use the local Compose services:

```sh
export LIVEENGINE_TOKEN="$(openssl rand -hex 32)"
LIVEENGINE_BIND=127.0.0.1:8787 \
LIVEENGINE_PUBLIC_URL=ws://127.0.0.1:8787/v1/live \
LIVEENGINE_LIVE_ALLOWED_ORIGINS=http://127.0.0.1:8970 \
liveengine-hub
```

```sh
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" \
LIVEENGINE_HUB_ID=user-hub \
node examples/user-hub/server.js
```

Open <http://127.0.0.1:8970/>.

If you use the Compose services, point `LIVEENGINE_TOKEN_FILE` at the central token file instead of exporting the secret:

```sh
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN_FILE=/path/to/central-token \
node examples/user-hub/server.js
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `LIVEENGINE_URL` | `http://127.0.0.1:8787` | Hub base URL |
| `LIVEENGINE_TOKEN` | none | Administrative hub token |
| `LIVEENGINE_TOKEN_FILE` | none | Read the token from a file instead |
| `LIVEENGINE_HUB_ID` | `user-hub` | Internal hub to create or use |
| `HOST` | `127.0.0.1` | Bridge listen address |
| `PORT` | `8970` | Bridge listen port |

The bridge creates the configured internal hub if it does not exist. Binding to `127.0.0.1` is deliberate: the bridge is an administrative proxy and must not be exposed without adding application authentication.

## API used by the browser

| Route | Purpose |
| --- | --- |
| `GET /api/config` | Hub ID, URL and readiness |
| `GET /api/state?userId=` | User, matched room, rooms and stats |
| `GET /api/ping?samples=5` | Native hub ping samples |
| `POST /api/join` | Upsert user and optionally auto-match |
| `POST /api/join-room` | Join a specific room |
| `POST /api/leave` | Leave the current room |
| `POST /api/rooms` | Create a demo room |
| `POST /api/ticket` | Issue a room ticket for a paired user |
| `POST /api/watch` | Issue a registry watch ticket |

API routes validate `Host` and any `Origin` header; POST bodies are limited to 64 KiB.

## Live tree and messaging

Open two separate browser contexts, choose different user IDs, create a room, and register each user with auto-match disabled before joining that room. Select **Connect live** in each. Broadcast uses the relay; the targeted button uses WebRTC when a data channel is open and otherwise the relay. Transport and incoming message paths are shown in the interface. The live tree uses a scoped watch ticket and resnapshots after changes. The bridge obtains fresh room tickets on reconnect.

The hub's public URL must be reachable by each browser and its origin must be allowed. Configure `LIVEENGINE_ICE_SERVERS` and a reachable TURN server for browsers behind strict NAT. The demo bridge has no application authentication and is suitable only for trusted local use.
