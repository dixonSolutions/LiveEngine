# SvelteKit example

Server routes for listing rooms, matchmaking, and native ping, plus a Svelte page that connects as a client.

Copy `src/` into a SvelteKit project.

```sh
npm install @dixonsolutions/liveengine
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" \
LIVEENGINE_HUB_ID=ranked \
npm run dev
```

Open `/liveengine`. The page polls the server list and calls the match endpoint.

| Route | Purpose |
| --- | --- |
| `GET /api/liveengine/servers` | List servers and stats |
| `POST /api/liveengine/match` | Upsert a user and auto-match |
| `GET /api/liveengine/ping` | Native hub ping |

`$env/dynamic/private` keeps the hub token server-side in SvelteKit.
