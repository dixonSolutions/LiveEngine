# Nitro example

Server routes for a Nitro/Nuxt app. Copy `server/` into a Nitro project.

```sh
npm install nitropack @dixonsolutions/liveengine
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" \
LIVEENGINE_HUB_ID=ranked \
npx nitro dev
```

| Route | Purpose |
| --- | --- |
| `GET /api/liveengine/servers` | List servers and stats |
| `POST /api/liveengine/match` | Upsert a user and auto-match |
| `GET /api/liveengine/ping` | Native hub ping |

Use Nitro's runtime config instead of `process.env` if you deploy with a preset that does not expose environment variables directly.
