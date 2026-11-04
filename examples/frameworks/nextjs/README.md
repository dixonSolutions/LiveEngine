# Next.js example

App Router route handlers plus a client page that lists rooms and joins the hub.

Copy `app/` and `lib/` into a Next.js project.

```sh
npm install next react react-dom @dixonsolutions/liveengine
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" \
LIVEENGINE_HUB_ID=ranked \
npm run dev
```

Routes:

| Route | Purpose |
| --- | --- |
| `GET /api/liveengine/servers` | List servers and stats |
| `POST /api/liveengine/match` | Upsert a user and auto-match |
| `GET /api/liveengine/ping` | Native hub ping |

The page at `/liveengine` polls the server list and calls the match endpoint. Keep the hub token in server-only environment variables; never use `NEXT_PUBLIC_*` for it.
