# Framework examples

These examples use the published `@dixonsolutions/liveengine` package. They are intentionally small: authentication, persistence and application authorization belong in your service.

| Example | Model | What it demonstrates |
| --- | --- | --- |
| `express-server.js` | In-process `LiveEngine` | HTTP matchmaking routes and SSE event draining |
| `fastify-server.mjs` | `HubClient` | Calling an internal hub hosted by `liveengine-hub` |
| `nestjs/` | In-process `LiveEngine` provider | NestJS module, service and controller wiring |
| `nextjs/` | `HubClient` | App Router routes and a client page with live room listing |
| `nitro/` | `HubClient` | Nitro server routes for rooms, matchmaking and ping |
| `sveltekit/` | `HubClient` | SvelteKit server routes and a client page with room listing |
| `../user-hub` | Browser interface | Dependency-free player UI with rooms and native ping |
| `../axum-service.rs` | Embedded Rust `HubService` | Mounting LiveEngine routes inside an Axum app |

## Express

```sh
npm install express @dixonsolutions/liveengine
node express-server.js
```

```sh
curl -X POST http://127.0.0.1:3000/api/servers \
  -H 'content-type: application/json' \
  -d '{"id":"room-1","capacity":2,"scopes":{"region":"au"}}'

curl -X POST http://127.0.0.1:3000/api/match \
  -H 'content-type: application/json' \
  -d '{"userId":"alice","scopes":{"region":"au"}}'
```

## Fastify

Start a hub first, then run the API:

```sh
export LIVEENGINE_TOKEN="$(openssl rand -hex 32)"
LIVEENGINE_BIND=127.0.0.1:8787 liveengine-hub &

npm install fastify @dixonsolutions/liveengine
LIVEENGINE_URL=http://127.0.0.1:8787 \
LIVEENGINE_TOKEN="$LIVEENGINE_TOKEN" \
LIVEENGINE_HUB_ID=ranked \
node fastify-server.mjs
```

## NestJS

Copy the three files in `nestjs/` into a NestJS application, install `@dixonsolutions/liveengine`, and import `LiveEngineModule` in `AppModule`. The service holds the engine in memory, so use a single process for local matchmaking or call a central hub when you run multiple instances.

## Rust/Axum

```sh
LIVEENGINE_TOKEN="$(openssl rand -hex 32)" cargo run --example axum-service
```

## Next.js

Copy `nextjs/app/` and `nextjs/lib/` into an App Router project. See `nextjs/README.md` for the full route list. The `/liveengine` page lists rooms and connects as a client.

## Nitro

Copy `nitro/server/` into a Nitro or Nuxt project. The server routes list rooms, match a user, and sample native ping. See `nitro/README.md`.

## SvelteKit

Copy `sveltekit/src/` into a SvelteKit project. The `/liveengine` page lists rooms and connects as a client. See `sveltekit/README.md`.

## User hub interface

`../user-hub` is a standalone browser interface with room listing, matchmaking and a native ping panel. See `../user-hub/README.md`.
