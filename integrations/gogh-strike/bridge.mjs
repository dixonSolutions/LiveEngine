import {readFile, stat} from 'node:fs/promises';
import http from 'node:http';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {createHubApi} from '../../lib/hub-api.mjs';

// Gogh Strike's bridge: it serves the game and mounts the generic hub API with the game's schemas.
// Connecting, pairing, seats and limits are lib/hub-api.mjs; nothing about them lives here. The hub and
// the game's schemas are published on the first request (api.setup() would do it at start).
const here = path.dirname(fileURLToPath(import.meta.url));
const {HubClient} = createRequire(import.meta.url)('../../lib/hubs.js');
const gameRoot = path.resolve(process.env.GOGH_STRIKE_ROOT || path.join(here, '../../../gogh-strike'));
const port = Number(process.env.PORT || 8967);
const host = process.env.HOST || '127.0.0.1';
const list = value => (value || '').split(',').map(entry => entry.trim().toLowerCase()).filter(Boolean);
const allowedHosts = new Set([`127.0.0.1:${port}`, `localhost:${port}`, ...list(process.env.GOGH_ALLOWED_HOSTS)]);
if (!['127.0.0.1', 'localhost', '0.0.0.0', '::'].includes(host)) {
  allowedHosts.add(`${host.toLowerCase()}:${port}`);
}
// Pages allowed to call the API: this one, and any other page the owner lists (GOGH_ALLOWED_ORIGINS).
const allowedOrigins = [...list(process.env.GOGH_ALLOWED_ORIGINS), ...[...allowedHosts].map(value => `http://${value}`)];
const apiBase = (process.env.LIVEENGINE_URL || '').replace(/\/$/, '');
const token = process.env.LIVEENGINE_TOKEN || '';
const hubId = process.env.LIVEENGINE_HUB_ID || 'gogh-strike';
const hubMode = process.env.LIVEENGINE_HUB_MODE || 'internal';
// Slash-separated central IDs; an empty path selects the root registry.
const hubPath = (process.env.LIVEENGINE_HUB_PATH || '').split('/').filter(Boolean);
const remoteHubId = process.env.LIVEENGINE_REMOTE_HUB_ID || hubId;
const positive = (value, fallback) => (Number.isInteger(Number(value)) && Number(value) > 0 ? Number(value) : fallback);
// Gogh Strike's own DTO schemas and channels live in the game, so the game stays the one owner.
const net = await import(pathToFileURL(path.join(gameRoot, 'src/liveengine-net.js'))).catch(() => null);
const liveClientFile = path.join(here, '../../lib/live-client.mjs');

const api = createHubApi({
  client: apiBase && token ? new HubClient(apiBase, {token}) : undefined,
  notConfigured: 'Set LIVEENGINE_URL and LIVEENGINE_TOKEN to enable matchmaking.',
  hubId, hubPath, mode: hubMode, scopes: {game: 'gogh-strike'},
  remote: {address: process.env.LIVEENGINE_REMOTE_ADDRESS || '', hubId: remoteHubId, token: process.env.LIVEENGINE_REMOTE_TOKEN || ''},
  schemas: net?.LIVE_SCHEMAS, channels: net?.LIVE_CHANNELS,
  // A verifier for the hub's `auth` (see docs/hosting.md), as JSON. Unset leaves the hub open.
  auth: process.env.GOGH_HUB_AUTH ? JSON.parse(process.env.GOGH_HUB_AUTH) : undefined,
  // "hub" pairs every player with the hub's shared room. "peer" pairs players with each other
  // (the hub is still the signalling and fallback path); the first player waits for the next.
  peerLink: {kind: process.env.GOGH_LIVE_LINK === 'peer' ? 'peer' : 'hub', waitMs: positive(process.env.GOGH_PEER_WAIT_MS, 30_000)},
  roomCapacity: positive(process.env.GOGH_MATCH_CAPACITY, 12),
  allowedOrigins, basePath: '/api/liveengine',
  limits: {requestsPerMinute: positive(process.env.GOGH_REQUEST_LIMIT, 10), readsPerMinute: positive(process.env.GOGH_REQUEST_LIMIT, 300), idleMs: positive(process.env.GOGH_IDLE_MS, 3 * 60_000)},
  // What a request says about the player, as the scopes the hub matches on.
  subject: input => ({game: 'gogh-strike', mode: 'tdm', team: input.team === 1 ? 1 : 0, artist: typeof input.role === 'string' ? input.role.slice(0, 40) : 'vanguard'}),
  join: {server: {id: process.env.GOGH_MATCH_SERVER_ID || 'gogh-strike-local', capacity: Number(process.env.GOGH_MATCH_CAPACITY || 12), minMembers: 2,
    scopes: {game: 'gogh-strike', mode: 'tdm', map: 'van-goghs-town', region: 'local'}}},
  // Where the browser opens the live registry; the hub's own address unless it is reached another way.
  watchUrl: process.env.LIVEENGINE_WATCH_URL || undefined,
});

const mime = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json', '.png': 'image/png', '.webp': 'image/webp',
  '.svg': 'image/svg+xml', '.glb': 'model/gltf-binary', '.woff2': 'font/woff2',
};

function json(res, status, value) {
  res.writeHead(status, {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store'});
  res.end(JSON.stringify(value));
}

const server = http.createServer(async (req, res) => {
  try {
    const hostHeader = req.headers.host?.toLowerCase();
    if (!allowedHosts.has(hostHeader)) return json(res, 403, {error: 'Invalid Host header'});
    const url = new URL(req.url, `http://${host}:${port}`);
    if (url.pathname === '/liveengine/live-client.mjs' && req.method === 'GET') {
      res.writeHead(200, {'Content-Type': mime['.mjs'], 'Cache-Control': 'no-cache'});
      return res.end(await readFile(liveClientFile));
    }
    if (url.pathname === '/health') return json(res, 200, {app: 'Gogh Strike LiveEngine bridge', hubId});
    if (url.pathname.startsWith('/api/liveengine/')) return await api(req, res);
    if (url.pathname.startsWith('/api/') && req.headers.origin && !allowedOrigins.includes(req.headers.origin)) {
      return json(res, 403, {error: 'Cross-origin request rejected'});
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') return json(res, 405, {error: 'Method not allowed'});
    let pathname = decodeURIComponent(url.pathname);
    if (pathname.endsWith('/')) pathname += 'index.html';
    const file = path.resolve(gameRoot, `.${pathname}`);
    if (!file.startsWith(gameRoot + path.sep)) return json(res, 400, {error: 'Invalid path'});
    const info = await stat(file);
    if (!info.isFile()) return json(res, 404, {error: 'Not found'});
    res.writeHead(200, {'Content-Type': mime[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache'});
    if (req.method === 'HEAD') return res.end();
    res.end(await readFile(file));
  } catch (error) {
    if (error.code === 'ENOENT' || error.code === 'ENOTDIR') return json(res, 404, {error: 'Not found'});
    return json(res, error.status || 502, {error: error.message || 'Bridge request failed'});
  }
});

server.listen(port, host, () => {
  console.log(`Gogh Strike + LiveEngine: http://${host}:${port}`);
  console.log(`Allowed hosts: ${[...allowedHosts].join(', ')}`);
});
