import {readFile, stat} from 'node:fs/promises';
import http from 'node:http';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const {HubClient} = createRequire(import.meta.url)('../../lib/hubs.js');
const gameRoot = path.resolve(process.env.GOGH_STRIKE_ROOT || path.join(here, '../../../gogh-strike'));
const port = Number(process.env.PORT || 8967);
const host = process.env.HOST || '127.0.0.1';
const extraHosts = (process.env.GOGH_ALLOWED_HOSTS || '')
  .split(',')
  .map(value => value.trim().toLowerCase())
  .filter(Boolean);
const extraOrigins = (process.env.GOGH_ALLOWED_ORIGINS || '')
  .split(',')
  .map(value => value.trim().toLowerCase())
  .filter(Boolean);
const allowedHosts = new Set([`127.0.0.1:${port}`, `localhost:${port}`, ...extraHosts]);
if (!['127.0.0.1', 'localhost', '0.0.0.0', '::'].includes(host)) {
  allowedHosts.add(`${host.toLowerCase()}:${port}`);
}
const allowedOrigins = new Set([
  ...extraOrigins,
  ...[...allowedHosts].map(value => `http://${value}`),
]);
const apiBase = (process.env.LIVEENGINE_URL || '').replace(/\/$/, '');
const token = process.env.LIVEENGINE_TOKEN || '';
const remoteToken = process.env.LIVEENGINE_REMOTE_TOKEN || '';
const hubId = process.env.LIVEENGINE_HUB_ID || 'gogh-strike';
const hubMode = process.env.LIVEENGINE_HUB_MODE || 'internal';
// Slash-separated central IDs; an empty path selects the root registry.
const hubPath = (process.env.LIVEENGINE_HUB_PATH || '').split('/').filter(Boolean);
const remoteAddress = process.env.LIVEENGINE_REMOTE_ADDRESS || '';
const remoteHubId = process.env.LIVEENGINE_REMOTE_HUB_ID || hubId;
const hubScopes = {game: 'gogh-strike'};

const joinRates = new Map();
const RATE_WINDOW_MS = 60_000;
const JOIN_LIMIT = 10;
const serverId = process.env.GOGH_MATCH_SERVER_ID || 'gogh-strike-local';
const capacity = Number(process.env.GOGH_MATCH_CAPACITY || 12);
const configuredIdleMs = Number(process.env.GOGH_IDLE_MS);
const idleMs = Number.isFinite(configuredIdleMs) && configuredIdleMs > 0 ? configuredIdleMs : 3 * 60_000;
const seats = new Map();
const seatOperations = new Map();

async function withSeat(userId, operation) {
  const previous = seatOperations.get(userId) || Promise.resolve();
  const current = previous.catch(() => {}).then(operation);
  seatOperations.set(userId, current);
  try { return await current; }
  finally { if (seatOperations.get(userId) === current) seatOperations.delete(userId); }
}

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

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 16_384) throw Object.assign(new Error('Request body is too large'), {status: 413});
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf8');
  try { return raw ? JSON.parse(raw) : {}; }
  catch { throw Object.assign(new Error('Expected a JSON object'), {status: 400}); }
}

class LiveEngineClient {
  constructor() {
    if (!apiBase || !token) throw new Error('Set LIVEENGINE_URL and LIVEENGINE_TOKEN to enable matchmaking.');
    this.client = new HubClient(apiBase, {token});
    this.ready = null;
  }

  async call(method, ...args) {
    if (!hubPath.length) return this.client.call(hubId, method, args);
    return (await this.client.route(hubPath, 'call', {id: hubId, method, args})).result;
  }

  async ensureReady() {
    this.ready ||= (async () => {
      await this.client.contract();
      const listing = hubPath.length ? await this.client.route(hubPath, 'list') : await this.client.list();
      const hubs = Array.isArray(listing) ? listing : listing?.hubs;
      const existing = Array.isArray(hubs) ? hubs.find(hub => hub.id === hubId) : null;
      if (!existing) {
        if (hubMode === 'external' && !remoteAddress) throw new Error('Set LIVEENGINE_REMOTE_ADDRESS to register an external hub.');
        if (hubMode === 'external' && !process.env.LIVEENGINE_REMOTE_TOKEN) throw new Error('Set LIVEENGINE_REMOTE_TOKEN to register an external hub.');
        const dto = hubMode === 'external'
          ? {id: hubId, mode: 'external', address: remoteAddress, remoteHubId, remoteToken, scopes: hubScopes}
          : {id: hubId, mode: 'internal', scopes: hubScopes};
        try {
          if (hubPath.length) await this.client.route(hubPath, 'register', dto);
          else await this.client.register(dto);
        } catch (error) {
          // Another local bridge may have registered the same stable hub id concurrently.
          const latest = hubPath.length ? await this.client.route(hubPath, 'list') : await this.client.list();
          const latestHubs = Array.isArray(latest) ? latest : latest?.hubs;
          if (!Array.isArray(latestHubs) || !latestHubs.some(hub => hub.id === hubId)) throw error;
        }
      }
    })();
    try { await this.ready; }
    catch (error) { this.ready = null; throw error; }
  }

  async join(userId, player) {
    await this.ensureReady();
    await this.call('upsertServer', {
      id: serverId, capacity, minMembers: 2,
      scopes: {game: 'gogh-strike', mode: 'tdm', map: 'van-goghs-town', region: 'local'},
    });
    const current = await this.call('getUser', userId);
    if (current?.status === 'paired' && current.serverId) {
      return {ok: true, userId, serverId: current.serverId, existing: true};
    }
    await this.call('upsertUser', {
      id: userId, scopes: {game: 'gogh-strike', mode: 'tdm', team: player.team, artist: player.role},
    });
    const result = await this.call('autoJoin', userId);
    if (!result?.ok) return result || {ok: false, reason: 'empty_response', userId};
    return {...result, localSimulation: true};
  }

  async leave(userId) {
    await this.ensureReady();
    return this.call('leave', userId);
  }
}

let client;
function getClient() { return client ||= new LiveEngineClient(); }

const reaper = setInterval(async () => {
  for (const [userId, lastSeen] of seats) {
    if (Date.now() - lastSeen < idleMs) continue;
    try {
      await withSeat(userId, async () => {
        if (seats.get(userId) !== lastSeen) return;
        await getClient().leave(userId);
        seats.delete(userId);
      });
    } catch { /* Retry on the next sweep. */ }
  }
}, Math.min(30_000, idleMs / 2));
reaper.unref();

const server = http.createServer(async (req, res) => {
  try {
    const hostHeader = req.headers.host?.toLowerCase();
    if (!allowedHosts.has(hostHeader)) return json(res, 403, {error: 'Invalid Host header'});
    const url = new URL(req.url, `http://${host}:${port}`);
    if (url.pathname === '/health') return json(res, 200, {app: 'Gogh Strike LiveEngine bridge', hubId});
    if (url.pathname.startsWith('/api/') && req.headers.origin && !allowedOrigins.has(req.headers.origin)) {
      return json(res, 403, {error: 'Cross-origin request rejected'});
    }
    if (url.pathname === '/api/liveengine/status' && req.method === 'GET') {
      return json(res, 200, {configured: Boolean(apiBase && token), hubId, mode: hubMode, externalConfigured: Boolean(remoteAddress), localSimulation: true});
    }
    if (['/api/liveengine/join', '/api/liveengine/leave', '/api/liveengine/heartbeat'].includes(url.pathname) && req.method === 'POST') {
      const origin = req.headers.origin;
      if (!allowedOrigins.has(origin)) return json(res, 403, {error: 'Cross-origin request rejected'});
      const now = Date.now();
      const timestamps = (joinRates.get(req.socket.remoteAddress) || []).filter(time => now - time < RATE_WINDOW_MS);
      if (timestamps.length >= JOIN_LIMIT) return json(res, 429, {error: 'Too many matchmaking requests'});
      timestamps.push(now);
      joinRates.set(req.socket.remoteAddress, timestamps);
      const input = await readJson(req);
      if (typeof input.userId !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(input.userId)) return json(res, 400, {error: 'Invalid userId'});
      if (url.pathname.endsWith('/heartbeat')) {
        const found = await withSeat(input.userId, () => {
          if (!seats.has(input.userId)) return false;
          seats.set(input.userId, Date.now());
          return true;
        });
        if (!found) return json(res, 404, {error: 'Seat not found'});
        return json(res, 200, {ok: true});
      }
      if (url.pathname.endsWith('/leave')) {
        try {
          const result = await withSeat(input.userId, async () => {
            const left = await getClient().leave(input.userId);
            seats.delete(input.userId);
            return left;
          });
          return json(res, 200, result);
        } catch { return json(res, 502, {error: 'LiveEngine matchmaking request failed'}); }
      }
      const player = {team: input.team === 1 ? 1 : 0, role: typeof input.role === 'string' ? input.role.slice(0, 40) : 'vanguard'};
      let result;
      try {
        result = await withSeat(input.userId, async () => {
          const joined = await getClient().join(input.userId, player);
          if (joined.ok) seats.set(input.userId, Date.now());
          return joined;
        });
      }
      catch (error) {
        if (error.message?.startsWith('Set LIVEENGINE_')) return json(res, 503, {error: error.message});
        return json(res, error.status || 502, {error: 'LiveEngine matchmaking request failed'});
      }
      if (!result.ok) return json(res, 409, result);
      return json(res, 200, result);
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
