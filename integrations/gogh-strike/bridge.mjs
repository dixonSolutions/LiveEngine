import {readFile, stat} from 'node:fs/promises';
import http from 'node:http';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {pruneRates} from './rate-limit.mjs';

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
// "hub" pairs every player with the hub's shared room. "peer" pairs players with each other
// (the hub is still the signalling and fallback path); the first player waits for the next.
const linkKind = process.env.GOGH_LIVE_LINK === 'peer' ? 'peer' : 'hub';
const PEER_POLL_MS = 500;
const peerWaitMs = Number(process.env.GOGH_PEER_WAIT_MS) > 0 ? Number(process.env.GOGH_PEER_WAIT_MS) : 30_000;
// Where the browser opens the live registry; the hub's own address unless it is reached another way.
const watchUrl = process.env.LIVEENGINE_WATCH_URL || `${(process.env.LIVEENGINE_URL || '').replace(/^http/, 'ws').replace(/\/$/, '')}/v1/live/registry`;
// A verifier for the hub's `auth` (see docs/hosting.md), as JSON. Unset leaves the hub open.
const hubAuth = process.env.GOGH_HUB_AUTH ? JSON.parse(process.env.GOGH_HUB_AUTH) : undefined;
// Gogh Strike's own DTO schemas and channels live in the game, so the game stays the one owner.
const net = await import(pathToFileURL(path.join(gameRoot, 'src/liveengine-net.js'))).catch(() => null);
const liveClientFile = path.join(here, '../../lib/live-client.mjs');

const joinRates = new Map();
const RATE_WINDOW_MS = 60_000;
// Requests per client address per minute across join, pair, renew, discover, heartbeat and leave.
const JOIN_LIMIT = Number.isInteger(Number(process.env.GOGH_REQUEST_LIMIT)) && Number(process.env.GOGH_REQUEST_LIMIT) > 0 ? Number(process.env.GOGH_REQUEST_LIMIT) : 10;
const serverId = process.env.GOGH_MATCH_SERVER_ID || 'gogh-strike-local';
const capacity = Number(process.env.GOGH_MATCH_CAPACITY || 12);
const configuredIdleMs = Number(process.env.GOGH_IDLE_MS);
const idleMs = Number.isFinite(configuredIdleMs) && configuredIdleMs > 0 ? configuredIdleMs : 3 * 60_000;
const seats = new Map();
// What `renewal` needs to pair a known player again. Credentials are never kept.
const pairings = new Map();
const PAIRING_LIMIT = 4096;
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
        if (hubMode !== 'external') await this.registerSchemas();
        const dto = hubMode === 'external'
          ? {id: hubId, mode: 'external', address: remoteAddress, remoteHubId, remoteToken, scopes: hubScopes}
          : {id: hubId, mode: 'internal', scopes: hubScopes,
            ...(net?.LIVE_CHANNELS ? {channels: net.LIVE_CHANNELS} : {}), ...(hubAuth ? {auth: hubAuth} : {})};
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

  // Publish the game's schemas once. A version is immutable, so an existing one is left alone.
  async registerSchemas() {
    if (!net?.LIVE_SCHEMAS) return;
    const listed = await this.client.listSchemas();
    const known = new Set((listed?.schemas || []).map(entry => entry.ref));
    for (const schema of net.LIVE_SCHEMAS) {
      if (known.has(`${schema.id}@${schema.version}`)) continue;
      try { await this.client.createSchema(schema); }
      catch (error) { if (error.status !== 409) throw error; }
    }
  }

  async pair(userId, player, credential) {
    await this.ensureReady();
    if (linkKind === 'peer') return this.pairPeer(userId, player);
    const request = {
      subject: {id: userId, kind: 'peer', scopes: {game: 'gogh-strike', mode: 'tdm', team: player.team, artist: player.role}},
      target: {kind: 'hub', id: hubId, requirements: hubScopes}, path: hubPath,
    };
    const descriptor = await this.client.pair({...request, credential});
    pairings.set(userId, request);
    return descriptor;
  }

  // Peer pairing: register as a candidate, then take the first peer found or wait to be taken.
  async pairPeer(userId, player) {
    // `arena` keeps these players to this hub: other players on the service are never candidates.
    const scopes = {game: 'gogh-strike', mode: 'tdm', team: player.team, artist: player.role, arena: hubId};
    const request = {subject: {id: userId, kind: 'peer', scopes}, target: {kind: 'peer', requirements: {...hubScopes, arena: hubId}}, path: hubPath};
    const known = await this.call('getUser', userId);
    if (!known) await this.call('upsertUser', {id: userId, scopes});
    pairings.set(userId, {peer: true});
    const deadline = Date.now() + peerWaitMs;
    // Registered first, so of two players arriving together the later one finds the earlier.
    // The pairing itself costs several hub requests, so a waiting player only polls its own seat.
    for (let attempt = 0; ; attempt++) {
      const current = attempt ? await this.call('getUser', userId) : known;
      if (current?.status === 'paired' && current.serverId) return this.peerDescriptor(userId, current.serverId);
      if (attempt === 0) {
        try { return await this.client.pair(request); }
        catch (error) { if (error.status !== 404 && error.status !== 409) throw error; }
      }
      if (Date.now() >= deadline) {
        pairings.delete(userId);
        await this.call('leave', userId).catch(() => {});
        throw Object.assign(new Error('No other player arrived in time'), {status: 408});
      }
      await new Promise(resolve => setTimeout(resolve, PEER_POLL_MS));
    }
  }

  async peerDescriptor(userId, room) {
    const ticket = await this.client.ticket(hubId, {userId, serverId: room, path: hubPath});
    return {link: 'peer-peer', room, userId, ticket: ticket.ticket, url: ticket.url, transports: ['webrtc', 'websocket']};
  }

  async renew(userId, credential) {
    const request = pairings.get(userId);
    if (!request) return null;
    if (request.peer) {
      const current = await this.call('getUser', userId);
      return current?.serverId ? this.peerDescriptor(userId, current.serverId) : null;
    }
    return this.client.renewal(request)({credential});
  }

  // A read-only registry watch for the namespace that holds the game hub. The browser opens the
  // registry socket itself with this ticket and never sees the administrator token.
  async watch(depth) {
    await this.ensureReady();
    const issued = await this.client.watchTicket({path: hubPath, depth});
    return {url: watchUrl, ticket: issued.ticket, expiresAt: issued.expiresAt, path: hubPath, depth};
  }

  async discover(query) {
    await this.ensureReady();
    return this.client.discover({...query, path: hubPath});
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
  pruneRates(joinRates, Date.now(), RATE_WINDOW_MS);
  for (const [userId, lastSeen] of seats) {
    if (Date.now() - lastSeen < idleMs) continue;
    try {
      await withSeat(userId, async () => {
        if (seats.get(userId) !== lastSeen) return;
        await getClient().leave(userId);
        seats.delete(userId);
        pairings.delete(userId);
      });
    } catch { /* Retry on the next sweep. */ }
  }
}, Math.min(30_000, idleMs / 2));
reaper.unref();

// The hub's own 4xx answer (a refused credential, no matching hub) reaches the browser; anything else is a bridge failure.
function failure(res, error) {
  if (error.message?.startsWith('Set LIVEENGINE_')) return json(res, 503, {error: error.message});
  if (error.status >= 400 && error.status < 500 && error.status !== 401) return json(res, error.status, {error: error.message});
  return json(res, 502, {error: 'LiveEngine request failed'});
}

// The browser gets a descriptor (a single-use ticket and a URL), never the administrator token.
async function pairRoute(res, renewing, input) {
  const credential = typeof input.credential === 'string' && input.credential.length <= 4096 ? input.credential : undefined;
  const player = {team: input.team === 1 ? 1 : 0, role: typeof input.role === 'string' ? input.role.slice(0, 40) : 'vanguard'};
  try {
    const descriptor = await withSeat(input.userId, async () => {
      if (!renewing && !pairings.has(input.userId) && pairings.size >= PAIRING_LIMIT) throw Object.assign(new Error('Too many players'), {status: 429});
      const result = renewing ? await getClient().renew(input.userId, credential) : await getClient().pair(input.userId, player, credential);
      if (result) seats.set(input.userId, Date.now());
      return result;
    });
    if (!descriptor) return json(res, 404, {error: 'Pair before renewing'});
    return json(res, 200, descriptor);
  } catch (error) { return failure(res, error); }
}

async function watch(res, input) {
  const depth = Number.isInteger(input.depth) && input.depth >= 1 && input.depth <= 8 ? input.depth : 1;
  try { return json(res, 200, await getClient().watch(depth)); }
  catch (error) { return failure(res, error); }
}

async function discover(res, input) {
  const kind = ['hub', 'peer', 'any'].includes(input.kind) ? input.kind : 'hub';
  const query = {kind, ...(Number.isInteger(input.depth) && input.depth >= 1 && input.depth <= 8 ? {depth: input.depth} : {}),
    ...(input.requirements && typeof input.requirements === 'object' ? {requirements: input.requirements} : {})};
  try { return json(res, 200, await getClient().discover(query)); }
  catch (error) { return failure(res, error); }
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
    if (url.pathname.startsWith('/api/') && req.headers.origin && !allowedOrigins.has(req.headers.origin)) {
      return json(res, 403, {error: 'Cross-origin request rejected'});
    }
    if (url.pathname === '/api/liveengine/status' && req.method === 'GET') {
      return json(res, 200, {configured: Boolean(apiBase && token), hubId, mode: hubMode, externalConfigured: Boolean(remoteAddress), localSimulation: true});
    }
    if (['/api/liveengine/join', '/api/liveengine/leave', '/api/liveengine/heartbeat', '/api/liveengine/pair', '/api/liveengine/renew', '/api/liveengine/discover', '/api/liveengine/watch'].includes(url.pathname) && req.method === 'POST') {
      const origin = req.headers.origin;
      if (!allowedOrigins.has(origin)) return json(res, 403, {error: 'Cross-origin request rejected'});
      const now = Date.now();
      const timestamps = (joinRates.get(req.socket.remoteAddress) || []).filter(time => now - time < RATE_WINDOW_MS);
      if (timestamps.length >= JOIN_LIMIT) return json(res, 429, {error: 'Too many matchmaking requests'});
      timestamps.push(now);
      joinRates.set(req.socket.remoteAddress, timestamps);
      const input = await readJson(req);
      if (url.pathname.endsWith('/discover')) return await discover(res, input);
      if (url.pathname.endsWith('/watch')) return await watch(res, input);
      if (typeof input.userId !== 'string' || !/^[a-zA-Z0-9_-]{1,64}$/.test(input.userId)) return json(res, 400, {error: 'Invalid userId'});
      if (url.pathname.endsWith('/pair') || url.pathname.endsWith('/renew')) return await pairRoute(res, url.pathname.endsWith('/renew'), input);
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
            pairings.delete(input.userId);
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
