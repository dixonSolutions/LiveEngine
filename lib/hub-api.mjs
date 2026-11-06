// A hub API for browsers: the routes a page needs to connect to a hosted hub, written once.
//
// `createHubApi` returns a plain Node `(req, res[, next])` handler, so it mounts in `http`,
// Express, Fastify (through `@fastify/middie` or `fastify.routing`), Next route handlers and
// anything else that hands over Node requests. The administrator token stays inside the `client`
// you give it; the page only ever sees descriptors, single-use tickets and public status.
//
// No field of any game or application lives here: schemas, channels, scopes and the mapping from
// a request to scopes are all passed in.
import { readFile } from 'node:fs/promises';

const ROUTES = {
  status: 'GET', ping: 'GET', 'live-client.mjs': 'GET',
  discover: 'POST', pair: 'POST', renew: 'POST', leave: 'POST', heartbeat: 'POST', watch: 'POST', join: 'POST',
};
const ID = /^[a-zA-Z0-9_-]{1,64}$/;
const RATE_WINDOW_MS = 60_000;
const liveClientFile = new URL('./live-client.mjs', import.meta.url);

/** Drop idle peers and old timestamps from a `Map<peer, number[]>` of request times. */
export function pruneRates(rates, now, windowMs) {
  for (const [peer, timestamps] of rates) {
    const active = timestamps.filter(time => now - time < windowMs);
    if (active.length) rates.set(peer, active);
    else rates.delete(peer);
  }
}

const positive = (value, fallback) => (Number.isFinite(Number(value)) && Number(value) > 0 ? Number(value) : fallback);
const fail = (message, status) => Object.assign(new Error(message), { status });

function originMatcher(allowed) {
  if (typeof allowed === 'function') return origin => Boolean(origin) && Boolean(allowed(origin));
  const set = new Set([...(allowed || [])].map(value => String(value).trim().replace(/\/$/, '').toLowerCase()).filter(Boolean));
  return origin => typeof origin === 'string' && set.has(origin.toLowerCase());
}

/**
 * @param {object} options
 * @param {object} [options.client] A `HubClient` (administrator token inside). Omit it, with `notConfigured`, to answer 503.
 * @param {string} options.hubId The hub this API fronts. Created on first use when the service has no such hub.
 * @param {string[]} [options.hubPath] Central namespace that holds the hub.
 * @param {'internal'|'external'} [options.mode]
 * @param {{address: string, hubId?: string, token: string}} [options.remote] For `mode: 'external'`.
 * @param {object} [options.scopes] The hub's own scopes, and what a hub pairing requires.
 * @param {object[]} [options.schemas] `{id, version, schema}` published once. A version is immutable.
 * @param {object} [options.channels] The hub's channel table.
 * @param {object} [options.auth] The hub's `auth` verifier DTO. Unset leaves the hub open.
 * @param {{kind?: 'hub'|'peer', waitMs?: number, maxWaitMs?: number}} [options.peerLink]
 * @param {Iterable<string>|((origin: string) => boolean)} [options.allowedOrigins] Pages allowed to call the API.
 * @param {Iterable<string>} [options.allowedHosts] When set, a request with another Host header is refused.
 * @param {string} [options.basePath] Path the handler answers under when it is not mounted by a router.
 * @param {{bodyBytes?: number, requestsPerMinute?: number, readsPerMinute?: number, idleMs?: number, maxPlayers?: number}} [options.limits]
 * @param {(input: object) => object} [options.subject] Request body to the scopes a player pairs with.
 * @param {{server: object, extra?: object}} [options.join] Enables the seat-based `join` route.
 * @param {object} [options.statusExtra] Added to the status answer.
 * @param {string} [options.watchUrl] Where the page opens the registry socket. Defaults to the hub's own address.
 * @param {string} [options.notConfigured] The 503 message when there is no client.
 */
export function createHubApi(options = {}) {
  const {
    client, hubId, hubPath = [], mode = 'internal', remote, scopes = {}, schemas = [], channels, auth,
    peerLink = {}, allowedOrigins = [], allowedHosts, basePath = '', limits = {}, subject, join,
    statusExtra = {}, notConfigured = 'The hub API has no hub client configured.',
  } = options;
  if (typeof hubId !== 'string' || !hubId) throw new TypeError('hubId is required');
  if (!Array.isArray(hubPath)) throw new TypeError('hubPath must be an array');
  if (!['internal', 'external'].includes(mode)) throw new TypeError('mode must be "internal" or "external"');
  const linkKind = peerLink.kind === 'peer' ? 'peer' : 'hub';
  const peerWaitMs = positive(peerLink.waitMs, 30_000);
  const peerMaxWaitMs = Math.max(peerWaitMs, positive(peerLink.maxWaitMs, 120_000));
  const bodyBytes = positive(limits.bodyBytes, 16_384);
  const requestLimit = positive(limits.requestsPerMinute, 10);
  const readLimit = positive(limits.readsPerMinute, 300);
  const idleMs = positive(limits.idleMs, 3 * 60_000);
  const maxPlayers = positive(limits.maxPlayers, 4096);
  const originAllowed = originMatcher(allowedOrigins);
  const hostSet = allowedHosts ? new Set([...allowedHosts].map(value => String(value).toLowerCase())) : null;
  const prefix = `/${String(basePath).split('/').filter(Boolean).join('/')}`.replace(/^\/$/, '');
  const subjectScopes = typeof subject === 'function' ? subject : () => ({ ...scopes });
  const watchUrl = options.watchUrl || (client?.address ? `${client.address.replace(/^http/, 'ws')}/v1/live/registry` : '');
  const configured = Boolean(client) && !(mode === 'external' && !(remote?.address && remote?.token));
  const unconfigured = client ? 'An external hub needs remote.address and remote.token.' : notConfigured;

  const rates = new Map();
  const reads = new Map();
  const seats = new Map(); // userId -> last seen, for the idle reaper
  const pairings = new Map(); // what `renew` needs to pair a known player again; credentials are never kept
  const pending = new Map(); // userId -> AbortController of a pair request that is waiting for a partner
  const seatOperations = new Map();
  let ready = null;
  let reaper = null;

  const call = (method, ...args) => (hubPath.length
    ? client.route(hubPath, 'call', { id: hubId, method, args }).then(answer => answer.result)
    : client.call(hubId, method, args));
  const listHubs = async () => {
    const listing = hubPath.length ? await client.route(hubPath, 'list') : await client.list();
    return Array.isArray(listing) ? listing : listing?.hubs;
  };

  // `leave` only returns a player to "waiting", which still makes them a candidate for the next
  // pairing. A player who paired through this API is removed as well, so nobody is matched with a
  // ghost; the answer is the `leave` answer.
  async function release(userId) {
    const left = await call('leave', userId);
    if (linkKind === 'peer' || pairings.has(userId)) await call('removeUser', userId).catch(() => {});
    return left;
  }

  async function withSeat(userId, operation) {
    const previous = seatOperations.get(userId) || Promise.resolve();
    const current = previous.catch(() => {}).then(operation);
    seatOperations.set(userId, current);
    try { return await current; }
    finally { if (seatOperations.get(userId) === current) seatOperations.delete(userId); }
  }

  async function registerSchemas() {
    if (!schemas.length) return;
    const known = new Set(((await client.listSchemas())?.schemas || []).map(entry => entry.ref));
    for (const schema of schemas) {
      if (known.has(`${schema.id}@${schema.version}`)) continue;
      try { await client.createSchema(schema); }
      catch (error) { if (error.status !== 409) throw error; }
    }
  }

  async function ensureReady() {
    if (!configured) throw fail(unconfigured, 503);
    ready ||= (async () => {
      await client.contract();
      const existing = (await listHubs())?.find?.(hub => hub.id === hubId);
      if (!existing) {
        if (mode !== 'external') await registerSchemas();
        const dto = mode === 'external'
          ? { id: hubId, mode: 'external', address: remote.address, remoteHubId: remote.hubId || hubId, remoteToken: remote.token, scopes }
          : { id: hubId, mode: 'internal', scopes, ...(channels ? { channels } : {}), ...(auth ? { auth } : {}) };
        try {
          if (hubPath.length) await client.route(hubPath, 'register', dto);
          else await client.register(dto);
        } catch (error) {
          // Another process may have registered the same stable hub id concurrently.
          if (!(await listHubs())?.some?.(hub => hub.id === hubId)) throw error;
        }
      }
    })();
    try { await ready; }
    catch (error) { ready = null; throw error; }
  }

  function startReaper() {
    if (reaper) return;
    reaper = setInterval(async () => {
      pruneRates(rates, Date.now(), RATE_WINDOW_MS);
      pruneRates(reads, Date.now(), RATE_WINDOW_MS);
      for (const [userId, lastSeen] of seats) {
        if (Date.now() - lastSeen < idleMs) continue;
        try {
          await withSeat(userId, async () => {
            if (seats.get(userId) !== lastSeen) return;
            await ensureReady();
            await release(userId);
            seats.delete(userId);
            pairings.delete(userId);
          });
        } catch { /* Retry on the next sweep. */ }
      }
    }, Math.min(30_000, idleMs / 2));
    reaper.unref();
  }

  // ---- hub operations ----

  async function pairHub(userId, input, credential) {
    const request = {
      subject: { id: userId, kind: 'peer', scopes: subjectScopes(input) },
      target: { kind: 'hub', id: hubId, requirements: scopes }, path: hubPath,
    };
    const descriptor = await client.pair({ ...request, credential });
    pairings.set(userId, request);
    return descriptor;
  }

  // Register as a candidate, then ask LiveEngine to pair; it holds the request until another
  // player arrives or `waitMs` passes. The wait belongs to the engine, and a request dropped by
  // its caller (a cancel, a closed tab) leaves the engine's queue at once.
  async function pairPeer(userId, input, waitMs, signal) {
    // `arena` keeps these players to this hub: other players on the service are never candidates.
    const own = { ...subjectScopes(input), arena: hubId };
    const request = { subject: { id: userId, kind: 'peer', scopes: own }, target: { kind: 'peer', requirements: { ...scopes, arena: hubId } }, path: hubPath, waitMs };
    pairings.set(userId, { peer: true });
    try {
      if (!await call('getUser', userId)) await call('upsertUser', { id: userId, scopes: own });
      return await client.pair({ ...request, signal });
    } catch (error) {
      await release(userId).catch(() => {});
      pairings.delete(userId);
      if (error.status === 408) throw fail('No other player arrived in time', 408);
      throw error;
    }
  }

  async function pair(userId, input, credential, waitMs, signal) {
    await ensureReady();
    return linkKind === 'peer' ? pairPeer(userId, input, waitMs, signal) : pairHub(userId, input, credential);
  }

  async function renew(userId, credential) {
    await ensureReady();
    const request = pairings.get(userId);
    if (!request) return null;
    if (request.peer) {
      const current = await call('getUser', userId);
      if (!current?.serverId) return null;
      const ticket = await client.ticket(hubId, { userId, serverId: current.serverId, path: hubPath });
      return { link: 'peer-peer', room: current.serverId, userId, ticket: ticket.ticket, url: ticket.url, transports: ['webrtc', 'websocket'] };
    }
    return client.renewal(request)({ credential });
  }

  async function joinSeat(userId, input) {
    await ensureReady();
    await call('upsertServer', join.server);
    const current = await call('getUser', userId);
    if (current?.status === 'paired' && current.serverId) return { ok: true, userId, serverId: current.serverId, existing: true };
    await call('upsertUser', { id: userId, scopes: subjectScopes(input) });
    const result = await call('autoJoin', userId);
    if (!result?.ok) return result || { ok: false, reason: 'empty_response', userId };
    return { ...result, ...(join.extra || {}) };
  }

  // ---- HTTP ----

  function send(res, status, value) {
    if (res.destroyed || res.writableEnded) return;
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(value));
  }

  async function readJson(req) {
    if (Number(req.headers['content-length']) > bodyBytes) throw fail('Request body is too large', 413);
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > bodyBytes) throw fail('Request body is too large', 413);
      chunks.push(chunk);
    }
    const raw = Buffer.concat(chunks).toString('utf8');
    let value;
    try { value = raw ? JSON.parse(raw) : {}; }
    catch { throw fail('Expected a JSON object', 400); }
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail('Expected a JSON object', 400);
    return value;
  }

  // The hub's own 4xx answer (a refused credential, no matching hub) reaches the page; anything else is a server failure.
  function failure(res, error) {
    if (error.status === 503) return send(res, 503, { error: error.message });
    if (error.name === 'AbortError') return send(res, 409, { error: 'Pairing was cancelled' });
    if (error.status >= 400 && error.status < 500 && error.status !== 401) return send(res, error.status, { error: error.message });
    return send(res, 502, { error: 'LiveEngine request failed' });
  }

  function cors(req, res) {
    const origin = req.headers.origin;
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Access-Control-Max-Age', '600');
    // A public page reaching a hub API on a private address (the LAN) asks for this in its preflight.
    if (req.headers['access-control-request-private-network'] === 'true') res.setHeader('Access-Control-Allow-Private-Network', 'true');
  }

  function limited(table, limit, address) {
    const now = Date.now();
    const stamps = (table.get(address) || []).filter(time => now - time < RATE_WINDOW_MS);
    if (stamps.length >= limit) return true;
    stamps.push(now);
    table.set(address, stamps);
    return false;
  }

  function player(input) {
    const credential = typeof input.credential === 'string' && input.credential.length <= 4096 ? input.credential : undefined;
    return { credential };
  }

  async function pairRoute(req, res, input, renewing) {
    const { credential } = player(input);
    const controller = new AbortController();
    const userId = input.userId;
    // A page that goes away (cancel, closed tab) stops the wait, and the seat goes with it.
    const dropped = () => { if (!res.writableEnded) controller.abort(); };
    res.once('close', dropped);
    if (!renewing) pending.set(userId, controller);
    try {
      const waitMs = Math.min(Number.isSafeInteger(input.waitMs) && input.waitMs > 0 ? input.waitMs : peerWaitMs, peerMaxWaitMs);
      const descriptor = await withSeat(userId, async () => {
        if (!renewing && !pairings.has(userId) && pairings.size >= maxPlayers) throw fail('Too many players', 429);
        const result = renewing ? await renew(userId, credential) : await pair(userId, input, credential, waitMs, controller.signal);
        if (result) { seats.set(userId, Date.now()); startReaper(); }
        return result;
      });
      if (!descriptor) return send(res, 404, { error: 'Pair before renewing' });
      return send(res, 200, descriptor);
    } catch (error) { return failure(res, error); }
    finally {
      res.off('close', dropped);
      if (pending.get(userId) === controller) pending.delete(userId);
    }
  }

  async function leaveRoute(res, userId) {
    // Stop a pair that is still waiting first: the seat lock is held for the whole wait.
    pending.get(userId)?.abort();
    try {
      const result = await withSeat(userId, async () => {
        await ensureReady();
        const left = await release(userId);
        seats.delete(userId);
        pairings.delete(userId);
        return left;
      });
      return send(res, 200, result);
    } catch (error) { return failure(res, error); }
  }

  async function handle(req, res, route, url) {
    const input = req.method === 'POST' ? await readJson(req) : {};
    if (route === 'discover') {
      const kind = ['hub', 'peer', 'any'].includes(input.kind) ? input.kind : 'hub';
      const depth = Number.isInteger(input.depth) && input.depth >= 1 && input.depth <= 8 ? { depth: input.depth } : {};
      const requirements = input.requirements && typeof input.requirements === 'object' ? { requirements: input.requirements } : {};
      await ensureReady();
      return send(res, 200, await client.discover({ kind, ...depth, ...requirements, path: hubPath }));
    }
    if (route === 'watch') {
      const depth = Number.isInteger(input.depth) && input.depth >= 1 && input.depth <= 8 ? input.depth : 1;
      await ensureReady();
      const issued = await client.watchTicket({ path: hubPath, depth });
      return send(res, 200, { url: watchUrl, ticket: issued.ticket, expiresAt: issued.expiresAt, path: hubPath, depth });
    }
    if (typeof input.userId !== 'string' || !ID.test(input.userId)) return send(res, 400, { error: 'Invalid userId' });
    const userId = input.userId;
    if (route === 'pair' || route === 'renew') return pairRoute(req, res, input, route === 'renew');
    if (route === 'heartbeat') {
      const found = await withSeat(userId, () => {
        if (!seats.has(userId)) return false;
        seats.set(userId, Date.now());
        return true;
      });
      return found ? send(res, 200, { ok: true }) : send(res, 404, { error: 'Seat not found' });
    }
    if (route === 'leave') return leaveRoute(res, userId);
    if (route === 'join') {
      if (!join) return send(res, 404, { error: 'Not found' });
      const joined = await withSeat(userId, async () => {
        const result = await joinSeat(userId, input);
        if (result.ok) { seats.set(userId, Date.now()); startReaper(); }
        return result;
      });
      return send(res, joined.ok ? 200 : 409, joined);
    }
    return send(res, 404, { error: 'Not found' });
  }

  async function handleGet(req, res, route) {
    if (route === 'status') {
      return send(res, 200, { configured, hubId, mode, externalConfigured: Boolean(remote?.address), ...statusExtra });
    }
    if (route === 'live-client.mjs') {
      res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'no-cache' });
      return res.end(await readFile(liveClientFile));
    }
    // ping: how far the hub is from this API, and who is here. The page times its own round trip.
    if (!configured) throw fail(unconfigured, 503);
    const measured = await client.ping({ samples: 1 });
    return send(res, 200, { ok: true, hubId, hubPingMs: measured.avgMs, serverTimeMs: measured.serverTimeMs, waiting: pending.size, seats: seats.size });
  }

  function handler(req, res, next) {
    const url = new URL(req.url, 'http://hub-api.invalid');
    let pathname = url.pathname.replace(/\/+$/, '');
    if (prefix) {
      if (!pathname.startsWith(`${prefix}/`)) return next ? next() : send(res, 404, { error: 'Not found' });
      pathname = pathname.slice(prefix.length);
    }
    const route = pathname.replace(/^\//, '');
    if (!Object.hasOwn(ROUTES, route) || route.includes('/')) return next ? next() : send(res, 404, { error: 'Not found' });
    return (async () => {
      try {
        if (hostSet && !hostSet.has(String(req.headers.host).toLowerCase())) return send(res, 403, { error: 'Invalid Host header' });
        const origin = req.headers.origin;
        const method = req.method;
        // A page must come from an allowed Origin to POST; a GET without an Origin (a health check) is fine.
        if (origin ? !originAllowed(origin) : method === 'POST') return send(res, 403, { error: 'Cross-origin request rejected' });
        if (origin) cors(req, res);
        if (method === 'OPTIONS') { res.writeHead(204); return res.end(); }
        if (method !== ROUTES[route]) return send(res, 405, { error: 'Method not allowed' });
        const address = req.socket.remoteAddress;
        if (limited(method === 'POST' ? rates : reads, method === 'POST' ? requestLimit : readLimit, address)) {
          return send(res, 429, { error: 'Too many matchmaking requests' });
        }
        startReaper();
        if (method === 'GET') return await handleGet(req, res, route);
        return await handle(req, res, route, url);
      } catch (error) {
        if (error.status === 413 || error.status === 400) return send(res, error.status, { error: error.message });
        return failure(res, error);
      }
    })();
  }

  /** Publish the schemas, channels and hub now instead of on the first request. Starts the idle reaper. */
  handler.setup = async () => { startReaper(); await ensureReady(); };
  /** Stop the idle reaper. Seats are left to the hub's own expiry. */
  handler.close = () => { clearInterval(reaper); reaper = null; };
  /** The same handler, for a framework that wants a named function. */
  handler.handle = handler;
  /** Counts for status pages and tests. */
  handler.stats = () => ({ seats: seats.size, waiting: pending.size, pairings: pairings.size });
  return handler;
}

export default createHubApi;
