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
import { randomBytes, createHash, timingSafeEqual } from 'node:crypto';

const ROUTES = {
  status: 'GET', ping: 'GET', 'live-client.mjs': 'GET', login: 'POST', logout: 'POST', password: 'POST', me: 'GET', admin: 'GET', 'admin/accounts': 'GET', 'admin/connections': 'GET',
  discover: 'POST', pair: 'POST', renew: 'POST', leave: 'POST', heartbeat: 'POST', watch: 'POST', join: 'POST',
};
const ID = /^[a-zA-Z0-9._-]{1,64}$/;
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
 * @param {{bodyBytes?: number, requestsPerMinute?: number, readsPerMinute?: number, idleMs?: number, maxPeers?: number, maxPlayers?: number}} [options.limits]
 *   `maxPeers` caps the peers holding a pairing at once; `maxPlayers` is its deprecated alias.
 * @param {(input: object) => object} [options.subject] Request body to the scopes a player pairs with.
 * @param {{server: object, extra?: object}} [options.join] Enables the seat-based `join` route.
 * @param {object} [options.statusExtra] Added to the status answer.
 * @param {string} [options.watchUrl] Where the page opens the registry socket. Defaults to the hub's own address.
 * @param {string} [options.notConfigured] The 503 message when there is no client.
 * @param {true|false|'auto'} [options.secureCookies] Mark the session cookie `Secure`: always, never, or ('auto', the
 *   default) when the request reached this handler over TLS, or, with `trustProxy`, when `X-Forwarded-Proto` says https.
 * @param {boolean} [options.trustProxy] Believe `X-Forwarded-Proto` from the proxy in front. Only set it behind a proxy
 *   that overwrites the header, or a client could claim https on a plain connection.
 * @param {string} [options.peerScopeKey] The scope key that keeps peer pairings to this hub (default `arena`). It is
 *   added to every peer's scopes and requirements, so a user schema with `additionalProperties: false` must allow it.
 */
export function createHubApi(options = {}) {
  const {
    client, hubId, hubPath = [], mode = 'internal', remote, scopes = {}, schemas = [], channels, auth,
    peerLink = {}, roomCapacity = 2, allowedOrigins = [], allowedHosts, basePath = '', limits = {}, subject, join,
    adminConsole = false, statusExtra = {}, notConfigured = 'The hub API has no hub client configured.',
    secureCookies = 'auto', trustProxy = false, peerScopeKey = 'arena',
  } = options;
  if (![true, false, 'auto'].includes(secureCookies)) throw new TypeError('secureCookies must be true, false or "auto"');
  if (typeof peerScopeKey !== 'string' || !peerScopeKey) throw new TypeError('peerScopeKey must be a non-empty string');
  if (!Number.isSafeInteger(roomCapacity) || roomCapacity < 2) throw new TypeError('roomCapacity must be an integer of at least 2');
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
  // `maxPlayers` is the deprecated name of `maxPeers`; `maxPeers` wins when both are set.
  const maxPeers = positive(limits.maxPeers, positive(limits.maxPlayers, 4096));
  const originAllowed = originMatcher(allowedOrigins);
  const hostSet = allowedHosts ? new Set([...allowedHosts].map(value => String(value).toLowerCase())) : null;
  const prefix = `/${String(basePath).split('/').filter(Boolean).join('/')}`.replace(/^\/$/, '');
  const subjectScopes = typeof subject === 'function' ? subject : () => ({ ...scopes });
  const watchUrl = options.watchUrl || (client?.address ? `${client.address.replace(/^http/, 'ws')}/v1/live/registry` : '');
  const configured = Boolean(client) && !(mode === 'external' && !(remote?.address && remote?.token));
  const unconfigured = client ? 'An external hub needs remote.address and remote.token.' : notConfigured;

  const browserSessions = new Map();
  const cookieName = 'liveengine_session';
  function secureCookie(req) {
    if (secureCookies !== 'auto') return secureCookies;
    if (req.socket?.encrypted) return true;
    if (!trustProxy) return false;
    const forwarded = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim().toLowerCase();
    return forwarded === 'https';
  }
  const cookieAttributes = req => `Path=${prefix || '/'}; HttpOnly; SameSite=Strict${secureCookie(req) ? '; Secure' : ''}`;
  function browserToken(req) {
    const explicit = /^Bearer (.+)$/.exec(req.headers.authorization || '')?.[1];
    if (explicit) return explicit;
    const cookie = (req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith(`${cookieName}=`));
    const session = browserSessions.get(cookie?.slice(cookieName.length + 1));
    if (!session || session.expiresAt * 1000 <= Date.now()) return undefined;
    return session.token;
  }
  // Accounts live with the service that owns the hub. For an external hub that is the remote service,
  // which is also the one that checks the session the central forwards on a pair.
  let accountClient;
  function accounts() {
    if (mode !== 'external') return client;
    accountClient ||= new client.constructor(remote.address, { token: remote.token });
    return accountClient;
  }
  async function identity(req) {
    const token = browserToken(req);
    if (!token) throw fail('Sign in required', 401);
    try { return await accounts().request('/v1/sessions/me', { token }); }
    catch { throw fail('Sign in required', 401); }
  }
  async function admin(req) {
    const account = await identity(req);
    if ((account.account || account).role !== 'admin') throw fail('Administrator access required', 403);
    return browserToken(req);
  }
  const rates = new Map();
  const reads = new Map();
  const seats = new Map(); // userId -> { lastSeen, hash }; never retain the bearer token
  const pairings = new Map(); // what `renew` needs to pair a known player again; credentials are never kept
  const createdUsers = new Set(); // only users created by this handler may be removed by it
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
    if (createdUsers.has(userId)) {
      await call('removeUser', userId);
      createdUsers.delete(userId);
    }
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
      for (const [userId, seat] of seats) {
        if (Date.now() - seat.lastSeen < idleMs) continue;
        try {
          await withSeat(userId, async () => {
            if (seats.get(userId) !== seat) return;
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

  function authorized(userId, token) {
    const hash = createHash('sha256').update(typeof token === 'string' && token.length <= 1024 ? token : '').digest();
    const expected = seats.get(userId)?.hash || Buffer.alloc(32);
    return timingSafeEqual(hash, expected) && seats.has(userId);
  }

  function recordSeat(userId, token) {
    seats.set(userId, { lastSeen: Date.now(), hash: createHash('sha256').update(token).digest() });
    startReaper();
  }

  const refusedSeat = () => fail('Seat authorization failed', 403);

  // ---- hub operations ----

  async function pairHub(userId, input, credential) {
    const existing = await call('getUser', userId);
    const request = {
      subject: { id: userId, kind: 'peer', scopes: subjectScopes(input) },
      target: { kind: 'hub', id: hubId, requirements: scopes, capacity: roomCapacity }, path: hubPath,
    };
    const descriptor = await client.pair({ ...request, credential });
    if (!existing) createdUsers.add(userId);
    pairings.set(userId, request);
    return descriptor;
  }

  // Register as a candidate, then ask LiveEngine to pair; it holds the request until another
  // player arrives or `waitMs` passes. The wait belongs to the engine, and a request dropped by
  // its caller (a cancel, a closed tab) leaves the engine's queue at once.
  async function pairPeer(userId, input, credential, waitMs, signal) {
    // `peerScopeKey` (default `arena`) keeps these peers to this hub: other peers on the service are never candidates.
    const own = { ...subjectScopes(input), [peerScopeKey]: hubId };
    const request = { subject: { id: userId, kind: 'peer', scopes: own }, target: { kind: 'peer', requirements: { ...scopes, [peerScopeKey]: hubId }, capacity: roomCapacity }, path: hubPath, waitMs };
    pairings.set(userId, { peer: true });
    try {
      if (!await call('getUser', userId)) {
        await call('upsertUser', { id: userId, scopes: own });
        createdUsers.add(userId);
      }
      return await client.pair({ ...request, credential, signal });
    } catch (error) {
      await release(userId).catch(() => {});
      pairings.delete(userId);
      if (error.status === 408) throw fail('No other peer arrived in time', 408);
      throw error;
    }
  }

  async function pair(userId, input, credential, waitMs, signal) {
    await ensureReady();
    return linkKind === 'peer' ? pairPeer(userId, input, credential, waitMs, signal) : pairHub(userId, input, credential);
  }

  async function renew(userId, credential) {
    await ensureReady();
    const request = pairings.get(userId);
    if (!request) return null;
    if (request.peer) {
      const current = await call('getUser', userId);
      if (!current?.serverId) return null;
      const ticket = await client.ticket(hubId, { userId, serverId: current.serverId, path: hubPath, credential });
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
    if (!current) createdUsers.add(userId);
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
    if (error.status === 429 && Number.isSafeInteger(error.retryAfter) && !res.headersSent) res.setHeader('Retry-After', String(error.retryAfter));
    if (error.status === 401) return send(res, 401, { error: error.message });
    if (error.status === 503) return send(res, 503, { error: error.message });
    if (error.name === 'AbortError') return send(res, 409, { error: 'Pairing was cancelled' });
    if (error.status >= 400 && error.status < 500 && error.status !== 401) return send(res, error.status, { error: error.message });
    return send(res, 502, { error: 'LiveEngine request failed' });
  }

  function cors(req, res) {
    const origin = req.headers.origin;
    res.setHeader('Vary', 'Origin');
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
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
    const credential = browserToken(req) || player(input).credential;
    const controller = new AbortController();
    const userId = input.userId;
    // A page that goes away (cancel, closed tab) stops the wait, and the seat goes with it.
    const dropped = () => { if (!res.writableEnded) controller.abort(); };
    res.once('close', dropped);
    if (!renewing && pending.has(userId)) return send(res, 403, { error: 'Seat authorization failed' });
    if (!renewing) pending.set(userId, controller);
    try {
      const waitMs = Math.min(Number.isSafeInteger(input.waitMs) && input.waitMs > 0 ? input.waitMs : peerWaitMs, peerMaxWaitMs);
      const descriptor = await withSeat(userId, async () => {
        if ((renewing || seats.has(userId)) && !authorized(userId, input.seatToken)) throw refusedSeat();
        if (!renewing && !seats.has(userId)) {
          await ensureReady();
          if ((await call('getUser', userId))?.status === 'paired') throw refusedSeat();
        }
        if (!renewing && !pairings.has(userId) && pairings.size >= maxPeers) throw fail('Too many peers', 429);
        const { seatToken: ignored, ...safeInput } = input;
        const result = (renewing || seats.has(userId)) ? await renew(userId, credential) : await pair(userId, safeInput, credential, waitMs, controller.signal);
        if (!result) return result;
        const token = seats.has(userId) ? input.seatToken : randomBytes(32).toString('hex');
        recordSeat(userId, token);
        return renewing ? result : { ...result, seatToken: token };
      });
      if (!descriptor) return send(res, 404, { error: 'Pair before renewing' });
      return send(res, 200, descriptor);
    } catch (error) { return failure(res, error); }
    finally {
      res.off('close', dropped);
      if (pending.get(userId) === controller) pending.delete(userId);
    }
  }

  async function leaveRoute(res, userId, token) {
    if (!authorized(userId, token)) return send(res, 403, { error: 'Seat authorization failed' });
    try {
      const result = await withSeat(userId, async () => {
        if (!authorized(userId, token)) throw refusedSeat();
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
    const input = ['POST', 'PATCH'].includes(req.method) ? await readJson(req) : {};
    if (route === 'login') {
      const result = await accounts().login(input.username, input.password);
      for (const [key, session] of browserSessions) if (session.expiresAt * 1000 <= Date.now()) browserSessions.delete(key);
      if (browserSessions.size >= 4096) browserSessions.delete(browserSessions.keys().next().value);
      const id = randomBytes(32).toString('hex');
      browserSessions.set(id, { token: result.token, expiresAt: result.expiresAt });
      res.setHeader('Set-Cookie', `${cookieName}=${id}; ${cookieAttributes(req)}`);
      return send(res, 200, { account: result.account, expiresAt: result.expiresAt });
    }
    if (route === 'logout') {
      const token = browserToken(req);
      // A session the service already ended (a password change, a ban) is gone either way: the cookie still goes.
      if (token) await accounts().request('/v1/sessions', { method: 'DELETE', token }).catch(() => {});
      const cookie = (req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith(`${cookieName}=`));
      if (cookie) browserSessions.delete(cookie.slice(cookieName.length + 1));
      res.setHeader('Set-Cookie', `${cookieName}=; ${cookieAttributes(req)}; Max-Age=0`);
      return send(res, 200, { ok: true });
    }
    if (route === 'password') {
      const token = browserToken(req);
      if (!token) throw fail('Sign in required', 401);
      if (typeof input.current !== 'string' || typeof input.next !== 'string') throw fail('Expected current and next passwords', 400);
      await accounts().request('/v1/accounts/me/password', { method: 'POST', token, body: { current: input.current, next: input.next } });
      // Every session of the account ended with the change, this one included.
      const cookie = (req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith(`${cookieName}=`));
      if (cookie) browserSessions.delete(cookie.slice(cookieName.length + 1));
      res.setHeader('Set-Cookie', `${cookieName}=; ${cookieAttributes(req)}; Max-Age=0`);
      return send(res, 200, { ok: true });
    }
    if (route.startsWith('admin/')) {
      if (!adminConsole) throw fail('Not found', 404);
      const token = await admin(req);
      const suffix = route.slice(6);
      const target = `/v1/${suffix}${url.search}`;
      return send(res, 200, await accounts().request(target, { method: req.method, token, ...(['POST', 'PATCH'].includes(req.method) ? { body: input } : {}) }));
    }
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
        if (!authorized(userId, input.seatToken)) return false;
        recordSeat(userId, input.seatToken);
        return true;
      });
      return found ? send(res, 200, { ok: true }) : send(res, 403, { error: 'Seat authorization failed' });
    }
    if (route === 'leave') return leaveRoute(res, userId, input.seatToken);
    if (route === 'join') {
      if (!join) return send(res, 404, { error: 'Not found' });
      const joined = await withSeat(userId, async () => {
        if (seats.has(userId) && !authorized(userId, input.seatToken)) throw refusedSeat();
        const { seatToken: ignored, ...safeInput } = input;
        if (!seats.has(userId)) {
          await ensureReady();
          if ((await call('getUser', userId))?.status === 'paired') throw refusedSeat();
        }
        const result = await joinSeat(userId, safeInput);
        if (!result.ok) return result;
        const token = seats.has(userId) ? input.seatToken : randomBytes(32).toString('hex');
        recordSeat(userId, token);
        return { ...result, seatToken: token };
      });
      return send(res, joined.ok ? 200 : 409, joined);
    }
    return send(res, 404, { error: 'Not found' });
  }

  async function handleGet(req, res, route) {
    if (route === 'me') return send(res, 200, await identity(req));
    if (route === 'admin') {
      if (!adminConsole) throw fail('Not found', 404);
      // The page is a static shell with no data and no secrets; it shows its own sign-in form, and
      // every route it reads (`admin/accounts`, `admin/connections`) needs an administrator session.
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Security-Policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'self'" });
      return res.end(await readFile(new URL('./hub-admin.html', import.meta.url)));
    }
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
    const adminRoute = /^admin\/(accounts|connections)(\/[a-zA-Z0-9._-]+)?$/.test(route);
    if (!Object.hasOwn(ROUTES, route) && !adminRoute) return next ? next() : send(res, 404, { error: 'Not found' });
    return (async () => {
      try {
        if (hostSet && !hostSet.has(String(req.headers.host).toLowerCase())) return send(res, 403, { error: 'Invalid Host header' });
        const origin = req.headers.origin;
        const method = req.method;
        // A page must come from an allowed Origin to POST; a GET without an Origin (a health check) is fine.
        if (origin ? !originAllowed(origin) : !['GET', 'HEAD', 'OPTIONS'].includes(method)) return send(res, 403, { error: 'Cross-origin request rejected' });
        if (origin) cors(req, res);
        if (method === 'OPTIONS') { res.writeHead(204); return res.end(); }
        if (!(adminRoute ? (route.startsWith('admin/accounts') ? ['GET', 'POST', 'PATCH', 'DELETE'] : ['GET', 'DELETE']).includes(method) : method === ROUTES[route])) return send(res, 405, { error: 'Method not allowed' });
        const address = req.socket.remoteAddress;
        if (limited(method === 'POST' ? rates : reads, method === 'POST' ? requestLimit : readLimit, address)) {
          return send(res, 429, { error: 'Too many matchmaking requests' });
        }
        startReaper();
        if (method === 'GET' && !adminRoute) return await handleGet(req, res, route);
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
