'use strict';

const { EventEmitter } = require('node:events');
const { performance } = require('node:perf_hooks');

const HUB_PROTOCOL = 'liveengine.hub/v1';
const roundMs = value => (Math.round(value * 1000) / 1000);

/** Client for both hosted hubs and remote registries. Keep tokens on the server. */
class HubClient {
  constructor(address, { token, timeoutMs = 5000, maxResponseBytes = 1048576, fetch: fetchImpl = globalThis.fetch } = {}) {
    const url = new URL(address);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
      throw new TypeError('Hub address must be an HTTP(S) base URL without credentials, query or fragment');
    }
    if (typeof token !== 'string' || !token) throw new TypeError('Hub token is required');
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1 || !Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 1) {
      throw new TypeError('Invalid client limits');
    }
    this.address = url.href.replace(/\/$/, '');
    Object.defineProperty(this, '_token', { value: token });
    this.timeoutMs = timeoutMs;
    this.maxResponseBytes = maxResponseBytes;
    this._fetch = fetchImpl;
  }

  async request(route, { method = 'GET', body, signal, token = this._token, timeoutMs = this.timeoutMs } = {}) {
    const timeout = AbortSignal.timeout(timeoutMs);
    const response = await this._fetch(`${this.address}${route}`, {
      method,
      redirect: 'error',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
    let size = 0;
    const chunks = [];
    if (response.body) {
      const reader = response.body.getReader();
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > this.maxResponseBytes) throw new Error('Hub response exceeds configured limit');
          chunks.push(value);
        }
      } catch (error) {
        await reader.cancel().catch(() => {});
        throw error;
      } finally { reader.releaseLock(); }
    }
    const content = Buffer.concat(chunks).toString('utf8');
    let data;
    try { data = content ? JSON.parse(content) : null; }
    catch { throw new Error(`Hub returned invalid JSON (HTTP ${response.status})`); }
    if (!response.ok) {
      const error = new Error(data?.error || `Hub request failed (HTTP ${response.status})`);
      error.status = response.status;
      // Seconds to wait, from a `429` sign-in lockout's `Retry-After`.
      const retryAfter = Number(response.headers?.get?.('retry-after'));
      if (Number.isSafeInteger(retryAfter) && retryAfter >= 0) error.retryAfter = retryAfter;
      throw error;
    }
    return data;
  }

  login(username, password) { return this.request('/v1/sessions', { method: 'POST', body: { username, password } }); }
  logout() { return this.request('/v1/sessions', { method: 'DELETE' }); }
  me() { return this.request('/v1/sessions/me'); }
  changePassword(current, next) { return this.request('/v1/accounts/me/password', { method: 'POST', body: { current, next } }); }
  listAccounts() { return this.request('/v1/accounts'); }
  createAccount(account) { return this.request('/v1/accounts', { method: 'POST', body: account }); }
  getAccount(username) { return this.request(`/v1/accounts/${encodeURIComponent(username)}`); }
  updateAccount(username, changes) { return this.request(`/v1/accounts/${encodeURIComponent(username)}`, { method: 'PATCH', body: changes }); }
  deleteAccount(username) { return this.request(`/v1/accounts/${encodeURIComponent(username)}`, { method: 'DELETE' }); }
  /** List live connections. Filters left undefined or null are not sent. */
  connections(filters = {}) {
    const query = new URLSearchParams(Object.entries(filters || {}).filter(([, value]) => value !== undefined && value !== null).map(([key, value]) => [key, String(value)])).toString();
    return this.request(`/v1/connections${query ? `?${query}` : ''}`);
  }
  kick(id) { return this.request(`/v1/connections/${encodeURIComponent(id)}`, { method: 'DELETE' }); }

  async contract() {
    const contract = await this.request('/v1/contract');
    if (contract?.protocol !== HUB_PROTOCOL) throw new Error('Incompatible hub protocol');
    return contract;
  }
  async ping({ samples = 5, intervalMs = 0 } = {}) {
    if (!Number.isInteger(samples) || samples < 1 || samples > 32) {
      throw new TypeError('samples must be an integer between 1 and 32');
    }
    if (!Number.isFinite(intervalMs) || intervalMs < 0) {
      throw new TypeError('intervalMs must be a non-negative number');
    }
    const rtts = [];
    let serverTimeMs;
    for (let index = 0; index < samples; index++) {
      const started = performance.now();
      const response = await this.request('/v1/ping');
      if (response?.protocol !== HUB_PROTOCOL || response?.pong !== true) {
        throw new Error('Incompatible ping response');
      }
      rtts.push(roundMs(performance.now() - started));
      serverTimeMs = response.serverTimeMs;
      if (intervalMs > 0 && index < samples - 1) {
        await new Promise(resolve => setTimeout(resolve, intervalMs));
      }
    }
    const minMs = Math.min(...rtts);
    const maxMs = Math.max(...rtts);
    const avgMs = rtts.reduce((sum, value) => sum + value, 0) / rtts.length;
    const jitterMs = rtts.length < 2 ? 0 : rtts.slice(1)
      .reduce((sum, value, index) => sum + Math.abs(value - rtts[index]), 0) / (rtts.length - 1);
    return {
      protocol: HUB_PROTOCOL,
      samples: rtts,
      minMs: roundMs(minMs),
      maxMs: roundMs(maxMs),
      avgMs: roundMs(avgMs),
      jitterMs: roundMs(jitterMs),
      serverTimeMs,
    };
  }
  tree(depth = 1) { return this.request(`/v1/tree?depth=${encodeURIComponent(depth)}`); }
  route(path, op, body = {}) { return this.request('/v1/route', { method: 'POST', body: { path, op, body } }); }
  listTemplates() { return this.request('/v1/templates'); }
  createTemplate(template) { return this.request('/v1/templates', { method: 'POST', body: template }); }
  getTemplate(name) { return this.request(`/v1/templates/${encodeURIComponent(name)}`); }
  removeTemplate(name) { return this.request(`/v1/templates/${encodeURIComponent(name)}`, { method: 'DELETE' }); }
  listSchemas() { return this.request('/v1/schemas'); }
  createSchema(schema) { return this.request('/v1/schemas', { method: 'POST', body: schema }); }
  getSchema(id) { return this.request(`/v1/schemas/${encodeURIComponent(id)}`); }
  removeSchema(id) { return this.request(`/v1/schemas/${encodeURIComponent(id)}`, { method: 'DELETE' }); }
  exportRegistry() { return this.request('/v1/registry/export'); }
  list() { return this.request('/v1/hubs'); }
  register(hub) { return this.request('/v1/hubs', { method: 'POST', body: hub }); }
  get(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}`); }
  remove(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}`, { method: 'DELETE' }); }
  async call(id, method, args = [], { signal } = {}) {
    if (!Array.isArray(args)) throw new TypeError('args must be an array');
    const response = await this.request(`/v1/hubs/${encodeURIComponent(id)}/call`, { method: 'POST', body: { method, args }, signal });
    return response.result;
  }
  /**
   * Issue a live ticket for a user already paired in `serverId`. Tickets are short lived and
   * single use; browsers hold tickets, never the administrator token. `path` names the
   * central namespace that holds the hub. Pass `credential` for a hub that sets `auth`.
   */
  ticket(id, { userId, serverId, role = 'peer', credential, path = [] } = {}) {
    if (typeof userId !== 'string' || typeof serverId !== 'string') throw new TypeError('userId and serverId are required');
    if (!Array.isArray(path)) throw new TypeError('path must be an array');
    const body = { userId, serverId, role, ...(credential === undefined ? {} : { credential }) };
    if (path.length === 0) return this.request(`/v1/hubs/${encodeURIComponent(id)}/tickets`, { method: 'POST', body });
    return this.route(path, 'ticket', { id, ...body });
  }
  /** Issue a read-only registry watch ticket scoped to `path` and `depth`. */
  watchTicket({ path = [], depth = 1 } = {}) {
    if (!Array.isArray(path)) throw new TypeError('path must be an array');
    return this.request('/v1/tickets/watch', { method: 'POST', body: { path, depth } });
  }
  /** Find hubs and peers that match `requirements`. `kind` is "hub", "peer" or "any". */
  discover({ kind = 'any', path = [], depth, requirements } = {}) {
    return this.request('/v1/discover', { method: 'POST', body: { kind, path, ...(depth === undefined ? {} : { depth }), ...(requirements === undefined ? {} : { requirements }) } });
  }
  /**
   * Pair a subject with a hub or peer and return the connection descriptor for `LiveClient`:
   * `{ link, room, userId, ticket, url, transports }`. `credential` is the subject's
   * credential for a hub that sets `auth`; it may also be given as `subject.credential`.
   * `target.capacity` sets the room capacity (default 2, bounded by the service).
   * `waitMs` makes a peer with no partner yet wait for one, up to the service's maximum, and
   * the request answers 408 if none arrives. The client allows that wait on top of `timeoutMs`.
   * Aborting `signal` drops the request, which also takes a waiting peer out of the queue.
   */
  async pair({ subject, target = { kind: 'any' }, path = [], credential, waitMs, signal } = {}) {
    if (!subject || typeof subject.id !== 'string') throw new TypeError('subject.id is required');
    if (waitMs !== undefined && !(Number.isSafeInteger(waitMs) && waitMs >= 0)) throw new TypeError('waitMs must be a non-negative integer');
    const joined = { kind: 'peer', scopes: {}, ...subject, ...(credential === undefined ? {} : { credential }) };
    const descriptor = await this.request('/v1/pair', {
      method: 'POST',
      body: { path, subject: joined, target, ...(waitMs === undefined ? {} : { waitMs }) },
      timeoutMs: this.timeoutMs + (waitMs ?? 0),
      signal,
    });
    return { userId: joined.id, ...descriptor };
  }
  /**
   * A `renew` function for `LiveClient`: it pairs again with `request` and returns a fresh
   * descriptor, so a reconnecting client gets a new single-use ticket. A credential given by
   * the client replaces the one in `request`. Run it where the administrator token lives.
   */
  renewal(request) {
    return async ({ credential } = {}) => this.pair({ ...request, ...(credential === undefined ? {} : { credential }) });
  }
  /**
   * Subscribe to the live registry. Resolves with a watch after the first snapshot. The watch
   * emits `snapshot`, `added`, `removed`, `updated` and `status` (also `event` for each), then
   * `close`. A gap in sequence numbers requests a fresh snapshot automatically. Pass `ticket`
   * (from `watchTicket`) to watch without the administrator token.
   */
  watchRegistry({ path = [], depth = 1, ticket, WebSocket: WebSocketImpl = globalThis.WebSocket, timeoutMs = this.timeoutMs } = {}) {
    if (typeof WebSocketImpl !== 'function') throw new Error('No WebSocket implementation available');
    const url = new URL('/v1/live/registry', this.address);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    const watch = new RegistryWatch();
    const socket = new WebSocketImpl(url.href);
    watch._socket = socket;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { fail(new Error('Registry watch timed out')); }, timeoutMs);
      let settled = false;
      const fail = error => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        try { socket.close(); } catch { /* already closed */ }
        reject(error);
      };
      socket.addEventListener('open', () => {
        const auth = ticket === undefined ? { type: 'auth', token: this._token } : { type: 'auth', ticket };
        socket.send(JSON.stringify({ ...auth, path, depth }));
      });
      socket.addEventListener('message', event => {
        let message;
        try { message = JSON.parse(String(event.data)); } catch { return; }
        watch._handle(message);
        if (!settled && message.type === 'snapshot') { settled = true; clearTimeout(timer); resolve(watch); }
      });
      socket.addEventListener('error', () => fail(new Error('Registry watch failed')));
      socket.addEventListener('close', () => {
        if (!settled) return fail(new Error('Hub closed the registry watch (check the token or ticket, path and depth)'));
        watch.closed = true;
        watch.emit('close');
      });
    });
  }
  importRegistry(registry) { return this.request('/v1/registries/import', { method: 'POST', body: registry }); }
  /** The links this service keeps open to rooms of remote hubs, with their status and frame counters. */
  listLinks() { return this.request('/v1/links'); }
  /**
   * Open a link to a room of a remote hub. `remoteToken` is a link credential the remote service
   * issued with `issueLinkCredential`; it is never echoed back.
   */
  createLink(link) { return this.request('/v1/links', { method: 'POST', body: link }); }
  getLink(id) { return this.request(`/v1/links/${encodeURIComponent(id)}`); }
  deleteLink(id) { return this.request(`/v1/links/${encodeURIComponent(id)}`, { method: 'DELETE' }); }
  /** Issue a credential another service's link uses to reach hub `id`. The token is shown once. */
  issueLinkCredential(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}/link-credentials`, { method: 'POST', body: {} }); }
  /** Revoke every link credential of hub `id`. */
  revokeLinkCredentials(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}/link-credentials`, { method: 'DELETE' }); }
}

/** A live registry subscription. `snapshot` is the latest snapshot message; diffs follow it in order. */
class RegistryWatch extends EventEmitter {
  constructor() {
    super();
    this.seq = -1;
    this.snapshot = null;
    this.closed = false;
    this._socket = null;
  }
  /** Ask the hub for a fresh snapshot. */
  resnapshot() {
    if (!this.closed && this._socket.readyState === 1) this._socket.send(JSON.stringify({ type: 'resnapshot' }));
  }
  close() {
    this._socket.close();
  }
  _handle(message) {
    if (message.type === 'snapshot') {
      this.seq = message.seq;
      this.snapshot = message;
    } else if (typeof message.seq === 'number') {
      if (message.seq !== this.seq + 1) { this.resnapshot(); return; }
      this.seq = message.seq;
    } else return;
    this.emit(message.type, message);
    this.emit('event', message);
  }
}

module.exports = { HubClient, RegistryWatch, HUB_PROTOCOL };
