'use strict';

const HUB_PROTOCOL = 'liveengine.hub/v1';

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

  async request(route, { method = 'GET', body, signal } = {}) {
    const timeout = AbortSignal.timeout(this.timeoutMs);
    const response = await this._fetch(`${this.address}${route}`, {
      method,
      redirect: 'error',
      headers: { authorization: `Bearer ${this._token}`, 'content-type': 'application/json' },
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
      throw error;
    }
    return data;
  }

  async contract() {
    const contract = await this.request('/v1/contract');
    if (contract?.protocol !== HUB_PROTOCOL) throw new Error('Incompatible hub protocol');
    return contract;
  }
  list() { return this.request('/v1/hubs'); }
  register(hub) { return this.request('/v1/hubs', { method: 'POST', body: hub }); }
  get(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}`); }
  remove(id) { return this.request(`/v1/hubs/${encodeURIComponent(id)}`, { method: 'DELETE' }); }
  async call(id, method, args = [], { signal } = {}) {
    if (!Array.isArray(args)) throw new TypeError('args must be an array');
    const response = await this.request(`/v1/hubs/${encodeURIComponent(id)}/call`, { method: 'POST', body: { method, args }, signal });
    return response.result;
  }
  importRegistry(registry) { return this.request('/v1/registries/import', { method: 'POST', body: registry }); }
}

module.exports = { HubClient, HUB_PROTOCOL };
