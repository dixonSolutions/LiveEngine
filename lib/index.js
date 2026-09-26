'use strict';

const { EventEmitter } = require('node:events');
const path = require('node:path');

function loadNative() {
  const candidates = [
    path.join(__dirname, '..', 'build', 'Release', 'liveengine.node'),
    path.join(__dirname, '..', 'build', 'Debug', 'liveengine.node'),
  ];
  let lastError;
  for (const p of candidates) {
    try { return require(p); } catch (err) { lastError = err; }
  }
  throw new Error(`LiveEngine native addon not built (run \`npm run build\`): ${lastError && lastError.message}`);
}

const native = loadNative();

const MUTATING = new Set([
  'upsertUser', 'upsertUsers', 'removeUser', 'setUserStatus',
  'upsertServer', 'upsertServers', 'removeServer', 'setServerStatus',
  'advertise', 'advertiseAll', 'join', 'leave', 'autoJoin', 'matchAll',
  'generateUsers', 'clear',
]);

const PASSTHROUGH = [
  'getUser', 'listUsers', 'getServer', 'listServers',
  'findServersForUser', 'findUsersForServer', 'fits',
  'pendingEvents', 'stats', 'reseed',
];

/**
 * LiveEngine — a user hub and a server hub, and the pairing between them.
 *
 * Every mutating call drains the native event queue and re-emits each event
 * as `engine.emit(event.type, event)` plus a catch-all `engine.emit('event', event)`.
 * Set `{ autoEmit: false }` to drain manually with `drainEvents()`.
 */
class LiveEngine extends EventEmitter {
  constructor(options = {}) {
    super();
    const { seed = 0, autoEmit = true } = typeof options === 'number' ? { seed: options } : options;
    this._native = new native.NativeEngine(seed);
    this.autoEmit = autoEmit;
  }

  _flush() {
    if (!this.autoEmit) return;
    const events = this._native.drainEvents();
    for (const ev of events) {
      this.emit(ev.type, ev);
      this.emit('event', ev);
    }
  }

  /** Drain queued events without emitting them (useful with autoEmit=false). */
  drainEvents() { return this._native.drainEvents(); }

  /** Convenience: add a user then immediately advertise fitting servers to it. */
  joinHub(user, options) {
    const created = this._native.upsertUser(user);
    const count = this._native.advertise(user.id, options);
    this._flush();
    return { created, advertised: count };
  }

  /** Convenience: find + join in one call, returning the full server object on success. */
  pair(userId, options) {
    const r = this._native.autoJoin(userId, options);
    this._flush();
    return r.ok ? { ...r, server: this._native.getServer(r.serverId) } : r;
  }

  /** Runs matchAll on the next tick and every `intervalMs` until stop() is called. */
  startAutoMatch(intervalMs = 250, options) {
    this.stopAutoMatch();
    this._timer = setInterval(() => { this.matchAll(options); }, intervalMs);
    if (this._timer.unref) this._timer.unref();
    return this;
  }

  stopAutoMatch() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    return this;
  }
}

for (const name of MUTATING) {
  LiveEngine.prototype[name] = function (...args) {
    const out = this._native[name](...args);
    this._flush();
    return out;
  };
}
for (const name of PASSTHROUGH) {
  LiveEngine.prototype[name] = function (...args) { return this._native[name](...args); };
}

module.exports = { LiveEngine, NativeEngine: native.NativeEngine, nativeVersion: native.version };
module.exports.default = LiveEngine;
