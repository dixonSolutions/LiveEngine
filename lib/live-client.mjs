// LiveEngine live client. Dependency-free ESM for browsers and Node 22+.
//
// A game passes the descriptor it got from `pair` to `LiveClient` and then only sends and
// receives frames. The client owns the rest: the WebSocket to the hub, the first-frame
// ticket authentication, retry with backoff, a fresh ticket on every reconnect, and, for
// peer-to-peer pairings, a WebRTC data channel to each peer (perfect negotiation, signalled
// through the hub) with automatic fallback to the hub relay.
//
// Frames use the fixed envelope {v, type, channel, to?, seq, payload}. The hub stamps `from`.
// Payloads, channel names and frame types are the application's own; nothing here knows them.
//
// Direct frames never touch the hub, so the hub's channel schemas and rate limits do not
// apply to them. Frames sent to one peer move to the data channel once it opens, so a frame
// sent just before the switch can arrive after one sent just after it. A broadcast goes through
// the hub unless the `directBroadcast` option is set, which sends it over each open data
// channel and relays a copy only to the peers that have none.

const ENVELOPE_VERSION = 1;
const SIGNAL_CHANNEL = 'signal';
const SERVER_TYPES = new Set(['welcome', 'presence', 'error', 'auth']);
const encoder = new TextEncoder();

const DEFAULTS = {
  retries: 5,
  retryBaseMs: 250,
  retryMaxMs: 8000,
  connectTimeoutMs: 10000,
  directTimeoutMs: 10000,
  maxFrameBytes: 65536,
  queueLimit: 64,
};

function fatal(message) {
  const error = new Error(message);
  error.fatal = true;
  return error;
}

function checkDescriptor(descriptor) {
  if (!descriptor || typeof descriptor !== 'object') throw new TypeError('A connection descriptor from pair is required');
  if (typeof descriptor.ticket !== 'string' || !descriptor.ticket) throw new TypeError('descriptor.ticket is required');
  if (typeof descriptor.url !== 'string') throw new TypeError('descriptor.url is required');
  const url = new URL(descriptor.url);
  if (url.protocol === 'http:') url.protocol = 'ws:';
  else if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') throw new TypeError('descriptor.url must be a ws(s) or http(s) URL');
  return { ...descriptor, url: url.href };
}

/**
 * One peer-to-peer link: a negotiated WebRTC data channel to `peerId`, with the hub relay as
 * the fallback. Create it with {@link connectPeer}.
 */
export class PeerLink {
  constructor(host, peerId, options = {}) {
    this.host = host;
    this.peer = peerId;
    // The lexicographically smaller id yields on an offer collision. Both sides compute
    // the same answer, so exactly one of them is polite.
    this.polite = host.id < peerId;
    this.directTimeoutMs = options.directTimeoutMs ?? host._options?.directTimeoutMs ?? DEFAULTS.directTimeoutMs;
    this.RTC = options.RTCPeerConnection ?? host._options?.RTCPeerConnection ?? globalThis.RTCPeerConnection;
    this.iceServers = options.iceServers ?? host.iceServers ?? [];
    this.pc = null;
    this.channel = null;
    this.closed = false;
    this.failure = null;
    this._wasOpen = false;
    this._making = false;
    this._ignoreOffer = false;
    this._restarted = false;
    this._chain = Promise.resolve();
    this._timer = null;
    this._settle = () => {};
    /** Resolves with the first settled transport: "direct" once the channel opens, else "relay". */
    this.ready = new Promise(resolve => { this._settle = resolve; });
  }

  /** "direct" while the data channel is open, otherwise "relay". */
  get transport() {
    return this.channel?.readyState === 'open' ? 'direct' : 'relay';
  }

  start() {
    if (this.pc || this.closed) return this;
    if (typeof this.RTC !== 'function') return this._fallback('WebRTC is not available');
    try {
      const pc = new this.RTC({ iceServers: this.iceServers });
      this.pc = pc;
      pc.onnegotiationneeded = () => this._negotiate();
      pc.onicecandidate = event => {
        if (event?.candidate) this.host._signal(this.peer, 'signal.ice', { candidate: event.candidate });
      };
      pc.oniceconnectionstatechange = () => this._iceChanged();
      // Both sides create the same negotiated channel, so no side waits for `ondatachannel`.
      const channel = pc.createDataChannel('liveengine', { negotiated: true, id: 0, ordered: true });
      this.channel = channel;
      channel.onopen = () => this._opened();
      channel.onclose = () => this._lost('data channel closed');
      channel.onerror = () => {};
      channel.onmessage = event => this._receive(event.data);
      this._timer = setTimeout(() => { if (this.transport !== 'direct') this._fallback('timed out'); }, this.directTimeoutMs);
    } catch (error) {
      this._fallback(error?.message || 'could not start WebRTC');
    }
    return this;
  }

  async _negotiate() {
    const pc = this.pc;
    try {
      this._making = true;
      await pc.setLocalDescription();
      const { type, sdp } = pc.localDescription;
      this.host._signal(this.peer, type === 'offer' ? 'signal.offer' : 'signal.answer', { description: { type, sdp } });
    } catch (error) {
      if (!this.closed) this._fallback(error?.message || 'negotiation failed');
    } finally {
      this._making = false;
    }
  }

  /** Apply a signalling frame from the peer. Frames are applied one at a time, in order. */
  handleSignal(type, payload) {
    this._chain = this._chain.then(() => this._signalled(type, payload)).catch(error => {
      if (!this.closed) this._fallback(error?.message || 'negotiation failed');
    });
    return this._chain;
  }

  async _signalled(type, payload) {
    const pc = this.pc;
    if (!pc || this.closed || !payload || typeof payload !== 'object') return;
    if (type === 'signal.ice') {
      try {
        if (payload.candidate) await pc.addIceCandidate(payload.candidate);
      } catch (error) {
        if (!this._ignoreOffer) throw error;
      }
      return;
    }
    const description = payload.description;
    if (!description || typeof description.sdp !== 'string' || (description.type !== 'offer' && description.type !== 'answer')) return;
    const collision = description.type === 'offer' && (this._making || pc.signalingState !== 'stable');
    this._ignoreOffer = !this.polite && collision;
    if (this._ignoreOffer) return;
    // For the polite side setRemoteDescription rolls back its own pending offer.
    await pc.setRemoteDescription(description);
    if (description.type === 'offer') {
      await pc.setLocalDescription();
      const { type: answerType, sdp } = pc.localDescription;
      this.host._signal(this.peer, 'signal.answer', { description: { type: answerType, sdp } });
    }
  }

  _iceChanged() {
    const state = this.pc?.iceConnectionState;
    if (state !== 'failed' || this.closed) return;
    if (!this._restarted && this.transport === 'direct' && typeof this.pc.restartIce === 'function') {
      this._restarted = true;
      this.pc.restartIce();
      return;
    }
    this._lost('connection failed');
  }

  _opened() {
    if (this.closed) return;
    clearTimeout(this._timer);
    this._wasOpen = true;
    this._settle('direct');
    this.host._transport(this.peer, 'direct');
  }

  _lost(reason) {
    this._fallback(reason);
  }

  /** Give the link up and leave the peer on the relay. */
  _fallback(reason) {
    if (this.closed) return this;
    const wasOpen = this._wasOpen;
    this.failure = reason;
    this.close();
    if (!wasOpen) this.host._emit('direct-failed', { peer: this.peer, reason });
    return this;
  }

  _receive(data) {
    if (typeof data !== 'string' || data.length > (this.host._options?.maxFrameBytes ?? DEFAULTS.maxFrameBytes)) return;
    let frame;
    try { frame = JSON.parse(data); } catch { return; }
    if (!frame || typeof frame !== 'object' || typeof frame.type !== 'string' || typeof frame.channel !== 'string'
      || typeof frame.v !== 'number' || !('payload' in frame)) return;
    if (SERVER_TYPES.has(frame.type) || frame.type.startsWith('signal.') || frame.type.startsWith('link.')) return;
    if (frame.to !== undefined && frame.to !== this.host.id) return;
    // The sender is the peer on this channel, whatever the frame claims.
    this.host._deliver({ ...frame, from: this.peer }, 'direct');
  }

  /** Send over the data channel. Returns false when it is not open, so the caller relays. */
  send(frame) {
    if (this.transport !== 'direct') return false;
    try {
      this.channel.send(JSON.stringify({ ...frame, from: this.host.id }));
      return true;
    } catch {
      return false;
    }
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this._timer);
    const wasOpen = this._wasOpen;
    this._wasOpen = false;
    try { this.channel?.close(); } catch { /* already closed */ }
    try { this.pc?.close(); } catch { /* already closed */ }
    if (this.host.links.get(this.peer) === this) this.host.links.delete(this.peer);
    this._settle('relay');
    if (wasOpen) this.host._transport(this.peer, 'relay', this.failure ?? 'closed');
  }
}

/**
 * Open a direct link to `peerId` for `client` and return it. Signalling goes through the
 * hub; if the data channel does not open, or later fails, frames to that peer keep using
 * the relay. `link.ready` resolves with the transport that settled first.
 *
 * Options: `RTCPeerConnection` (a constructor, for tests or non-browser runtimes),
 * `iceServers`, `directTimeoutMs`.
 */
export function connectPeer(client, peerId, options = {}) {
  if (typeof peerId !== 'string' || !peerId) throw new TypeError('peerId is required');
  if (peerId === client.id) throw new TypeError('Cannot connect to yourself');
  const existing = client.links.get(peerId);
  if (existing && !existing.closed) return existing;
  const link = new PeerLink(client, peerId, options);
  client.links.set(peerId, link);
  return link.start();
}

/** Connection to a hub room. Create it with the descriptor `pair` returned, then call `connect`. */
export class LiveClient {
  /**
   * @param descriptor the object `pair` returned: `{ ticket, url, userId, link, room, transports }`
   * @param options `credential` (string or function) and `renew({credential, previous})`
   *   for a fresh descriptor on reconnect; `WebSocket`, `RTCPeerConnection`; `direct`
   *   (default `"auto"`: only for peer-to-peer pairings; `true` forces, `false` disables);
   *   `reconnect` (default true), `retries`, `retryBaseMs`, `retryMaxMs`, `connectTimeoutMs`,
   *   `directTimeoutMs`, `maxFrameBytes`, `queueLimit`; `directBroadcast` (default false) sends
   *   broadcasts over open data channels instead of the hub.
   */
  constructor(descriptor, options = {}) {
    this._descriptor = checkDescriptor(descriptor);
    this._options = { ...DEFAULTS, reconnect: true, direct: 'auto', ...options };
    if (this._options.renew !== undefined && typeof this._options.renew !== 'function') throw new TypeError('renew must be a function');
    this._pending = this._descriptor;
    this._listeners = new Map();
    this._peers = new Set();
    this._identities = new Map();
    this.links = new Map();
    this._socket = null;
    this._state = 'idle';
    this._wantClosed = false;
    this._seq = 0;
    this._queue = [];
    this._iceServers = [];
    this._establishing = null;
    this._wake = null;
    this._identity = null;
  }

  /** Our own peer id, from the descriptor. */
  get id() { return this._descriptor.userId; }
  /** The room this client joined. */
  get room() { return this._descriptor.room; }
  /** "idle", "connecting", "open", "reconnecting" or "closed". */
  get state() { return this._state; }
  /** ICE servers the hub handed out in its welcome frame. */
  get iceServers() { return this._iceServers; }
  /** The verified identity the hub attached to this client, for hubs that set `auth`. */
  get identity() { return this._identity; }

  on(event, listener) {
    if (typeof listener !== 'function') throw new TypeError('listener must be a function');
    if (!this._listeners.has(event)) this._listeners.set(event, new Set());
    this._listeners.get(event).add(listener);
    return () => this.off(event, listener);
  }
  off(event, listener) {
    this._listeners.get(event)?.delete(listener);
    return this;
  }
  once(event, listener) {
    const stop = this.on(event, (...args) => { stop(); listener(...args); });
    return stop;
  }
  _emit(event, data) {
    for (const listener of [...(this._listeners.get(event) ?? [])]) {
      try { listener(data); } catch (error) { if (event !== 'error') this._emit('error', error); }
    }
  }

  /** Ids of the other peers currently in the room. */
  peers() { return [...this._peers]; }
  /** The verified identity of `peerId`, for hubs that set `auth`. */
  identityOf(peerId) { return this._identities.get(peerId) ?? null; }
  /** "direct" when frames to `peerId` use a data channel, "relay" when they use the hub, or null if not in the room. */
  transportOf(peerId) {
    if (!this._peers.has(peerId)) return null;
    return this.links.get(peerId)?.transport ?? 'relay';
  }

  /**
   * Connect to the hub and resolve once it has welcomed us. Retries with backoff and asks
   * `renew` for a fresh ticket when the first one is spent.
   */
  connect() {
    if (this._state === 'open') return Promise.resolve(this);
    if (this._establishing) return this._establishing;
    this._wantClosed = false;
    this._state = 'connecting';
    return this._begin(false);
  }

  /** Drop the current socket and connect again now with a fresh ticket. */
  reconnect() {
    if (this._wantClosed) this._wantClosed = false;
    const socket = this._socket;
    this._socket = null;
    if (socket) { try { socket.close(); } catch { /* already closed */ } }
    if (this._establishing) return this._establishing;
    this._state = 'reconnecting';
    return this._begin(true);
  }

  /** Open a direct link to `peerId`; see {@link connectPeer}. */
  connectPeer(peerId, options = {}) {
    return connectPeer(this, peerId, options);
  }

  /**
   * Send `payload` to the room, or to one peer with `{ to }`. Frames to one peer use the
   * data channel when it is open and the hub relay otherwise. Returns true if the frame was
   * sent or queued for the reconnect in progress, false if it was dropped.
   */
  send(payload, { to, channel = 'default', type = 'message' } = {}) {
    if (typeof channel !== 'string' || !channel || typeof type !== 'string' || !type) throw new TypeError('channel and type must be non-empty strings');
    if (type.startsWith('signal.') || type.startsWith('link.') || SERVER_TYPES.has(type)) throw new TypeError(`Frame type "${type}" is reserved`);
    if (to !== undefined && typeof to !== 'string') throw new TypeError('to must be a peer id');
    const frame = { v: ENVELOPE_VERSION, type, channel, ...(to === undefined ? {} : { to }), seq: this._seq++, payload: payload === undefined ? null : payload };
    const text = JSON.stringify(frame);
    if (encoder.encode(text).length > this._options.maxFrameBytes) throw new RangeError(`Frame exceeds ${this._options.maxFrameBytes} bytes`);
    if (this._state === 'open') {
      if (to !== undefined && this.links.get(to)?.send(frame)) return true;
      if (to === undefined && this._options.directBroadcast) return this._broadcast(frame, text);
      return this._relay(text);
    }
    if (this._state === 'closed' || this._state === 'idle' || this._wantClosed) throw new Error('Not connected');
    if (this._queue.length >= this._options.queueLimit) return false;
    this._queue.push(text);
    return true;
  }

  /** Close the connection and every direct link. No reconnect follows. */
  close() {
    this._wantClosed = true;
    this._wake?.();
    const socket = this._socket;
    this._socket = null;
    if (socket) { try { socket.close(1000); } catch { /* already closed */ } }
    for (const link of [...this.links.values()]) link.close();
    this._queue = [];
    this._peers.clear();
    if (this._state !== 'closed') {
      this._state = 'closed';
      this._emit('close', { reason: 'closed', reconnecting: false });
    }
  }

  // ---- connection ----

  _begin(reconnected) {
    const run = this._establish().then(() => {
      this._establishing = null;
      this._emit('open', { reconnected });
      return this;
    }, error => {
      this._establishing = null;
      if (this._state !== 'closed') {
        this._state = 'closed';
        this._emit('close', { reason: error.message, error, reconnecting: false });
      }
      throw error;
    });
    this._establishing = run;
    // A background reconnect has no caller to hear a failure; it is reported by `close`.
    if (reconnected) run.catch(() => {});
    return run;
  }

  async _establish() {
    for (let attempt = 0; ; attempt++) {
      if (this._wantClosed) throw new Error('Closed');
      try {
        const descriptor = await this._nextDescriptor();
        if (this._wantClosed) throw new Error('Closed');
        await this._attempt(descriptor);
        // close() landed while the attempt was finishing: the connection it made is not wanted.
        if (this._wantClosed) {
          const socket = this._socket;
          this._socket = null;
          if (socket) { try { socket.close(1000); } catch { /* already closed */ } }
          throw new Error('Closed');
        }
        return;
      } catch (error) {
        // Once close() has been called, a pending connect always ends with Closed, whatever the
        // attempt's own failure was and whichever of the two was seen first. No retry starts.
        if (this._wantClosed) throw error.message === 'Closed' ? error : new Error('Closed');
        if (error.fatal || attempt >= this._options.retries) throw error;
        const delayMs = Math.round(Math.min(this._options.retryMaxMs, this._options.retryBaseMs * 2 ** attempt) * (0.5 + Math.random() / 2));
        this._emit('reconnecting', { attempt: attempt + 1, delayMs, error });
        if (this._wantClosed) throw new Error('Closed');
        await new Promise(resolve => { this._wake = resolve; setTimeout(resolve, delayMs); });
        this._wake = null;
      }
    }
  }

  async _nextDescriptor() {
    if (this._pending) {
      const descriptor = this._pending;
      this._pending = null;
      return descriptor;
    }
    const { renew, credential } = this._options;
    if (!renew) throw fatal('The ticket was already used and no renew function was given to get a new one');
    const fresh = await renew({ credential: typeof credential === 'function' ? await credential() : credential, previous: this._descriptor });
    this._descriptor = checkDescriptor({ userId: this._descriptor.userId, ...fresh });
    return this._descriptor;
  }

  _attempt(descriptor) {
    const WebSocketImpl = this._options.WebSocket ?? globalThis.WebSocket;
    if (typeof WebSocketImpl !== 'function') return Promise.reject(fatal('No WebSocket implementation available'));
    return new Promise((resolve, reject) => {
      let socket;
      let sent = false;
      let welcomed = false;
      let timer;
      let done = false;
      const finish = error => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        // A ticket the hub never saw can be tried again.
        if (!sent) this._pending = descriptor;
        try { socket.close(); } catch { /* already closed */ }
        reject(error);
      };
      try { socket = new WebSocketImpl(descriptor.url); } catch (error) { reject(fatal(error.message)); return; }
      timer = setTimeout(() => finish(new Error('Timed out waiting for the hub')), this._options.connectTimeoutMs);
      socket.addEventListener('open', () => {
        sent = true;
        socket.send(JSON.stringify({ type: 'auth', ticket: descriptor.ticket }));
      });
      socket.addEventListener('message', event => {
        let frame;
        try { frame = JSON.parse(String(event.data)); } catch { return; }
        if (!frame || typeof frame !== 'object') return;
        if (!welcomed) {
          if (frame.type === 'error' && !('from' in frame) && typeof frame.code === 'string') { const error = new Error(`Hub error: ${frame.code}`); error.code = frame.code; this._emit('error', error); finish(error); return; }
          if (frame.type !== 'welcome' || 'from' in frame) return;
          welcomed = true;
          clearTimeout(timer);
          this._socket = socket;
          this._welcome(frame);
          resolve();
          return;
        }
        if (socket === this._socket) this._frame(frame);
      });
      // Some runtimes report a failed connection as `error` alone, with no `close` after it.
      const ended = () => {
        if (!welcomed) {
          finish(new Error(sent ? 'The hub refused the ticket or closed the connection' : 'Could not reach the hub'));
        } else if (socket === this._socket) {
          this._dropped();
        }
      };
      socket.addEventListener('error', ended);
      socket.addEventListener('close', ended);
    });
  }

  _welcome(frame) {
    this._iceServers = Array.isArray(frame.iceServers) ? frame.iceServers : [];
    this._identity = frame.identity ?? null;
    this._peers = new Set(Array.isArray(frame.peers) ? frame.peers.filter(peer => typeof peer === 'string' && peer !== this.id) : []);
    this._identities = new Map(Object.entries(frame.peerIdentities ?? {}));
    this._state = 'open';
    for (const [peer, link] of [...this.links]) if (!this._peers.has(peer)) link.close();
    const queued = this._queue;
    this._queue = [];
    for (const text of queued) this._relay(text);
    this._emit('welcome', { peers: this.peers(), iceServers: this._iceServers, identity: this._identity });
    for (const peer of this._peers) this._autoConnect(peer);
  }

  _dropped() {
    this._socket = null;
    if (this._wantClosed) return;
    if (!this._options.reconnect) {
      for (const link of [...this.links.values()]) link.close();
      this._state = 'closed';
      this._emit('close', { reason: 'The hub closed the connection', reconnecting: false });
      return;
    }
    this._state = 'reconnecting';
    this._emit('disconnected', { reconnecting: true });
    this._begin(true);
  }

  // A broadcast over the data channels that are open; the hub gets a targeted copy for each other
  // peer, so nobody hears it twice. With no open channel it is the ordinary relayed broadcast.
  _broadcast(frame, text) {
    const rest = [];
    for (const peer of this._peers) if (!this.links.get(peer)?.send(frame)) rest.push(peer);
    if (rest.length === this._peers.size) return this._relay(text);
    let sent = true;
    for (const peer of rest) sent = this._relay(JSON.stringify({ ...frame, to: peer })) && sent;
    return sent;
  }

  _relay(text) {
    try {
      this._socket.send(text);
      return true;
    } catch {
      return false;
    }
  }

  _frame(frame) {
    if ('from' in frame && SERVER_TYPES.has(frame.type)) return;
    switch (frame.type) {
      case 'error': { const error = new Error(`Hub error: ${frame.code}`); error.code = frame.code; this._emit('error', error); return; }
      case 'presence': return this._presence(frame);
      case 'welcome': return undefined;
      case 'signal.offer':
      case 'signal.answer':
      case 'signal.ice': return this._signalFrame(frame);
      default:
        if (typeof frame.from === 'string' && typeof frame.channel === 'string' && 'payload' in frame) this._deliver(frame, 'relay');
        return undefined;
    }
  }

  _presence(frame) {
    const peer = frame.peer;
    if (typeof peer !== 'string' || peer === this.id) return;
    if (frame.event === 'join') {
      this._peers.add(peer);
      if (frame.identity) this._identities.set(peer, frame.identity);
      this._emit('presence', { event: 'join', peer, identity: frame.identity ?? null });
      this._autoConnect(peer);
    } else if (frame.event === 'leave') {
      this._peers.delete(peer);
      this._identities.delete(peer);
      this.links.get(peer)?.close();
      this._emit('presence', { event: 'leave', peer });
    }
  }

  _directEnabled() {
    const { direct } = this._options;
    if (direct === false || typeof this.id !== 'string') return false;
    if (typeof (this._options.RTCPeerConnection ?? globalThis.RTCPeerConnection) !== 'function') return false;
    return direct === true || this._descriptor.link === 'peer-peer';
  }

  // The larger id offers; the smaller waits for the offer, so the pair normally negotiates once.
  _autoConnect(peer) {
    if (this._directEnabled() && this.id > peer && !this.links.has(peer)) this.connectPeer(peer);
  }

  _signalFrame(frame) {
    const peer = frame.from;
    if (typeof peer !== 'string' || !this._peers.has(peer)) return;
    let link = this.links.get(peer);
    if (!link && frame.type === 'signal.offer' && this._directEnabled()) link = this.connectPeer(peer);
    link?.handleSignal(frame.type, frame.payload);
  }

  _deliver(frame, via) {
    this._emit('message', { ...frame, via });
  }

  _signal(to, type, payload) {
    if (this._state !== 'open') return false;
    return this._relay(JSON.stringify({ v: ENVELOPE_VERSION, type, channel: SIGNAL_CHANNEL, to, seq: this._seq++, payload }));
  }

  _transport(peer, transport, reason) {
    this._emit('transport', { peer, transport, ...(reason ? { reason } : {}) });
  }
}

async function postJson(fetchImpl, url, body, what, extra = {}) {
  let response;
  try {
    response = await fetchImpl(url, { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json' }, body: JSON.stringify(body), ...extra });
  } catch (error) {
    throw new Error(`${what} failed: ${error?.message || 'network error'}`);
  }
  let data = null;
  try { data = await response.json(); } catch { /* an empty or non-JSON body */ }
  if (!response.ok) {
    const error = new Error(data?.error || data?.reason || `${what} failed (HTTP ${response.status})`);
    error.status = response.status;
    // A refused credential or a bad request will be refused again; only 408, 429 and 5xx are worth a retry.
    if (response.status >= 400 && response.status < 500 && response.status !== 408 && response.status !== 429) error.fatal = true;
    throw error;
  }
  return data;
}

/**
 * Pair through an application's own routes, connect, and resolve with the open client. The
 * application keeps the administrator token behind `pairUrl`; the browser only sees the
 * descriptor. `renewUrl` is posted the same `body` (plus `credential`, if one is set) whenever a
 * reconnect needs a fresh ticket. A 4xx answer, other than 408 or 429, stops the retries.
 *
 * Options: `pairUrl`, `renewUrl` (default: `pairUrl`), `body` (the request both routes get),
 * `credential`, `fetch`, and every `LiveClient` option.
 */
const memorySeats = new Map();
const failedSeatStores = new WeakSet();

export async function connectLive({ pairUrl, renewUrl = pairUrl, leaveUrl, heartbeatUrl, seatStore, body = {}, fetch: fetchImpl = globalThis.fetch, ...options } = {}) {
  if (typeof pairUrl !== 'string' || !pairUrl) throw new TypeError('pairUrl is required');
  if (typeof fetchImpl !== 'function') throw new Error('No fetch implementation available');
  const route = name => pairUrl.replace(/\/pair\/?(?=\?|#|$)/, `/${name}`);
  leaveUrl ??= route('leave');
  heartbeatUrl ??= route('heartbeat');
  const key = JSON.stringify([pairUrl, body.userId]);
  let store = seatStore;
  let storeIdentity = store;
  if (!store) {
    try {
      const storage = globalThis.sessionStorage;
      if (storage) {
        storeIdentity = storage;
        store = { get: key => storage.getItem(key), set: (key, value) => storage.setItem(key, value), delete: key => storage.removeItem(key) };
      }
    } catch { /* Storage may be disabled by the browser. */ }
  }
  let storageFailed = !store || failedSeatStores.has(storeIdentity);
  const failed = () => { storageFailed = true; if (storeIdentity) failedSeatStores.add(storeIdentity); };
  const getSeat = () => { try { return storageFailed ? memorySeats.get(key) : store.get(key); } catch { failed(); return memorySeats.get(key); } };
  const setSeat = token => { memorySeats.set(key, token); try { if (!storageFailed) store.set(key, token); } catch { failed(); } };
  const clearSeat = () => { memorySeats.delete(key); try { store?.delete(key); } catch { /* Already cleared in memory. */ } };
  const postSeat = async (url, data, what, extra) => {
    try { return await postJson(fetchImpl, url, data, what, extra); }
    catch (error) { if (error.fatal) clearSeat(); throw error; }
  };
  const credential = typeof options.credential === 'function' ? await options.credential() : options.credential;
  const savedSeat = getSeat();
  const descriptor = await postSeat(pairUrl, { ...body, ...(savedSeat ? { seatToken: savedSeat } : {}), ...(credential === undefined ? {} : { credential }) }, 'Pairing');
  const { seatToken, ...publicDescriptor } = descriptor;
  if (seatToken) setSeat(seatToken);
  const seatBody = { userId: body.userId, seatToken };
  const renew = options.renew
    ? async (...args) => { try { return await options.renew(...args); } catch (error) { if (error.fatal || (error.status >= 400 && error.status < 500 && error.status !== 408 && error.status !== 429)) clearSeat(); throw error; } }
    : (({ credential: next } = {}) => postSeat(renewUrl, { ...body, seatToken, ...(next === undefined ? {} : { credential: next }) }, 'Renewal'));
  const client = new LiveClient(publicDescriptor, { ...options, renew });
  client.heartbeat = () => postSeat(heartbeatUrl, seatBody, 'Heartbeat');
  client.leave = async () => { try { return await postSeat(leaveUrl, seatBody, 'Leave', { keepalive: true }); } finally { clearSeat(); } };
  try { await client.connect(); } catch (error) { client.close(); throw error; }
  return client;
}

/**
 * Watch the live hub list from a browser. `watchUrl` is the application's route that issues a
 * watch ticket (it keeps the administrator token); the page opens the registry socket itself
 * with the ticket. Resolves after the first snapshot with `{ hubs(), on(event, listener),
 * close(), closed }`. Events: `change` (the flat list, after every snapshot and diff) and
 * `close` (`{ reason }`). A gap in sequence numbers asks for a fresh snapshot by itself. The
 * list holds `{ id, mode, status, roomCount, memberCount, path }` and never an address,
 * credential or scope. Options: `body` (posted to `watchUrl`), `fetch`, `WebSocket`, `timeoutMs`.
 */
export async function watchHubs({ watchUrl, body = {}, fetch: fetchImpl = globalThis.fetch, WebSocket: WebSocketImpl = globalThis.WebSocket, timeoutMs = 10000 } = {}) {
  if (typeof watchUrl !== 'string' || !watchUrl) throw new TypeError('watchUrl is required');
  if (typeof fetchImpl !== 'function') throw new Error('No fetch implementation available');
  if (typeof WebSocketImpl !== 'function') throw new Error('No WebSocket implementation available');
  const issued = await postJson(fetchImpl, watchUrl, body, 'Watch');
  const nodes = new Map();
  const listeners = { change: new Set(), close: new Set() };
  let seq = -1;
  let closed = false;
  const watch = {
    hubs: () => [...nodes.values()],
    get closed() { return closed; },
    on(event, listener) { listeners[event]?.add(listener); return () => listeners[event]?.delete(listener); },
    close() { try { socket.close(); } catch { /* already closed */ } },
  };
  const emit = (event, data) => { for (const listener of [...listeners[event]]) { try { listener(data); } catch { /* a listener's failure is its own */ } } };
  const keyOf = path => path.join('/');
  const flatten = (list, parent) => {
    for (const { hubs: children, ...hub } of list || []) {
      const path = [...parent, hub.id];
      nodes.set(keyOf(path), { ...hub, path });
      if (children) flatten(children, path);
    }
  };
  const socket = new WebSocketImpl(issued.url);
  return new Promise((resolve, reject) => {
    let settled = false;
    const settle = error => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) { try { socket.close(); } catch { /* already closed */ } reject(error); } else resolve(watch);
    };
    const timer = setTimeout(() => settle(new Error('The hub list did not answer in time')), timeoutMs);
    socket.addEventListener('open', () => socket.send(JSON.stringify({ type: 'auth', ticket: issued.ticket, path: issued.path ?? [], depth: issued.depth ?? 1 })));
    socket.addEventListener('message', event => {
      let message;
      try { message = JSON.parse(String(event.data)); } catch { return; }
      if (message.type === 'snapshot') {
        nodes.clear();
        flatten(message.hubs, []);
        seq = message.seq;
      } else if (typeof message.seq === 'number') {
        if (message.seq !== seq + 1) { if (socket.readyState === 1) socket.send(JSON.stringify({ type: 'resnapshot' })); return; }
        seq = message.seq;
        const key = keyOf(message.path || []);
        if (message.type === 'removed') {
          for (const known of [...nodes.keys()]) if (known === key || known.startsWith(`${key}/`)) nodes.delete(known);
        } else if (message.hub) nodes.set(key, { ...message.hub, path: message.path });
      } else return;
      emit('change', watch.hubs());
      if (message.type === 'snapshot') settle();
    });
    socket.addEventListener('error', () => settle(new Error('The hub list connection failed')));
    socket.addEventListener('close', () => {
      closed = true;
      if (!settled) return settle(new Error('The hub closed the hub list (the ticket may have expired)'));
      emit('close', { reason: 'closed' });
    });
  });
}

export default LiveClient;
