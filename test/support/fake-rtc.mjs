// A stand-in for RTCPeerConnection. Plain Node has no WebRTC, so this models only what
// LiveClient depends on: the signalling state machine (offer, answer, implicit rollback of a
// pending local offer), negotiationneeded, trickled candidates, a negotiated data channel
// that opens once both sides have completed an exchange, and ICE failure. It does not model
// real network behaviour: browser coverage with real RTCPeerConnection is still to come.

const tick = () => new Promise(resolve => setTimeout(resolve, 0));

export class FakeNet {
  constructor() {
    this.pcs = new Map();
    this.nextId = 1;
    /** "connect" opens channels, "blackhole" never does, "fail" reports ICE failure. */
    this.mode = 'connect';
    this.rollbacks = 0;
    this.offers = 0;
  }
  /** A constructor to hand to LiveClient or connectPeer as `RTCPeerConnection`. */
  rtc() {
    const net = this;
    return class FakePeerConnection {
      constructor(config) {
        this.id = `pc${net.nextId++}`;
        net.pcs.set(this.id, this);
        this.config = config;
        this.signalingState = 'stable';
        this.iceConnectionState = 'new';
        this.localDescription = null;
        this.remoteDescription = null;
        this.channel = null;
        this.closed = false;
        this.restarts = 0;
        this.candidates = [];
        this._needed = false;
        this._done = false;
        this._ops = 0;
      }
      createDataChannel(label, options) {
        this.channel = new FakeChannel(this, label, options);
        this._need();
        return this.channel;
      }
      _need() {
        this._needed = true;
        // Like a browser, wait until queued session description operations have finished.
        const check = () => {
          if (this.closed || !this._needed) return;
          if (this._ops > 0) { setTimeout(check, 0); return; }
          if (this.signalingState === 'stable') this.onnegotiationneeded?.();
        };
        setTimeout(check, 0);
      }
      async setLocalDescription(description) {
        this._ops++;
        try { await this._setLocal(description); } finally { this._ops--; }
      }
      async _setLocal(description) {
        await tick();
        const type = description?.type ?? (this.signalingState === 'have-remote-offer' ? 'answer' : 'offer');
        if (type === 'offer') {
          if (this.signalingState !== 'stable' && this.signalingState !== 'have-local-offer') throw new Error('InvalidStateError: cannot offer');
          this.signalingState = 'have-local-offer';
          net.offers++;
        } else {
          if (this.signalingState !== 'have-remote-offer') throw new Error('InvalidStateError: cannot answer');
          this.signalingState = 'stable';
        }
        this.localDescription = { type, sdp: `fake-sdp ${this.id}` };
        setTimeout(() => {
          if (this.closed) return;
          this.onicecandidate?.({ candidate: { candidate: `candidate ${this.id}`, sdpMid: '0' } });
          this.onicecandidate?.({ candidate: null });
        }, 0);
        if (type === 'answer') this._complete();
      }
      async setRemoteDescription(description) {
        this._ops++;
        try { await this._setRemote(description); } finally { this._ops--; }
      }
      async _setRemote(description) {
        await tick();
        if (description.type === 'offer') {
          if (this.signalingState === 'have-local-offer') {
            // Implicit rollback, as in current browsers.
            this.localDescription = null;
            this.signalingState = 'stable';
            net.rollbacks++;
          }
          if (this.signalingState !== 'stable') throw new Error('InvalidStateError: cannot take an offer');
          this.signalingState = 'have-remote-offer';
        } else {
          if (this.signalingState !== 'have-local-offer') throw new Error('InvalidStateError: unexpected answer');
          this.signalingState = 'stable';
        }
        this.remoteDescription = description;
        if (description.type === 'answer') this._complete();
      }
      async addIceCandidate(candidate) {
        await tick();
        if (!this.remoteDescription) throw new Error('InvalidStateError: no remote description');
        this.candidates.push(candidate);
      }
      restartIce() {
        this.restarts++;
        this._need();
      }
      close() {
        if (this.closed) return;
        this.closed = true;
        this.signalingState = 'closed';
        this.channel?._close();
      }
      get partner() {
        const match = /fake-sdp (pc\d+)/.exec(this.remoteDescription?.sdp ?? '');
        return match ? net.pcs.get(match[1]) : null;
      }
      _complete() {
        this._needed = false;
        this._done = true;
        const other = this.partner;
        if (!other || !other._done || other.partner !== this) return;
        net._link(this, other);
      }
    };
  }
  _link(a, b) {
    for (const pc of [a, b]) pc._done = false;
    if (this.mode === 'blackhole') return;
    setTimeout(() => {
      for (const pc of [a, b]) {
        if (pc.closed) continue;
        if (this.mode === 'fail') {
          pc.iceConnectionState = 'failed';
          pc.oniceconnectionstatechange?.();
        } else {
          pc.iceConnectionState = 'connected';
          pc.oniceconnectionstatechange?.();
          pc.channel?._open();
        }
      }
    }, 0);
  }
  /** Report ICE failure on one connection. */
  failIce(pc) {
    pc.iceConnectionState = 'failed';
    pc.oniceconnectionstatechange?.();
  }
  /** Close every open channel, as a dropped path would. */
  dropChannels() {
    for (const pc of this.pcs.values()) pc.channel?._close();
  }
}

class FakeChannel {
  constructor(pc, label, options) {
    this.pc = pc;
    this.label = label;
    this.options = options;
    this.readyState = 'connecting';
    this.sent = [];
  }
  _open() {
    if (this.readyState !== 'connecting') return;
    this.readyState = 'open';
    this.onopen?.();
  }
  _close() {
    if (this.readyState === 'closed') return;
    this.readyState = 'closed';
    this.onclose?.();
    // The far end notices shortly after.
    const other = this.pc.partner?.channel;
    if (other) setTimeout(() => other._close(), 0);
  }
  close() { this._close(); }
  send(data) {
    if (this.readyState !== 'open') throw new Error('InvalidStateError: channel is not open');
    this.sent.push(data);
    const other = this.pc.partner?.channel;
    setTimeout(() => other?.onmessage?.({ data }), 0);
  }
}
