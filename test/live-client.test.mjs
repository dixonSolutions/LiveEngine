// LiveClient and the HubClient pairing methods.
//
// Everything that touches the hub runs against a real liveengine-hub process with real
// WebSockets. Plain Node has no WebRTC, so `connectPeer`'s negotiation and fallback logic is
// tested with an injected RTCPeerConnection stand-in (test/support/fake-rtc.mjs). That proves
// the logic and that the hub accepts and routes the signalling frames; it does not prove
// browser interoperability. Real-browser coverage is still to come.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { after, before, describe, test } from 'node:test';
import http from 'node:http';
import { LiveClient, connectLive, connectPeer } from '../lib/live-client.mjs';
import { FakeNet } from './support/fake-rtc.mjs';
import { ADMIN_TOKEN, startHub, startProxy } from './support/hub-process.mjs';

const require = createRequire(import.meta.url);
const { HubClient } = require('../lib/hubs.js');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, { ms = 8000, what = 'condition' } = {}) {
  const deadline = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(15);
  }
}
const quiet = (client, ms = 250) => sleep(ms).then(() => client);

let hub;
let admin;
let counter = 0;
const clients = [];
const track = client => { clients.push(client); return client; };

before(async () => {
  hub = await startHub();
  admin = new HubClient(hub.address, { token: ADMIN_TOKEN });
  await admin.register({ id: 'game', mode: 'internal', scopes: { tier: 'open' } });
  await admin.register({
    id: 'secure',
    mode: 'internal',
    scopes: { tier: 'secure' },
    auth: { kind: 'token', tokens: [
      { token: 'zed-credential-0123456789abcdef-aaaa', identity: 'zed', claims: { team: 'red' } },
      { token: 'zed-second-credential-0123456789abcdef', identity: 'zed', claims: { team: 'red' } },
    ] },
  });
});
after(async () => {
  for (const client of clients) client.close();
  await hub?.stop();
});

/** Two peers in one room, paired through the hub. Returns both descriptors. */
async function pairPeers(extra = {}) {
  const group = `g${++counter}`;
  const a = `${group}-a`;
  const b = `${group}-b`;
  await admin.call('game', 'upsertUser', [{ id: a, scopes: { group } }]);
  const second = await admin.pair({ subject: { id: b, scopes: {} }, target: { kind: 'peer', requirements: { group } } });
  const first = { ...second, userId: a, ...(await admin.ticket('game', { userId: a, serverId: second.room })) };
  return { a: { ...first, ...extra }, b: { ...second, ...extra }, room: second.room };
}

describe('HubClient pairing', () => {
  test('pair returns a descriptor LiveClient can use, with the subject id', async () => {
    const { b } = await pairPeers();
    assert.equal(b.link, 'peer-peer');
    assert.equal(b.url, hub.wsUrl);
    assert.deepEqual(b.transports, ['webrtc', 'websocket']);
    assert.ok(b.userId.endsWith('-b'));
    assert.ok(b.ticket && b.room.startsWith('pair:'));
  });

  test('discover finds hubs and peers and hides scopes and addresses', async () => {
    const group = `d${++counter}`;
    await admin.call('game', 'upsertUser', [{ id: `${group}-x`, scopes: { group } }]);
    const { candidates } = await admin.discover({ kind: 'peer', requirements: { group } });
    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].id, `${group}-x`);
    const hubs = await admin.discover({ kind: 'hub' });
    assert.ok(hubs.candidates.some(c => c.id === 'game'));
    assert.ok(!JSON.stringify(hubs).includes(ADMIN_TOKEN));
  });

  test('ticket is issued only for paired users and rejects bad roles', async () => {
    const { room, a } = await pairPeers();
    const issued = await admin.ticket('game', { userId: a.userId, serverId: room, role: 'service' });
    assert.ok(issued.ticket);
    await assert.rejects(admin.ticket('game', { userId: 'nobody', serverId: room }), error => error.status === 403);
    await assert.rejects(admin.ticket('game', { userId: a.userId, serverId: room, role: 'root' }), error => error.status >= 400);
    // The same ticket through a routed path.
    const routed = await admin.ticket('game', { userId: a.userId, serverId: room, path: [] });
    assert.ok(routed.ticket);
  });

  test('credentials reach hubs that set auth, from pair and from ticket', async () => {
    const request = { subject: { id: 'zed', scopes: {} }, target: { kind: 'hub', requirements: { tier: 'secure' } } };
    await assert.rejects(admin.pair(request), error => error.status === 403);
    await assert.rejects(admin.pair({ ...request, credential: 'wrong-credential-0123456789abcdef-zzzz' }), error => error.status === 403);
    const descriptor = await admin.pair({ ...request, credential: 'zed-credential-0123456789abcdef-aaaa' });
    assert.equal(descriptor.link, 'peer-hub');
    assert.equal(descriptor.room, 'pair:hub:secure');
    await assert.rejects(admin.ticket('secure', { userId: 'zed', serverId: descriptor.room }), error => error.status === 403);
    const again = await admin.ticket('secure', { userId: 'zed', serverId: descriptor.room, credential: 'zed-credential-0123456789abcdef-aaaa' });
    assert.ok(again.ticket);
    // credential given inside the subject works too.
    const nested = await admin.pair({ ...request, subject: { ...request.subject, credential: 'zed-second-credential-0123456789abcdef' } });
    assert.ok(nested.ticket);
  });

  test('watchTicket is scoped and watchRegistry streams a snapshot then ordered diffs', async () => {
    const { ticket } = await admin.watchTicket({ depth: 1 });
    assert.ok(ticket);
    await assert.rejects(admin.watchTicket({ path: ['missing'], depth: 1 }), error => error.status === 400);
    for (const options of [{ depth: 1 }, { depth: 1, ticket }]) {
      const watch = await admin.watchRegistry(options);
      assert.equal(watch.snapshot.type, 'snapshot');
      assert.equal(watch.seq, 0);
      const name = `watched-${++counter}`;
      const added = new Promise(resolve => watch.once('added', resolve));
      await admin.register({ id: name, mode: 'internal' });
      const message = await added;
      assert.deepEqual(message.path, [name]);
      assert.equal(message.seq, 1);
      const removed = new Promise(resolve => watch.once('removed', resolve));
      await admin.remove(name);
      assert.equal((await removed).seq, 2);
      watch.close();
      await until(() => watch.closed, { what: 'watch close' });
    }
  });

  test('a watch ticket cannot watch outside its scope and a bad token is refused', async () => {
    const { ticket } = await admin.watchTicket({ depth: 1 });
    await assert.rejects(admin.watchRegistry({ depth: 3, ticket: ticket.slice(0, -2) + 'xx', timeoutMs: 2000 }), /closed|failed|timed out/i);
    const stranger = new HubClient(hub.address, { token: 'not-the-administrator-token-0123456789' });
    await assert.rejects(stranger.watchRegistry({ depth: 1, timeoutMs: 2000 }), /closed|failed|timed out/i);
  });

  test('a sequence gap triggers a resnapshot and the stale diff is dropped', async () => {
    const sent = [];
    class StubSocket {
      readyState = 1;
      listeners = {};
      addEventListener(name, fn) { (this.listeners[name] ??= []).push(fn); }
      send(text) { sent.push(JSON.parse(text)); }
      close() { this.readyState = 3; for (const fn of this.listeners.close ?? []) fn({}); }
      emit(name, data) { for (const fn of this.listeners[name] ?? []) fn({ data: JSON.stringify(data) }); }
    }
    let stub;
    const client = new HubClient('https://hub.example', { token: 'secret-token', fetch: async () => Response.json({}) });
    const promise = client.watchRegistry({ depth: 1, WebSocket: class extends StubSocket { constructor() { super(); stub = this; } } });
    stub.emit('open', {});
    assert.equal(sent[0].type, 'auth');
    assert.equal(sent[0].token, 'secret-token');
    assert.deepEqual([sent[0].path, sent[0].depth], [[], 1]);
    stub.emit('message', { type: 'snapshot', seq: 0, path: [], hubs: [] });
    const watch = await promise;
    const seen = [];
    watch.on('event', message => seen.push([message.type, message.seq]));
    stub.emit('message', { type: 'added', seq: 1, path: ['a'] });
    stub.emit('message', { type: 'added', seq: 3, path: ['c'] });
    assert.deepEqual(sent.at(-1), { type: 'resnapshot' });
    stub.emit('message', { type: 'snapshot', seq: 3, path: [], hubs: [] });
    stub.emit('message', { type: 'removed', seq: 4, path: ['a'] });
    assert.deepEqual(seen, [['added', 1], ['snapshot', 3], ['removed', 4]]);
  });

  test('the new methods validate their input', async () => {
    assert.throws(() => admin.ticket('game', {}), /userId and serverId/);
    await assert.rejects(admin.pair({}), /subject.id/);
    assert.throws(() => admin.watchTicket({ path: 'x' }), /array/);
  });
});

describe('LiveClient over a real hub (relay)', () => {
  test('connects with the descriptor alone and exchanges frames', async () => {
    const { a, b } = await pairPeers();
    const alice = track(new LiveClient(a));
    const bob = track(new LiveClient(b));
    const heard = [];
    const presence = [];
    alice.on('message', frame => heard.push(frame));
    alice.on('presence', event => presence.push(event));
    await alice.connect();
    assert.equal(alice.state, 'open');
    assert.deepEqual(alice.peers(), []);
    await bob.connect();
    assert.deepEqual(bob.peers(), [a.userId]);
    await until(() => alice.peers().length === 1, { what: 'presence' });
    assert.deepEqual(presence, [{ event: 'join', peer: b.userId, identity: null }]);
    assert.deepEqual(alice.iceServers, [{ urls: 'stun:stun.example.test:3478' }]);
    assert.equal(alice.transportOf(b.userId), 'relay');

    bob.send({ n: 1 }, { channel: 'moves' });
    bob.send('only you', { to: a.userId, type: 'whisper' });
    await until(() => heard.length === 2, { what: 'frames' });
    assert.deepEqual(heard.map(f => [f.from, f.channel, f.type, f.payload, f.via]), [
      [b.userId, 'moves', 'message', { n: 1 }, 'relay'],
      [b.userId, 'default', 'whisper', 'only you', 'relay'],
    ]);
    assert.ok(heard[0].seq < heard[1].seq);

    const bobHeard = [];
    bob.on('message', frame => bobHeard.push(frame));
    alice.send(null);
    await until(() => bobHeard.length === 1, { what: 'null payload' });
    assert.equal(bobHeard[0].payload, null);
    // Not echoed to the sender.
    await quiet(alice);
    assert.equal(heard.length, 2);

    bob.close();
    await until(() => alice.peers().length === 0, { what: 'leave' });
    assert.equal(presence.at(-1).event, 'leave');
    assert.equal(bob.state, 'closed');
  });

  test('send validates and refuses reserved frame types', async () => {
    const { a } = await pairPeers();
    const client = track(new LiveClient(a, { maxFrameBytes: 200 }));
    assert.throws(() => client.send('x'), /Not connected/);
    await client.connect();
    for (const type of ['signal.offer', 'link.frame', 'welcome', 'presence', 'auth']) {
      assert.throws(() => client.send('x', { type }), /reserved/);
    }
    assert.throws(() => client.send('x'.repeat(300)), RangeError);
    assert.throws(() => client.send('x', { channel: '' }), TypeError);
    assert.equal(client.send('fine'), true);
  });

  test('a spent or bad ticket fails clearly without a renew function', async () => {
    const { a } = await pairPeers();
    const first = track(new LiveClient(a, { retries: 0 }));
    await first.connect();
    first.close();
    const reused = track(new LiveClient(a, { retries: 0 }));
    await assert.rejects(reused.connect(), /refused the ticket/);
    assert.equal(reused.state, 'closed');
    const forged = track(new LiveClient({ ...a, ticket: `${a.ticket}x` }, { retries: 0 }));
    await assert.rejects(forged.connect(), /refused the ticket/);
    assert.throws(() => new LiveClient({ url: a.url }), /ticket/);
    assert.throws(() => new LiveClient({ ticket: 't', url: 'ftp://x' }), /ws\(s\)/);
  });

  test('retries an unreachable hub and reuses the ticket the hub never saw', async () => {
    const { a } = await pairPeers();
    const proxy = await startProxy(hub.port);
    const url = `ws://127.0.0.1:${proxy.port}/v1/live`;
    await proxy.pause();
    const retries = [];
    const client = track(new LiveClient({ ...a, url }, { retryBaseMs: 40, retryMaxMs: 80, connectTimeoutMs: 1000 }));
    client.on('reconnecting', event => retries.push(event.attempt));
    const connecting = client.connect();
    await until(() => retries.length >= 2, { what: 'two failed attempts' });
    await proxy.resume();
    await connecting;
    assert.equal(client.state, 'open');
    assert.ok(retries.length >= 2);
    await proxy.stop();
  });

  test('reconnects after the connection is cut, with a fresh ticket, and queues sends meanwhile', async () => {
    const { a, b, room } = await pairPeers();
    const proxy = await startProxy(hub.port);
    const through = descriptor => ({ ...descriptor, url: `ws://127.0.0.1:${proxy.port}/v1/live` });
    let renewals = 0;
    const alice = track(new LiveClient(through(a), {
      retryBaseMs: 40,
      renew: async () => { renewals++; return through({ ...a, ...(await admin.ticket('game', { userId: a.userId, serverId: room })) }); },
    }));
    const bob = track(new LiveClient(b));
    const events = [];
    for (const name of ['disconnected', 'reconnecting', 'open']) alice.on(name, event => events.push([name, event.reconnected]));
    const heard = [];
    bob.on('message', frame => heard.push(frame.payload));
    await alice.connect();
    await bob.connect();
    await until(() => alice.peers().length === 1);
    alice.send('before');
    await until(() => heard.length === 1);

    await proxy.pause();
    await until(() => alice.state === 'reconnecting', { what: 'reconnecting state' });
    assert.equal(alice.send('while down'), true);
    await proxy.resume();
    await until(() => alice.state === 'open', { what: 'reopen' });
    await until(() => heard.length === 2, { what: 'queued frame' });
    assert.deepEqual(heard, ['before', 'while down']);
    assert.ok(renewals >= 1);
    assert.ok(events.some(([name]) => name === 'disconnected'));
    assert.ok(events.some(([name, reconnected]) => name === 'open' && reconnected === true));
    alice.send('after');
    await until(() => heard.length === 3);
    assert.equal(heard[2], 'after');
    await proxy.stop();
  });

  test('without renew a dropped connection closes with a plain reason; reconnect:false closes at once', async () => {
    for (const options of [{}, { reconnect: false }]) {
      const { a } = await pairPeers();
      const proxy = await startProxy(hub.port);
      const client = track(new LiveClient({ ...a, url: `ws://127.0.0.1:${proxy.port}/v1/live` }, { retries: 0, ...options }));
      const closed = new Promise(resolve => client.on('close', resolve));
      await client.connect();
      proxy.sever();
      const event = await closed;
      assert.equal(event.reconnecting, false);
      assert.equal(client.state, 'closed');
      if (options.reconnect === undefined) assert.match(event.reason, /renew/);
      assert.throws(() => client.send('x'), /Not connected/);
      await proxy.stop();
    }
  });

  test('reconnect() replaces the connection on demand', async () => {
    const { a, room } = await pairPeers();
    const client = track(new LiveClient(a, { renew: async () => ({ ...a, ...(await admin.ticket('game', { userId: a.userId, serverId: room })) }) }));
    await client.connect();
    await client.reconnect();
    assert.equal(client.state, 'open');
    assert.equal(client.send('still works'), true);
  });

  test('a credential reaches renew, and the verified identity arrives in the welcome', async () => {
    const request = { subject: { id: 'zed', scopes: {} }, target: { kind: 'hub', requirements: { tier: 'secure' } } };
    const good = 'zed-credential-0123456789abcdef-aaaa';
    const descriptor = await admin.pair({ ...request, credential: good });
    const seen = [];
    const renew = admin.renewal(request);
    const client = track(new LiveClient(descriptor, {
      credential: () => good,
      renew: async context => { seen.push(context.credential); return renew(context); },
    }));
    await client.connect();
    assert.deepEqual(client.identity, { identity: 'zed', claims: { team: 'red' } });
    assert.equal(client.id, 'zed');
    await client.reconnect();
    assert.deepEqual(seen, [good]);
    assert.equal(client.state, 'open');
    // A refused credential surfaces as an error from connect, not a hang.
    const refusing = track(new LiveClient({ ...descriptor, ticket: 'x' }, {
      retries: 0,
      credential: 'wrong-credential-0123456789abcdef-zzzz',
      renew: admin.renewal(request),
    }));
    await assert.rejects(refusing.connect());
  });

  test('close() ends retrying and is final', async () => {
    const { a } = await pairPeers();
    const proxy = await startProxy(hub.port);
    await proxy.pause();
    const client = track(new LiveClient({ ...a, url: `ws://127.0.0.1:${proxy.port}/v1/live` }, { retryBaseMs: 50, connectTimeoutMs: 500 }));
    const connecting = client.connect();
    connecting.catch(() => {});
    await sleep(120);
    client.close();
    await assert.rejects(connecting, /Closed/);
    assert.equal(client.state, 'closed');
    await proxy.stop();
  });
});

describe('connectPeer negotiation and fallback (injected RTCPeerConnection)', () => {
  function stubHost(id, router, rtc, extra = {}) {
    const host = {
      id,
      iceServers: [],
      links: new Map(),
      _options: { RTCPeerConnection: rtc, maxFrameBytes: 65536, ...extra },
      delivered: [],
      events: [],
      signals: [],
      _signal(to, type, payload) {
        host.signals.push({ to, type });
        setTimeout(() => router.get(to)?._incoming(id, type, payload), 0);
        return true;
      },
      _incoming(from, type, payload) {
        let link = host.links.get(from);
        if (!link && type === 'signal.offer') link = connectPeer(host, from, { directTimeoutMs: host.timeoutMs });
        link?.handleSignal(type, payload);
      },
      _deliver(frame, via) { host.delivered.push({ frame, via }); },
      _emit(name, data) { host.events.push([name, data]); },
      _transport(peer, transport, reason) { host.events.push(['transport', { peer, transport, reason }]); },
      timeoutMs: 600,
    };
    router.set(id, host);
    return host;
  }
  const setup = (netMode = 'connect') => {
    const net = new FakeNet();
    net.mode = netMode;
    const router = new Map();
    return { net, router, rtc: net.rtc(), a: null, b: null };
  };
  const frame = (payload, extra = {}) => ({ v: 1, type: 'message', channel: 'c', seq: 0, payload, ...extra });

  test('one side offers: both settle on a direct channel and frames bypass the relay', async () => {
    const s = setup();
    const a = stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 600 });
    assert.equal(link.polite, false);
    assert.equal(await link.ready, 'direct');
    await until(() => a.links.get('b')?.transport === 'direct');
    assert.equal(a.links.get('b').polite, true);
    assert.equal(s.net.offers, 1);
    assert.equal(s.net.rollbacks, 0);
    assert.ok(link.send(frame('hi', { to: 'a' })));
    await until(() => a.delivered.length === 1);
    assert.deepEqual(a.delivered[0].via, 'direct');
    assert.equal(a.delivered[0].frame.from, 'b');
    assert.equal(a.delivered[0].frame.payload, 'hi');
    // Candidates were trickled and accepted on both sides.
    assert.ok([...s.net.pcs.values()].every(pc => pc.candidates.length >= 1));
  });

  test('simultaneous offers collide, the polite side rolls back, and one channel results', async () => {
    const s = setup();
    const a = stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const la = connectPeer(a, 'b', { directTimeoutMs: 600 });
    const lb = connectPeer(b, 'a', { directTimeoutMs: 600 });
    assert.deepEqual([la.polite, lb.polite], [true, false]);
    assert.deepEqual(await Promise.all([la.ready, lb.ready]), ['direct', 'direct']);
    assert.equal(s.net.rollbacks, 1);
    assert.equal(la.transport, 'direct');
    assert.equal(lb.transport, 'direct');
    la.send(frame('from a', { to: 'b' }));
    lb.send(frame('from b', { to: 'a' }));
    await until(() => a.delivered.length === 1 && b.delivered.length === 1);
    assert.equal(a.delivered[0].frame.payload, 'from b');
    assert.equal(b.delivered[0].frame.payload, 'from a');
  });

  test('the impolite side ignores a colliding offer and its candidates without failing', async () => {
    const s = setup('blackhole');
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 300 });
    await until(() => link.pc.signalingState === 'have-local-offer');
    await link.handleSignal('signal.offer', { description: { type: 'offer', sdp: 'fake-sdp pc999' } });
    await link.handleSignal('signal.ice', { candidate: { candidate: 'late' } });
    assert.equal(link.pc.signalingState, 'have-local-offer');
    assert.equal(link.closed, false);
    assert.equal(link.failure, null);
    link.close();
  });

  test('no answer: the link times out, settles on relay, and frames report relay', async () => {
    const s = setup('blackhole');
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'ghost', { directTimeoutMs: 120 });
    assert.equal(link.transport, 'relay');
    assert.equal(await link.ready, 'relay');
    assert.equal(link.failure, 'timed out');
    assert.equal(link.send(frame('x')), false);
    assert.equal(b.links.has('ghost'), false);
    assert.ok(b.events.some(([name, data]) => name === 'direct-failed' && data.peer === 'ghost'));
  });

  test('a path that never connects falls back after the timeout even with signalling working', async () => {
    const s = setup('blackhole');
    const a = stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 150 });
    assert.equal(await link.ready, 'relay');
    assert.equal(s.net.offers, 1);
    assert.equal(link.send(frame('x', { to: 'a' })), false);
    await until(() => !a.links.has('b'), { what: 'far side to give up too' });
  });

  test('ICE failure before the channel opens falls back immediately', async () => {
    const s = setup('fail');
    stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 5000 });
    assert.equal(await link.ready, 'relay');
    assert.equal(link.failure, 'connection failed');
  });

  test('a channel that opens and then closes returns the peer to the relay and says so', async () => {
    const s = setup();
    stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 600 });
    assert.equal(await link.ready, 'direct');
    s.net.dropChannels();
    await until(() => link.closed, { what: 'link to close' });
    assert.equal(link.transport, 'relay');
    assert.equal(link.send(frame('x', { to: 'a' })), false);
    const moves = b.events.filter(([name]) => name === 'transport').map(([, data]) => data.transport);
    assert.deepEqual(moves, ['direct', 'relay']);
  });

  test('ICE failure on an open channel restarts ICE once, then falls back', async () => {
    const s = setup();
    stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 600 });
    await link.ready;
    s.net.failIce(link.pc);
    assert.equal(link.pc.restarts, 1);
    assert.equal(link.closed, false);
    await until(() => s.net.offers >= 2, { what: 'the restart offer' });
    s.net.failIce(link.pc);
    assert.equal(link.closed, true);
    assert.equal(link.failure, 'connection failed');
    assert.equal(link.transport, 'relay');
  });

  test('without WebRTC the link settles on relay at once', async () => {
    const s = setup();
    const b = stubHost('b', s.router, undefined);
    const link = connectPeer(b, 'a');
    assert.equal(await link.ready, 'relay');
    assert.equal(link.failure, 'WebRTC is not available');
    assert.equal(link.send(frame('x')), false);
  });

  test('frames on a data channel cannot spoof the sender or the hub, and are bounded', async () => {
    const s = setup();
    const a = stubHost('a', s.router, s.rtc);
    const b = stubHost('b', s.router, s.rtc);
    const link = connectPeer(b, 'a', { directTimeoutMs: 600 });
    await link.ready;
    const channel = link.channel;
    const inject = value => a.links.get('b').channel.onmessage({ data: typeof value === 'string' ? value : JSON.stringify(value) });
    inject({ ...frame('spoof'), from: 'someone-else' });
    inject({ ...frame('welcome'), type: 'welcome' });
    inject({ ...frame('presence'), type: 'presence' });
    inject({ ...frame('sig'), type: 'signal.offer' });
    inject({ ...frame('link'), type: 'link.frame' });
    inject({ ...frame('wrong target'), to: 'c' });
    inject({ v: 1, type: 'message', channel: 'c', seq: 0 });
    inject({ type: 'message', channel: 'c', payload: 1 });
    inject('not json');
    inject('x'.repeat(70000));
    inject({ ...frame('ok'), to: 'a' });
    assert.equal(a.delivered.length, 2);
    assert.equal(a.delivered[0].frame.from, 'b');
    assert.equal(a.delivered[0].frame.payload, 'spoof');
    assert.equal(a.delivered[1].frame.payload, 'ok');
    assert.ok(channel.sent.length === 0);
  });

  test('connectPeer rejects bad arguments and reuses a live link', async () => {
    const s = setup('blackhole');
    const b = stubHost('b', s.router, s.rtc);
    assert.throws(() => connectPeer(b, ''), TypeError);
    assert.throws(() => connectPeer(b, 'b'), /yourself/);
    const link = connectPeer(b, 'a', { directTimeoutMs: 1000 });
    assert.equal(connectPeer(b, 'a'), link);
    link.close();
    assert.equal(b.links.size, 0);
  });
});

describe('LiveClient direct path over a real hub (injected RTCPeerConnection)', () => {
  test('peers negotiate through the hub, switch to direct, and fall back to the relay when it drops', async () => {
    const net = new FakeNet();
    const RTCPeerConnection = net.rtc();
    const { a, b } = await pairPeers();
    const options = { RTCPeerConnection, directTimeoutMs: 1500 };
    const alice = track(new LiveClient(a, options));
    const bob = track(new LiveClient(b, options));
    const aliceHeard = [];
    const bobMoves = [];
    alice.on('message', frame => aliceHeard.push(frame));
    bob.on('transport', event => bobMoves.push(event.transport));
    await alice.connect();
    await bob.connect();
    await until(() => alice.transportOf(b.userId) === 'direct' && bob.transportOf(a.userId) === 'direct', { what: 'direct transport' });
    assert.equal(net.rollbacks, 0);
    // The larger id offered, the smaller waited.
    const offerer = a.userId > b.userId ? alice : bob;
    assert.equal(offerer.links.get(offerer === alice ? b.userId : a.userId).polite, false);

    bob.send('direct hello', { to: a.userId });
    await until(() => aliceHeard.length === 1);
    assert.equal(aliceHeard[0].via, 'direct');
    assert.equal(aliceHeard[0].from, b.userId);
    // Broadcast always goes through the hub, which fans out.
    bob.send('to everyone');
    await until(() => aliceHeard.length === 2);
    assert.equal(aliceHeard[1].via, 'relay');

    net.dropChannels();
    await until(() => bob.transportOf(a.userId) === 'relay', { what: 'fallback' });
    assert.deepEqual(bobMoves, ['direct', 'relay']);
    bob.send('relay again', { to: a.userId });
    await until(() => aliceHeard.length === 3);
    assert.equal(aliceHeard[2].via, 'relay');
    assert.equal(aliceHeard[2].payload, 'relay again');
  });

  test('a peer without WebRTC stays on the relay and nothing breaks', async () => {
    const net = new FakeNet();
    const { a, b } = await pairPeers();
    const withRtc = track(new LiveClient(a.userId > b.userId ? a : b, { RTCPeerConnection: net.rtc(), directTimeoutMs: 300 }));
    const without = track(new LiveClient(a.userId > b.userId ? b : a));
    const heard = [];
    const failures = [];
    without.on('message', frame => heard.push(frame));
    withRtc.on('direct-failed', event => failures.push(event));
    await withRtc.connect();
    await without.connect();
    await until(() => failures.length === 1, { what: 'direct to give up' });
    assert.equal(withRtc.transportOf(without.id), 'relay');
    withRtc.send('still delivered', { to: without.id });
    await until(() => heard.length === 1);
    assert.equal(heard[0].via, 'relay');
  });

  test('peer-hub pairings never open direct links, even with WebRTC available', async () => {
    const net = new FakeNet();
    const descriptor = await admin.pair({ subject: { id: `hubber-${++counter}`, scopes: {} }, target: { kind: 'hub', requirements: { tier: 'open' } } });
    assert.equal(descriptor.link, 'peer-hub');
    const client = track(new LiveClient(descriptor, { RTCPeerConnection: net.rtc() }));
    await client.connect();
    await quiet(client);
    assert.equal(net.pcs.size, 0);
  });

  test('direct:false disables WebRTC for a peer-peer pairing', async () => {
    const net = new FakeNet();
    const { a, b } = await pairPeers();
    const options = { RTCPeerConnection: net.rtc(), direct: false };
    const alice = track(new LiveClient(a, options));
    const bob = track(new LiveClient(b, options));
    await alice.connect();
    await bob.connect();
    await quiet(bob);
    assert.equal(net.pcs.size, 0);
  });
});

describe('connectLive', () => {
  let app;
  let base;
  const seen = [];
  before(async () => {
    await admin.register({
      id: 'secure-bridge', mode: 'internal', scopes: { tier: 'secure-bridge' },
      auth: { kind: 'token', tokens: [{ token: 'yan-credential-0123456789abcdef-aaaa', identity: 'yan' }] },
    });
    // A stand-in application bridge: it holds the administrator token, the caller never does.
    app = http.createServer(async (req, res) => {
      let raw = '';
      for await (const chunk of req) raw += chunk;
      const request = JSON.parse(raw || '{}');
      seen.push({ url: req.url, request });
      const reply = (status, value) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(value)); };
      try {
        const target = { kind: 'hub', requirements: { tier: request.tier } };
        const pairing = { subject: { id: request.userId, scopes: {} }, target, credential: request.credential };
        if (req.url === '/pair') return reply(200, await admin.pair(pairing));
        if (req.url === '/renew') return reply(200, await admin.renewal(pairing)({ credential: request.credential }));
        if (req.url === '/busy') return reply(503, { error: 'try later' });
        return reply(404, { error: 'unknown route' });
      } catch (error) {
        return reply(error.status || 502, { error: error.message });
      }
    });
    await new Promise(resolve => app.listen(0, '127.0.0.1', resolve));
    base = `http://127.0.0.1:${app.address().port}`;
  });
  after(async () => { await new Promise(resolve => app.close(resolve)); });

  test('pairs through the application routes and exchanges frames without an administrator token', async () => {
    const a = track(await connectLive({ pairUrl: `${base}/pair`, body: { userId: `cl-a-${++counter}`, tier: 'open' } }));
    const b = track(await connectLive({ pairUrl: `${base}/pair`, body: { userId: `cl-b-${counter}`, tier: 'open' } }));
    assert.equal(a.state, 'open');
    const heard = [];
    a.on('message', frame => heard.push(frame));
    await until(() => b.peers().includes(a.id));
    b.send({ hello: 'a' });
    await until(() => heard.length === 1);
    assert.deepEqual(heard[0].payload, { hello: 'a' });
    assert.ok(!JSON.stringify(seen).includes(ADMIN_TOKEN));
  });

  test('a reconnect asks renewUrl for a fresh ticket', async () => {
    const before = seen.filter(entry => entry.url === '/renew').length;
    const client = track(await connectLive({ pairUrl: `${base}/pair`, renewUrl: `${base}/renew`, body: { userId: `cl-r-${++counter}`, tier: 'open' } }));
    const reopened = new Promise(resolve => client.once('open', resolve));
    client.reconnect();
    assert.equal((await reopened).reconnected, true);
    assert.equal(seen.filter(entry => entry.url === '/renew').length, before + 1);
  });

  test('a refused credential rejects with the status and is not retried', async () => {
    const before = seen.length;
    await assert.rejects(
      connectLive({ pairUrl: `${base}/pair`, body: { userId: 'yan', tier: 'secure-bridge' }, credential: 'wrong-credential-0123456789abcdef-zzzz' }),
      error => error.status === 403 && error.fatal === true,
    );
    assert.equal(seen.length, before + 1);
  });

  test('a credential refused on renewal ends the reconnect without a retry loop', async () => {
    const credentials = ['yan-credential-0123456789abcdef-aaaa', 'wrong-credential-0123456789abcdef-zzzz'];
    const client = track(await connectLive({
      pairUrl: `${base}/pair`, renewUrl: `${base}/renew`, body: { userId: 'yan', tier: 'secure-bridge' },
      credential: () => credentials.shift(), retryBaseMs: 10,
    }));
    const before = seen.filter(entry => entry.url === '/renew').length;
    const closed = new Promise(resolve => client.once('close', resolve));
    await assert.rejects(client.reconnect(), error => error.status === 403);
    const event = await closed;
    assert.equal(event.reconnecting, false);
    assert.equal(seen.filter(entry => entry.url === '/renew').length, before + 1);
    assert.equal(client.state, 'closed');
  });

  test('a server error is not fatal, so the caller may retry', async () => {
    await assert.rejects(connectLive({ pairUrl: `${base}/busy` }), error => error.status === 503 && !error.fatal);
    await assert.rejects(connectLive({}), TypeError);
  });
});
