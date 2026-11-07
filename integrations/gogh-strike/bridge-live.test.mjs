// The bridge against a real liveengine-hub process and real WebSockets: schemas and channels
// at start, discover, pair and renew routes, LiveClient served to the browser, hub auth.
// The game is a fixture that supplies only the DTO module the bridge imports.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdir, mkdtemp, rm, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {after, before, describe, test} from 'node:test';
import {LiveClient, connectLive} from '../../lib/live-client.mjs';
import {ADMIN_TOKEN, freePort, startHub} from '../../test/support/hub-process.mjs';

const require = createRequire(import.meta.url);
const {HubClient} = require('../../lib/hubs.js');
const bridgeScript = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'bridge.mjs');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, what) {
  for (let i = 0; i < 400; i++) { const value = await check(); if (value) return value; await sleep(15); }
  throw new Error(`timed out waiting for ${what}`);
}

// Same shape as the game's liveengine-net.js exports.
const FIXTURE = `
export const LIVE_SCHEMAS = [
  {id: 'fixture.state', version: '1', schema: {type: 'object', additionalProperties: false, required: ['n', 'x'], properties: {n: {type: 'integer'}, x: {type: 'number'}}}},
  {id: 'fixture.event', version: '1', schema: {type: 'object', additionalProperties: false, required: ['kind'], properties: {kind: {enum: ['shot', 'down']}}}},
];
export const LIVE_CHANNELS = {
  state: {schemaRef: 'fixture.state@1', delivery: 'lossy', maxRate: 60},
  event: {schemaRef: 'fixture.event@1', delivery: 'reliable', maxRate: 60},
};
`;
const USER_TOKEN = 'gogh-auth-player-token-0123456789abcdef';

let hub, admin, root;
const children = [];
const clients = [];
const bodies = [];

async function startBridge(env = {}) {
  const port = await freePort();
  const child = spawn(process.execPath, [bridgeScript], {
    cwd: root, stdio: 'ignore',
    env: {...process.env, PORT: String(port), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: root,
      LIVEENGINE_URL: hub.address, LIVEENGINE_TOKEN: ADMIN_TOKEN, GOGH_REQUEST_LIMIT: '1000', ...env},
  });
  children.push(child);
  const base = `http://127.0.0.1:${port}`;
  await until(async () => { try { return (await fetch(`${base}/health`)).ok; } catch { return false; } }, 'the bridge to start');
  const post = async (route, body, origin = base) => {
    const response = await fetch(`${base}/api/liveengine/${route}`, {
      method: 'POST', headers: {'Content-Type': 'application/json', Origin: origin}, body: JSON.stringify(body),
    });
    const text = await response.text();
    bodies.push(text);
    return {status: response.status, body: text ? JSON.parse(text) : null};
  };
  // What the browser does: same-origin requests, which carry the page's Origin.
  const browserFetch = (url, init) => fetch(url, {...init, headers: {...init.headers, Origin: base}});
  const join = async (userId, extra = {}) => {
    const client = await connectLive({
      pairUrl: `${base}/api/liveengine/pair`, renewUrl: `${base}/api/liveengine/renew`,
      body: {userId, team: 0, role: 'vanguard'}, fetch: browserFetch, retryBaseMs: 10, ...extra,
    });
    clients.push(client);
    return client;
  };
  return {base, post, join, child};
}

before(async () => {
  hub = await startHub();
  admin = new HubClient(hub.address, {token: ADMIN_TOKEN});
  root = await mkdtemp(path.join(tmpdir(), 'liveengine-gogh-live-'));
  await mkdir(path.join(root, 'src'));
  await writeFile(path.join(root, 'index.html'), '<!doctype html><title>Gogh Strike fixture</title>');
  await writeFile(path.join(root, 'src/liveengine-net.js'), FIXTURE);
});

after(async () => {
  for (const client of clients) client.close();
  for (const child of children) { child.kill('SIGTERM'); await new Promise(resolve => child.once('exit', resolve)); }
  await hub?.stop();
  if (root) await rm(root, {recursive: true, force: true});
});

describe('bridge with a real hub', () => {
  let bridge;
  before(async () => { bridge = await startBridge({LIVEENGINE_HUB_ID: 'gogh-live'}); });

  test('serves LiveClient to the browser and the game beside it', async () => {
    const script = await fetch(`${bridge.base}/liveengine/live-client.mjs`);
    assert.equal(script.status, 200);
    assert.match(script.headers.get('content-type'), /javascript/);
    const text = await script.text();
    assert.match(text, /export class LiveClient/);
    assert.match(text, /export async function connectLive/);
    assert.match((await (await fetch(`${bridge.base}/`)).text()), /Gogh Strike/);
  });

  test('registers the game schemas and channels on first use, once', async () => {
    const first = await bridge.join('gogh-reg-1');
    assert.equal(first.state, 'open');
    const refs = (await admin.listSchemas()).schemas.map(entry => entry.ref);
    assert.ok(refs.includes('fixture.state@1') && refs.includes('fixture.event@1'));
    const registered = await admin.get('gogh-live');
    assert.deepEqual(registered.channels, {
      state: {schemaRef: 'fixture.state@1', delivery: 'lossy', maxRate: 60},
      event: {schemaRef: 'fixture.event@1', delivery: 'reliable', maxRate: 60},
    });
    assert.deepEqual(registered.scopes, {game: 'gogh-strike'});
    // A second bridge on the same hub finds the schemas and the hub already there.
    const other = await startBridge({LIVEENGINE_HUB_ID: 'gogh-live'});
    assert.equal((await other.join('gogh-reg-2')).state, 'open');
  });

  test('two browsers pair into one room and exchange typed frames on both channels', async () => {
    const a = await bridge.join('gogh-pair-a');
    const b = await bridge.join('gogh-pair-b');
    assert.equal(a.room, 'pair:hub:gogh-live');
    assert.equal(b.room, a.room);
    await until(() => a.peers().includes('gogh-pair-b') && b.peers().includes('gogh-pair-a'), 'both peers to see each other');
    const heard = [];
    b.on('message', frame => heard.push(frame));
    a.send({n: 1, x: 2.5}, {channel: 'state'});
    a.send({kind: 'shot'}, {channel: 'event'});
    await until(() => heard.length === 2, 'both frames');
    assert.deepEqual(heard.map(frame => [frame.channel, frame.from, frame.via]), [['state', 'gogh-pair-a', 'relay'], ['event', 'gogh-pair-a', 'relay']]);
    assert.deepEqual(heard[0].payload, {n: 1, x: 2.5});
  });

  test('the hub enforces the game schemas: a malformed state frame is never delivered', async () => {
    const a = await bridge.join('gogh-schema-a');
    const b = await bridge.join('gogh-schema-b');
    await until(() => a.peers().includes('gogh-schema-b'), 'peers');
    const heard = [];
    b.on('message', frame => heard.push(frame));
    a.send({n: 'one', x: 'far'}, {channel: 'state'});
    a.send({n: 2, x: 1, health: 999}, {channel: 'state'});
    await sleep(400);
    assert.deepEqual(heard, []);
  });

  test('renew gives a known player a fresh ticket and refuses a stranger', async () => {
    const client = await bridge.join('gogh-renew');
    assert.equal((await bridge.post('renew', {userId: 'gogh-renew'})).status, 403);
    assert.equal((await bridge.post('renew', {userId: 'gogh-nobody'})).status, 403);
    assert.equal((await client.heartbeat()).ok, true);
    assert.equal((await client.leave()).ok, true);
    assert.equal((await bridge.post('renew', {userId: 'gogh-renew'})).status, 403);
  });

  test('a dropped connection reconnects through the renew route with a new ticket', async () => {
    const client = await bridge.join('gogh-drop');
    const first = client._descriptor.ticket;
    const reopened = new Promise(resolve => client.once('open', resolve));
    client.reconnect();
    assert.equal((await reopened).reconnected, true);
    assert.notEqual(client._descriptor.ticket, first);
  });

  test('discover lists the hub and exposes no scopes beyond the public fields or secrets', async () => {
    const found = await bridge.post('discover', {kind: 'hub'});
    assert.equal(found.status, 200);
    assert.ok(found.body.candidates.some(candidate => candidate.id === 'gogh-live' && candidate.kind === 'hub'));
    assert.equal((await bridge.post('discover', {kind: 'hub', requirements: {game: 'chess'}})).body.candidates.length, 0);
    assert.equal((await bridge.post('discover', {kind: 'nonsense'})).status, 200);
  });

  test('new routes need an allowed Origin, a valid user id and never leak the administrator token', async () => {
    for (const route of ['pair', 'renew', 'discover']) {
      assert.equal((await bridge.post(route, {userId: 'x'}, 'https://attacker.example')).status, 403);
    }
    assert.equal((await bridge.post('pair', {userId: 'has space'})).status, 400);
    assert.equal((await bridge.post('pair', {})).status, 400);
    const unknown = await fetch(`${bridge.base}/api/liveengine/pair`, {method: 'POST', headers: {Host: 'attacker.example', Origin: bridge.base}, body: '{}'});
    assert.ok([403, 400].includes(unknown.status));
    assert.ok(bodies.length > 5);
    assert.ok(!bodies.join('').includes(ADMIN_TOKEN));
  });
});

describe('bridge with hub auth', () => {
  let bridge;
  before(async () => {
    bridge = await startBridge({
      LIVEENGINE_HUB_ID: 'gogh-secure',
      GOGH_HUB_AUTH: JSON.stringify({kind: 'token', tokens: [{token: USER_TOKEN, identity: 'gogh-auth-player', claims: {tier: 'gold'}}]}),
    });
  });

  test('bad and missing credentials are refused with the hub status; the good one connects', async () => {
    const refused = await bridge.post('pair', {userId: 'gogh-auth-player'});
    assert.equal(refused.status, 403);
    assert.match(refused.body.error, /credential refused/);
    assert.equal((await bridge.post('pair', {userId: 'gogh-auth-player', credential: 'wrong-credential-0123456789abcdef-zzzz'})).status, 403);
    await assert.rejects(bridge.join('gogh-auth-player', {credential: 'wrong-credential-0123456789abcdef-zzzz'}), error => error.status === 403 && error.fatal === true);
    let seatToken;
    const client = await connectLive({ pairUrl: `${bridge.base}/api/liveengine/pair`, renewUrl: `${bridge.base}/api/liveengine/renew`, body: {userId: 'gogh-auth-player'}, credential: USER_TOKEN, direct: false, fetch: async (url, init) => {
      const response = await fetch(url, {...init, headers: {...init.headers, Origin: bridge.base}});
      if (url.endsWith('/pair') && response.ok) seatToken = (await response.clone().json()).seatToken;
      return response;
    }});
    clients.push(client);
    assert.equal(client.state, 'open');
    assert.deepEqual(client.identity, {identity: 'gogh-auth-player', claims: {tier: 'gold'}});
    // Credentials are not kept: renewing needs the player to present one again.
    assert.equal((await bridge.post('renew', {userId: 'gogh-auth-player'})).status, 403);
    assert.equal((await bridge.post('renew', {userId: 'gogh-auth-player', seatToken})).status, 403);
    assert.equal((await bridge.post('renew', {userId: 'gogh-auth-player', seatToken, credential: USER_TOKEN})).status, 200);
  });

  test('a credential cannot mint a ticket for a different player id', async () => {
    assert.equal((await bridge.post('pair', {userId: 'someone-else', credential: USER_TOKEN})).status, 403);
  });
});

describe('bridge peer pairing and registry watch', () => {
  let bridge, peerRoom, peerSeatToken;
  before(async () => { bridge = await startBridge({LIVEENGINE_HUB_ID: 'gogh-peers', GOGH_LIVE_LINK: 'peer', GOGH_PEER_WAIT_MS: '5000'}); });

  test('the first player waits, the second completes the pairing, and both get a peer-peer descriptor for one room', async () => {
    const first = bridge.post('pair', {userId: 'gogh-peer-a', team: 0, role: 'vanguard'});
    // The first request is held while no other player exists.
    assert.equal(await Promise.race([first, sleep(500).then(() => 'waiting')]), 'waiting');
    const second = await bridge.post('pair', {userId: 'gogh-peer-b', team: 1, role: 'vanguard'});
    const held = await first;
    assert.equal(second.status, 200);
    assert.equal(held.status, 200);
    for (const result of [held, second]) {
      assert.equal(result.body.link, 'peer-peer');
      assert.ok(result.body.ticket);
    }
    assert.equal(held.body.room, second.body.room);
    peerRoom = held.body.room;
    peerSeatToken = second.body.seatToken;
    assert.equal(held.body.userId, 'gogh-peer-a');
    assert.equal(second.body.userId, 'gogh-peer-b');
    // Their tickets work: both connect and see each other.
    const a = new LiveClient(held.body, {direct: false});
    const b = new LiveClient(second.body, {direct: false});
    clients.push(a, b);
    await a.connect();
    await b.connect();
    await until(() => a.peers().includes('gogh-peer-b') && b.peers().includes('gogh-peer-a'), 'both peers to see each other');
  });

  test('renew gives a paired peer a fresh ticket without pairing again', async () => {
    const renewed = await bridge.post('renew', {userId: 'gogh-peer-b', seatToken: peerSeatToken});
    assert.equal(renewed.status, 200);
    assert.equal(renewed.body.link, 'peer-peer');
    assert.equal(renewed.body.room, peerRoom);
    assert.equal((await bridge.post('renew', {userId: 'gogh-peer-nobody'})).status, 403);
  });

  test('a lone player is told when no one arrived, and is not left registered', async () => {
    const other = await startBridge({LIVEENGINE_HUB_ID: 'gogh-lonely', GOGH_LIVE_LINK: 'peer', GOGH_PEER_WAIT_MS: '700'});
    const result = await other.post('pair', {userId: 'gogh-lonely-a', team: 0, role: 'vanguard'});
    assert.equal(result.status, 408);
    assert.equal((await other.post('renew', {userId: 'gogh-lonely-a'})).status, 403);
  });

  test('watch issues a ticket the browser can open the registry with, and shows no token', async () => {
    const issued = await bridge.post('watch', {depth: 1});
    assert.equal(issued.status, 200);
    assert.match(issued.body.url, /^ws:\/\/127\.0\.0\.1:\d+\/v1\/live\/registry$/);
    assert.ok(!JSON.stringify(issued.body).includes(ADMIN_TOKEN));
    const watch = await admin.watchRegistry({ticket: issued.body.ticket, path: issued.body.path, depth: issued.body.depth});
    try {
      assert.ok(JSON.stringify(watch.snapshot).includes('gogh-peers'));
      const added = new Promise(resolve => watch.on('added', resolve));
      await admin.register({id: 'gogh-watched', mode: 'internal'});
      assert.ok(JSON.stringify(await added).includes('gogh-watched'));
      await admin.remove('gogh-watched');
    } finally { watch.close(); }
  });
});
