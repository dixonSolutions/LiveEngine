// createHubApi against a real liveengine-hub process, a real http server and real WebSockets.
// Nothing is mocked: the handler is mounted in plain `http` and in a small Express-style router.
import assert from 'node:assert/strict';
import http from 'node:http';
import { createRequire } from 'node:module';
import { after, before, describe, test } from 'node:test';
import { createHubApi } from '../lib/hub-api.mjs';
import { LiveClient, watchHubs } from '../lib/live-client.mjs';
import { ADMIN_TOKEN, startHub } from './support/hub-process.mjs';

const require = createRequire(import.meta.url);
const { HubClient } = require('../lib/hubs.js');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, what, ms = 8000) {
  const deadline = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(15);
  }
}

const SCHEMAS = [
  { id: 'api.state', version: '1', schema: { type: 'object', additionalProperties: false, required: ['n'], properties: { n: { type: 'integer' } } } },
];
const CHANNELS = { state: { schemaRef: 'api.state@1', delivery: 'lossy', maxRate: 60 } };

let hub, admin;
const servers = [];
const apis = [];
const clients = [];
const seen = [];

async function serve(options, { mount = '' } = {}) {
  // The page is served from the same address, so its own origin is always allowed besides the listed ones.
  let base;
  const listed = options.allowedOrigins || [];
  const api = createHubApi({ client: new HubClient(hub.address, { token: ADMIN_TOKEN }), scopes: { app: 'test' }, schemas: SCHEMAS, channels: CHANNELS, ...options,
    allowedOrigins: origin => origin === base || listed.includes(origin) });
  apis.push(api);
  // A tiny Express-like router: strips `mount` the way `app.use(mount, api)` does and calls `next` on a miss.
  const server = http.createServer((req, res) => {
    if (mount && req.url.startsWith(`${mount}/`)) {
      req.url = req.url.slice(mount.length);
      return api(req, res, () => { res.writeHead(404, { 'x-next': 'yes' }); res.end('next'); });
    }
    if (mount) { res.writeHead(404); return res.end('outside'); }
    return api(req, res);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  servers.push(server);
  base = `http://127.0.0.1:${server.address().port}`;
  const request = async (route, { method = 'POST', body, origin = base, headers = {} } = {}) => {
    const response = await fetch(`${base}${mount}${options.basePath || ''}/${route}`, {
      method, headers: { ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...(origin ? { Origin: origin } : {}), ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    seen.push(text);
    return { status: response.status, headers: response.headers, text, body: text.startsWith('{') ? JSON.parse(text) : null };
  };
  return { api, base, request, server };
}

before(async () => { hub = await startHub({ LIVEENGINE_RATE_LIMIT_REQUESTS: '10000' }); admin = new HubClient(hub.address, { token: ADMIN_TOKEN }); });
after(async () => {
  for (const client of clients) client.close();
  for (const api of apis) api.close();
  for (const server of servers) await new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
  await hub?.stop();
});

describe('publishing and the public routes', () => {
  let app;
  before(async () => { app = await serve({ hubId: 'api-pub', allowedOrigins: ['https://page.example'], basePath: '/api' }); });

  test('setup publishes the schemas, the channels and the hub, and a second API on the same hub reuses them', async () => {
    await app.api.setup();
    assert.ok((await admin.listSchemas()).schemas.some(entry => entry.ref === 'api.state@1'));
    const registered = await admin.get('api-pub');
    assert.deepEqual(registered.channels, CHANNELS);
    assert.deepEqual(registered.scopes, { app: 'test' });
    const again = await serve({ hubId: 'api-pub' });
    await again.api.setup();
    assert.equal((await admin.list()).hubs.filter(entry => entry.id === 'api-pub').length, 1);
  });

  test('status, ping and the browser client are served with CORS only for allowed origins', async () => {
    const page = 'https://page.example';
    const status = await app.request('status', { method: 'GET', origin: page });
    assert.equal(status.status, 200);
    assert.deepEqual(status.body, { configured: true, hubId: 'api-pub', mode: 'internal', externalConfigured: false });
    assert.equal(status.headers.get('access-control-allow-origin'), page);
    assert.match(status.headers.get('vary'), /Origin/);
    const ping = await app.request('ping', { method: 'GET', origin: page });
    assert.equal(ping.status, 200);
    assert.equal(ping.body.ok, true);
    assert.ok(ping.body.hubPingMs >= 0 && ping.body.serverTimeMs > 0);
    assert.deepEqual([ping.body.waiting, ping.body.seats], [0, 0]);
    const script = await app.request('live-client.mjs', { method: 'GET', origin: page });
    assert.equal(script.status, 200);
    assert.match(script.headers.get('content-type'), /javascript/);
    assert.match(script.text, /export async function connectLive/);
    assert.equal(script.headers.get('access-control-allow-origin'), page);
    const noOrigin = await app.request('status', { method: 'GET', origin: '' });
    assert.equal(noOrigin.status, 200);
    assert.equal(noOrigin.headers.get('access-control-allow-origin'), null);
    const hostile = await app.request('status', { method: 'GET', origin: 'https://attacker.example' });
    assert.equal(hostile.status, 403);
    assert.equal(hostile.headers.get('access-control-allow-origin'), null);
  });

  test('a preflight from an allowed origin is answered, including the private network opt-in', async () => {
    const preflight = await app.request('pair', { method: 'OPTIONS', origin: 'https://page.example', headers: {
      'Access-Control-Request-Method': 'POST', 'Access-Control-Request-Headers': 'content-type', 'Access-Control-Request-Private-Network': 'true' } });
    assert.equal(preflight.status, 204);
    assert.equal(preflight.headers.get('access-control-allow-origin'), 'https://page.example');
    assert.match(preflight.headers.get('access-control-allow-headers'), /content-type/i);
    assert.equal(preflight.headers.get('access-control-allow-private-network'), 'true');
    assert.equal((await app.request('pair', { method: 'OPTIONS', origin: 'https://attacker.example' })).status, 403);
  });

  test('POST needs an allowed Origin, a valid user id and a JSON object; bodies and rates are limited; the token never appears', async () => {
    const page = 'https://page.example';
    assert.equal((await app.request('pair', { body: { userId: 'x' }, origin: '' })).status, 403);
    assert.equal((await app.request('pair', { body: { userId: 'x' }, origin: 'https://attacker.example' })).status, 403);
    assert.equal((await app.request('pair', { body: { userId: 'has space' }, origin: page })).status, 400);
    assert.equal((await app.request('pair', { body: {}, origin: page })).status, 400);
    const notObject = await fetch(`${app.base}/api/pair`, { method: 'POST', headers: { Origin: page }, body: '[1]' });
    assert.equal(notObject.status, 400);
    const big = await app.request('pair', { body: { userId: 'big', padding: 'x'.repeat(20_000) }, origin: page });
    assert.equal(big.status, 413);
    assert.equal((await app.request('nothing', { method: 'GET', origin: page })).status, 404);
    assert.equal((await app.request('status', { method: 'POST', body: {}, origin: page })).status, 405);
    assert.ok(!seen.join('').includes(ADMIN_TOKEN));
  });

  test('a request over the rate limit is refused with 429, and reads are counted apart from writes', async () => {
    const limited = await serve({ hubId: 'api-rate', allowedOrigins: [], limits: { requestsPerMinute: 3, readsPerMinute: 5 } });
    const post = () => limited.request('discover', { body: { kind: 'hub' } }).then(answer => answer.status);
    assert.deepEqual([await post(), await post(), await post(), await post()], [200, 200, 200, 429]);
    const get = () => limited.request('status', { method: 'GET', origin: '' }).then(answer => answer.status);
    assert.deepEqual(await Promise.all([get(), get(), get(), get(), get(), get()]), [200, 200, 200, 200, 200, 429]);
  });

  test('a router that mounts the handler under a prefix gets next() for a route it does not own', async () => {
    const mounted = await serve({ hubId: 'api-pub', allowedOrigins: [] }, { mount: '/hub' });
    const owned = await fetch(`${mounted.base}/hub/status`);
    assert.equal(owned.status, 200);
    const miss = await fetch(`${mounted.base}/hub/other`);
    assert.equal(miss.status, 404);
    assert.equal(miss.headers.get('x-next'), 'yes');
  });
});

describe('without a client', () => {
  test('every route that needs the hub answers 503 with the message, and status says not configured', async () => {
    const app = await serve({ client: undefined, hubId: 'api-none', notConfigured: 'Set the hub URL.', allowedOrigins: [] });
    assert.equal((await app.request('status', { method: 'GET', origin: '' })).body.configured, false);
    const pair = await app.request('pair', { body: { userId: 'nobody' } });
    assert.equal(pair.status, 503);
    assert.equal(pair.body.error, 'Set the hub URL.');
    assert.equal((await app.request('ping', { method: 'GET', origin: '' })).status, 503);
  });
});

describe('hub pairing: one shared room', () => {
  let app;
  before(async () => { app = await serve({ hubId: 'api-room', allowedOrigins: [], limits: { requestsPerMinute: 1000 } }); });
  const post = (route, body) => app.request(route, { body });

  test('pair, renew, heartbeat and leave track the seat; the descriptor carries a working single-use ticket', async () => {
    const paired = await post('pair', { userId: 'room-a' });
    assert.equal(paired.status, 200);
    assert.equal(paired.body.room, 'pair:hub:api-room');
    assert.equal(paired.body.userId, 'room-a');
    assert.deepEqual(app.api.stats(), { seats: 1, waiting: 0, pairings: 1 });
    const client = new LiveClient(paired.body, { direct: false });
    clients.push(client);
    await client.connect();
    assert.equal(client.state, 'open');
    const seatToken = paired.body.seatToken;
    const renewed = await post('renew', { userId: 'room-a', seatToken });
    assert.equal(renewed.status, 200);
    assert.notEqual(renewed.body.ticket, paired.body.ticket);
    assert.equal((await post('heartbeat', { userId: 'room-a', seatToken })).status, 200);
    assert.equal((await post('heartbeat', { userId: 'room-nobody' })).status, 403);
    assert.equal((await post('renew', { userId: 'room-nobody' })).status, 403);
    assert.equal((await post('leave', { userId: 'room-a', seatToken })).status, 200);
    assert.deepEqual(app.api.stats(), { seats: 0, waiting: 0, pairings: 0 });
    assert.equal((await post('renew', { userId: 'room-a' })).status, 403);
  });

  test('seat token prevents impersonation and is absent from other responses', async () => {
    const victim = await post('pair', { userId: 'seat-victim' });
    const token = victim.body.seatToken;
    assert.match(token, /^[a-f0-9]{64}$/);
    const socket = new LiveClient(victim.body, { direct: false });
    clients.push(socket);
    await socket.connect();
    for (const route of ['renew', 'leave', 'heartbeat', 'pair']) {
      for (const seatToken of [undefined, 'wrong-seat-token']) {
        const response = await post(route, { userId: 'seat-victim', seatToken });
        assert.equal(response.status, 403, route);
        assert.ok(!response.text.includes(token));
      }
    }
    assert.equal(socket.state, 'open');
    assert.equal((await admin.call('api-room', 'getUser', ['seat-victim'])).status, 'paired');
    for (const route of ['status', 'ping']) assert.ok(!(await app.request(route, { method: 'GET' })).text.includes(token));
    for (const [route, body] of [['discover', {}], ['watch', {}], ['heartbeat', { userId: 'seat-victim', seatToken: token }], ['renew', { userId: 'seat-victim', seatToken: token }]]) {
      const response = await post(route, body);
      assert.equal(response.status, 200, route);
      assert.ok(!response.text.includes(token), route);
    }
    const repeat = await post('pair', { userId: 'seat-victim', seatToken: token });
    assert.equal(repeat.status, 200);
    assert.equal(repeat.body.seatToken, token);
    assert.equal((await post('leave', { userId: 'seat-victim', seatToken: token })).status, 200);
  });

  test('discover lists the hub with only public fields, and watch issues a ticket without the token', async () => {
    const found = await post('discover', { kind: 'hub' });
    assert.ok(found.body.candidates.some(candidate => candidate.id === 'api-room'));
    const issued = await post('watch', { depth: 1 });
    assert.equal(issued.status, 200);
    assert.match(issued.body.url, /^ws:\/\/127\.0\.0\.1:\d+\/v1\/live\/registry$/);
    assert.ok(!JSON.stringify(issued.body).includes(ADMIN_TOKEN));
  });

  test('watchHubs shows the hub list from a ticket and follows hubs added and removed', async () => {
    const watch = await watchHubs({ watchUrl: `${app.base}/watch`, body: { depth: 1 }, fetch: (url, init) => fetch(url, { ...init, headers: { ...init.headers, Origin: app.base } }) })
      .catch(async error => { throw new Error(`${error.message} (${error.status})`); });
    try {
      assert.ok(watch.hubs().some(entry => entry.id === 'api-room'));
      const changed = new Promise(resolve => watch.on('change', list => { if (list.some(entry => entry.id === 'api-watched')) resolve(list); }));
      await admin.register({ id: 'api-watched', mode: 'internal' });
      assert.ok((await changed).some(entry => entry.id === 'api-watched'));
      const gone = new Promise(resolve => watch.on('change', list => { if (!list.some(entry => entry.id === 'api-watched')) resolve(list); }));
      await admin.remove('api-watched');
      await gone;
      assert.ok(watch.hubs().every(entry => entry.status && Array.isArray(entry.path)));
    } finally { watch.close(); }
  });

  test('the idle reaper releases a seat that stops sending heartbeats and keeps one that does not', async () => {
    const quick = await serve({ hubId: 'api-idle', allowedOrigins: [], limits: { idleMs: 500, requestsPerMinute: 1000 } });
    const send = (route, userId) => quick.request(route, { body: { userId } });
    assert.equal((await send('pair', 'idle-a')).status, 200);
    const active = await send('pair', 'idle-b');
    assert.equal(active.status, 200);
    await until(async () => {
      assert.equal((await quick.request('heartbeat', { body: { userId: 'idle-b', seatToken: active.body.seatToken } })).status, 200);
      return quick.api.stats().seats === 1;
    }, 'the idle seat to be released');
    assert.notEqual((await admin.call('api-idle', 'getUser', ['idle-a']))?.status, 'paired');
    assert.equal((await admin.call('api-idle', 'getUser', ['idle-b'])).status, 'paired');
  });
});

test('seat token protects legacy join and its owner can leave', async () => {
  const app = await serve({ hubId: 'api-legacy-seat', join: { server: { id: 'legacy-room', scopes: { app: 'test' }, maxUsers: 10 } }, limits: { requestsPerMinute: 1000 } });
  const post = (route, body) => app.request(route, { body });
  const joined = await post('join', { userId: 'legacy-seat' });
  assert.equal(joined.status, 200);
  const token = joined.body.seatToken;
  assert.match(token, /^[a-f0-9]{64}$/);
  for (const seatToken of [undefined, 'wrong-seat-token']) {
    assert.equal((await post('join', { userId: 'legacy-seat', seatToken })).status, 403);
    assert.equal((await post('leave', { userId: 'legacy-seat', seatToken })).status, 403);
  }
  assert.equal((await post('join', { userId: 'legacy-seat', seatToken: token })).status, 200);
  assert.equal((await post('leave', { userId: 'legacy-seat', seatToken: token })).status, 200);
});

test('seat release and reaping remove only API-created join and pair users, leaving no ghost candidate', async () => {
  for (const route of ['join', 'pair']) {
    for (const expiry of [false, true]) {
      const hubId = `seat-clean-${route}-${expiry}`;
      const app = await serve({ hubId, subject: () => ({ app: 'test', ghost: hubId }), join: { server: { id: 'seat-room', scopes: { app: 'test' }, maxUsers: 10 } }, limits: { idleMs: 250, requestsPerMinute: 1000 } });
      await app.api.setup();
      const userId = `seat-${route}-${expiry}`;
      const result = await app.request(route, { body: { userId } });
      assert.equal(result.status, 200);
      if (expiry) await until(() => app.api.stats().seats === 0, 'seat reaping');
      else assert.equal((await app.request('leave', { body: { userId, seatToken: result.body.seatToken } })).status, 200);
      assert.equal(await admin.call(hubId, 'getUser', [userId]), null);
      const candidates = await admin.discover({ kind: 'peer', requirements: { app: 'test' } });
      assert.ok(!candidates.candidates.some(candidate => candidate.id === userId));
      await assert.rejects(admin.pair({ subject: { id: `later-${route}-${expiry}`, kind: 'peer', scopes: { ghost: hubId } }, target: { kind: 'peer', requirements: { ghost: hubId } }, waitMs: 150 }), error => error.status === 408);
      await admin.call(hubId, 'upsertUser', [{ id: 'seat-preexisting', scopes: { app: 'test' } }]);
      const existing = await app.request(route, { body: { userId: 'seat-preexisting' } });
      assert.equal(existing.status, 200);
      assert.equal((await app.request('leave', { body: { userId: 'seat-preexisting', seatToken: existing.body.seatToken } })).status, 200);
      assert.ok(await admin.call(hubId, 'getUser', ['seat-preexisting']));
    }
  }
});

describe('peer pairing with waitMs', () => {
  let app;
  before(async () => { app = await serve({ hubId: 'api-peers', allowedOrigins: [], peerLink: { kind: 'peer', waitMs: 4000 }, limits: { requestsPerMinute: 1000 } }); });
  const post = (route, body) => app.request(route, { body });

  test('the first player waits and is counted, the second completes the pairing, both get one room', async () => {
    const first = post('pair', { userId: 'peer-a' });
    assert.equal(await Promise.race([first, sleep(400).then(() => 'waiting')]), 'waiting');
    assert.equal(app.api.stats().waiting, 1);
    assert.equal((await app.request('ping', { method: 'GET' })).body.waiting, 1);
    const second = await post('pair', { userId: 'peer-b' });
    const held = await first;
    assert.deepEqual([held.status, second.status], [200, 200]);
    assert.equal(held.body.link, 'peer-peer');
    assert.equal(held.body.room, second.body.room);
    assert.equal(app.api.stats().waiting, 0);
    const renewed = await post('renew', { userId: 'peer-b', seatToken: second.body.seatToken });
    assert.equal(renewed.body.room, held.body.room);
    await post('leave', { userId: 'peer-a', seatToken: held.body.seatToken });
    await post('leave', { userId: 'peer-b', seatToken: second.body.seatToken });
  });

  test('a page that drops its pairing request leaves the queue and no seat behind', async () => {
    const controller = new AbortController();
    const dropped = fetch(`${app.base}/pair`, { method: 'POST', headers: { Origin: app.base }, body: JSON.stringify({ userId: 'peer-drop', waitMs: 3000 }), signal: controller.signal }).catch(error => error.name);
    await until(() => app.api.stats().waiting === 1, 'the request to wait');
    await until(() => admin.call('api-peers', 'getUser', ['peer-drop']), 'the candidate to be registered');
    controller.abort();
    assert.equal(await dropped, 'AbortError');
    await until(async () => app.api.stats().waiting === 0 && (await admin.call('api-peers', 'getUser', ['peer-drop'])) === null, 'the seat to be gone');
    assert.equal(app.api.stats().seats, 0);
    // A later pair is unaffected: it waits and is not matched with the dropped player.
    const lone = post('pair', { userId: 'peer-after', waitMs: 600 });
    assert.equal((await lone).status, 408);
    assert.equal((await post('renew', { userId: 'peer-after' })).status, 403);
  });

  test('seat authorization refuses leave during a pending pairing', async () => {
    const controller = new AbortController();
    const waiting = fetch(`${app.base}/pair`, { method: 'POST', headers: { Origin: app.base }, body: JSON.stringify({ userId: 'peer-leave', waitMs: 4000 }), signal: controller.signal }).catch(error => error.name);
    await until(() => app.api.stats().waiting === 1, 'the request to wait');
    assert.equal((await post('leave', { userId: 'peer-leave' })).status, 403);
    controller.abort();
    assert.equal(await waiting, 'AbortError');
    await until(async () => app.api.stats().waiting === 0 && await admin.call('api-peers', 'getUser', ['peer-leave']) === null, 'cancelled seat');
  });

  test('the page may ask for a shorter wait, never a longer one than the API allows', async () => {
    const started = Date.now();
    assert.equal((await post('pair', { userId: 'peer-short', waitMs: 300 })).status, 408);
    assert.ok(Date.now() - started < 3000);
  });
});

describe('hub auth', () => {
  test('a refused credential is a 403 with the hub reason, and the right one connects', async () => {
    const token = 'api-auth-player-token-0123456789abcdef';
    const app = await serve({ hubId: 'api-auth', allowedOrigins: [], auth: { kind: 'token', tokens: [{ token, identity: 'api-player', claims: { tier: 'gold' } }] } });
    const refused = await app.request('pair', { body: { userId: 'api-player' } });
    assert.equal(refused.status, 403);
    assert.match(refused.body.error, /credential refused/);
    const accepted = await app.request('pair', { body: { userId: 'api-player', credential: token } });
    assert.equal(accepted.status, 200);
    assert.equal((await app.request('renew', { body: { userId: 'api-player' } })).status, 403);
  });
});

test('roomCapacity is passed to pairing', async () => {
  const app = await serve({ hubId: 'api-capacity', roomCapacity: 3, limits: { requestsPerMinute: 100 } });
  const answers = [];
  for (const userId of ['capacity-a', 'capacity-b', 'capacity-c']) answers.push(await app.request('pair', { body: { userId } }));
  assert.ok(answers.every(answer => answer.status === 200), JSON.stringify(answers));
  assert.ok(answers.every(answer => answer.body.room === answers[0].body.room));
  const room = await admin.call('api-capacity', 'getServer', [answers[0].body.room]);
  assert.equal(room.capacity, 3);
  assert.equal(room.members.length, 3);
});

test('browser sessions hide hub tokens and restrict the admin console', async () => {
  await admin.createAccount({ username: 'api-admin', role: 'admin', password: 'test-password-admin-123' });
  await admin.createAccount({ username: 'api-user', role: 'user', password: 'test-password-user-123' });
  const app = await serve({ hubId: 'api-accounts', adminConsole: true });
  assert.equal((await app.request('admin', { method: 'GET' })).status, 401);
  const login = await app.request('login', { body: { username: 'api-user', password: 'test-password-user-123' } });
  assert.equal(login.status, 200);
  assert.match(login.headers.get('set-cookie'), /HttpOnly; SameSite=Strict/);
  assert.equal(login.body.token, undefined);
  const cookie = login.headers.get('set-cookie').split(';')[0];
  assert.equal((await app.request('me', { method: 'GET', headers: { cookie } })).body.username, 'api-user');
  assert.equal((await app.request('admin', { method: 'GET', headers: { cookie } })).status, 403);
  assert.equal((await app.request('admin/accounts', { method: 'GET', headers: { cookie } })).status, 403);
  const adminLogin = await app.request('login', { body: { username: 'api-admin', password: 'test-password-admin-123' } });
  const adminCookie = adminLogin.headers.get('set-cookie').split(';')[0];
  assert.equal((await app.request('admin', { method: 'GET', headers: { cookie: adminCookie } })).status, 200);
  assert.equal((await app.request('admin/accounts', { method: 'GET', headers: { cookie: adminCookie } })).status, 200);
  assert.equal((await app.request('logout', { headers: { cookie }, body: {} })).status, 200);
  assert.equal((await app.request('me', { method: 'GET', headers: { cookie } })).status, 401);
});

test('browser session authenticates password hub pair and renewal', async () => {
  const app = await serve({ hubId: 'api-password', auth: { kind: 'password' } });
  const login = await app.request('login', { body: { username: 'api-user', password: 'test-password-user-123' } });
  const cookie = login.headers.get('set-cookie').split(';')[0];
  const paired = await app.request('pair', { headers: { cookie }, body: { userId: 'api-user' } });
  assert.equal(paired.status, 200, paired.text);
  assert.equal(paired.body.identity.identity, 'api-user');
  const renewed = await app.request('renew', { headers: { cookie }, body: { userId: 'api-user', seatToken: paired.body.seatToken } });
  assert.equal(renewed.status, 200, renewed.text);
  assert.ok(renewed.body.ticket);
});
