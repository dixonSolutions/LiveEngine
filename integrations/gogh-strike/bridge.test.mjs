import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import http from 'node:http';
import {mkdtemp, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import net from 'node:net';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {after, before, test} from 'node:test';
import {pruneRates} from './rate-limit.mjs';

let root;
const bridge = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'bridge.mjs');
let upstream, upstreamPort, bridgePort, child;
const users = new Map();
const calls = [];

function listen(server) {
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(server.address().port)));
}

async function freePort() {
  const socket = net.createServer();
  const port = await listen(socket);
  await new Promise(resolve => socket.close(resolve));
  return port;
}

function rawPost(port, headers, body) {
  return new Promise((resolve, reject) => {
    const request = http.request({hostname: '127.0.0.1', port, path: '/api/liveengine/join', method: 'POST',
      headers: {...headers, 'Content-Length': Buffer.byteLength(body)}}, response => {
      response.resume(); response.on('end', () => resolve(response.statusCode));
    });
    request.on('error', reject); request.end(body);
  });
}

before(async () => {
  root = await mkdtemp(path.join(tmpdir(), 'liveengine-game-fixture-'));
  await writeFile(path.join(root, 'index.html'), '<!doctype html><title>Gogh Strike fixture</title>');
  upstream = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const auth = req.headers.authorization;
    const json = (status, value) => { res.writeHead(status, {'Content-Type': 'application/json'}); res.end(JSON.stringify(value)); };
    if (url.pathname === '/v1/contract') return auth === 'Bearer test-administrator-token-long-enough' ? json(200, {protocol: 'liveengine.hub/v1'}) : json(401, {error: 'unauthorized'});
    if (url.pathname === '/v1/hubs' && req.method === 'GET') return auth === 'Bearer test-administrator-token-long-enough' ? json(200, {hubs: []}) : json(401, {error: 'unauthorized'});
    if (url.pathname === '/v1/hubs' && req.method === 'POST') {
      if (auth !== 'Bearer test-administrator-token-long-enough') return json(401, {error: 'unauthorized'});
      const dto = JSON.parse(await new Promise(resolve => { let body = ''; req.on('data', chunk => body += chunk); req.on('end', () => resolve(body)); }));
      calls.push({register: dto});
      return json(201, dto);
    }
    const call = url.pathname.match(/^\/v1\/hubs\/[^/]+\/call$/);
    if (call && req.method === 'POST') {
      if (auth !== 'Bearer test-administrator-token-long-enough') return json(401, {error: 'unauthorized'});
      const body = JSON.parse(await new Promise(resolve => { let raw = ''; req.on('data', chunk => raw += chunk); req.on('end', () => resolve(raw)); }));
      calls.push(body);
      const [arg] = body.args;
      if (body.method === 'getUser') return json(200, {result: users.get(arg) || null});
      if (body.method === 'upsertUser') { users.set(arg.id, {id: arg.id, status: 'waiting'}); return json(200, {result: true}); }
      if (body.method === 'autoJoin') {
        const user = users.get(arg); Object.assign(user, {status: 'paired', serverId: 'gogh-room'});
        return json(200, {result: {ok: true, userId: arg, serverId: 'gogh-room'}});
      }
      if (body.method === 'leave') { users.delete(arg); return json(200, {result: {ok: true, userId: arg}}); }
      return json(200, {result: true});
    }
    return json(404, {error: 'missing'});
  });
  upstreamPort = await listen(upstream);
  bridgePort = await freePort();
  child = spawn(process.execPath, [bridge], {
    cwd: root,
    env: {...process.env, PORT: String(bridgePort), HOST: '0.0.0.0', GOGH_STRIKE_ROOT: root,
      GOGH_ALLOWED_HOSTS: `gogh.test:${bridgePort}`,
      LIVEENGINE_URL: `http://127.0.0.1:${upstreamPort}`, LIVEENGINE_TOKEN: 'test-administrator-token-long-enough', LIVEENGINE_HUB_ID: 'gogh-test'},
    stdio: 'ignore',
  });
  let startupError;
  child.once('error', error => { startupError = error; });
  const base = `http://127.0.0.1:${bridgePort}`;
  for (let i = 0; i < 60; i++) {
    if (startupError) throw startupError;
    try { if ((await fetch(`${base}/health`)).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('Bridge process did not start');
});

after(async () => {
  child?.kill('SIGTERM');
  if (upstream?.listening) await new Promise(resolve => upstream.close(resolve));
  if (root) await rm(root, {recursive: true, force: true});
});

test('bridge serves the game and keeps the service bearer token server-side', async () => {
  const base = `http://127.0.0.1:${bridgePort}`;
  const page = await fetch(`${base}/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /Gogh Strike/i);
  const status = await (await fetch(`${base}/api/liveengine/status`)).json();
  assert.deepEqual(status, {configured: true, hubId: 'gogh-test', mode: 'internal', externalConfigured: false});
  assert.equal((await fetch(`${base}/api/liveengine/status`)).headers.get('authorization'), null);

  const joined = await fetch(`${base}/api/liveengine/join`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: base},
    body: JSON.stringify({userId: 'player-1', team: 1, role: 'vanguard'}),
  });
  assert.equal(joined.status, 200);
  assert.deepEqual(await joined.json(), {ok: true, userId: 'player-1', serverId: 'gogh-room'});
  assert.equal(calls[0].register.id, 'gogh-test');
  assert.equal(calls.at(-1).method, 'autoJoin');
  assert.equal(calls.some(call => call.method === 'upsertServer' && call.args[0].scopes.game === 'gogh-strike'), true);

  const callsBeforeAttack = calls.length;
  const crossOrigin = await fetch(`${base}/api/liveengine/join`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: 'https://attacker.example'},
    body: JSON.stringify({userId: 'attacker'}),
  });
  assert.equal(crossOrigin.status, 403);
  assert.match((await crossOrigin.json()).error, /Cross-origin/);
  const hostileHost = await rawPost(bridgePort, {'Content-Type': 'application/json', Origin: base, Host: 'attacker.example'}, JSON.stringify({userId: 'attacker'}));
  assert.equal(hostileHost, 403);
  assert.equal(calls.length, callsBeforeAttack);

  const networkHost = `gogh.test:${bridgePort}`;
  const networkOrigin = `http://${networkHost}`;
  const networkJoin = await rawPost(bridgePort, {'Content-Type': 'application/json', Origin: networkOrigin, Host: networkHost}, JSON.stringify({userId: 'network-player'}));
  assert.equal(networkJoin, 200);

  const repeated = await fetch(`${base}/api/liveengine/join`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: base},
    body: JSON.stringify({userId: 'player-1'}),
  });
  assert.equal(repeated.status, 200);
  assert.equal((await repeated.json()).existing, true);

  const externalPort = await freePort();
  const externalChild = spawn(process.execPath, [bridge], {
    cwd: root,
    env: {...process.env, PORT: String(externalPort), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: root,
      LIVEENGINE_URL: `http://127.0.0.1:${upstreamPort}`, LIVEENGINE_TOKEN: 'test-administrator-token-long-enough',
      LIVEENGINE_HUB_ID: 'gogh-remote', LIVEENGINE_HUB_MODE: 'external',
      LIVEENGINE_REMOTE_ADDRESS: 'http://127.0.0.1:9000', LIVEENGINE_REMOTE_HUB_ID: 'gogh',
      LIVEENGINE_REMOTE_TOKEN: 'separate-remote-token-32-characters'},
    stdio: 'ignore',
  });
  try {
    const externalBase = `http://127.0.0.1:${externalPort}`;
    for (let i = 0; i < 60; i++) {
      try { if ((await fetch(`${externalBase}/health`)).ok) break; } catch {}
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    const externalJoin = await fetch(`${externalBase}/api/liveengine/join`, {
      method: 'POST', headers: {'Content-Type': 'application/json', Origin: externalBase},
      body: JSON.stringify({userId: 'player-2'}),
    });
    assert.equal(externalJoin.status, 200);
    const registration = calls.find(call => call.register?.id === 'gogh-remote')?.register;
    assert.deepEqual(registration, {
      id: 'gogh-remote', mode: 'external', address: 'http://127.0.0.1:9000',
      remoteHubId: 'gogh', remoteToken: 'separate-remote-token-32-characters', scopes: {game: 'gogh-strike'},
    });
  } finally {
    externalChild.kill('SIGTERM');
  }
});


test('bridge rejects hostile Origin on status requests', async () => {
  const base = `http://127.0.0.1:${bridgePort}`;
  const statusAttack = await fetch(`${base}/api/liveengine/status`, {headers: {Origin: 'https://attacker.example'}});
  assert.equal(statusAttack.status, 403);
});

test('expired rate entries are removed while active timestamps remain', () => {
  const rates = new Map([['expired', [10, 20]], ['active', [20, 80]]]);
  pruneRates(rates, 100, 50);
  assert.deepEqual([...rates], [['active', [80]]]);
});

test('bridge rejects hostile Origin on leave and heartbeat', async () => {
  const base = `http://127.0.0.1:${bridgePort}`;
  const beforeCalls = calls.length;
  for (const action of ['leave', 'heartbeat']) {
    const response = await fetch(`${base}/api/liveengine/${action}`, {
      method: 'POST', headers: {'Content-Type': 'application/json', Origin: 'https://attacker.example'},
      body: JSON.stringify({userId: 'player-1'}),
    });
    assert.equal(response.status, 403);
  }
  assert.equal(calls.length, beforeCalls);
});

test('bridge enforces the request body byte limit', async () => {
  const base = `http://127.0.0.1:${bridgePort}`;
  const oversized = await rawPost(bridgePort, {'Content-Type': 'application/json', Origin: base}, JSON.stringify({userId: 'x', padding: 'é'.repeat(9000)}));
  assert.equal(oversized, 413);
});

test('bridge releases a seat on leave', async () => {
  const base = `http://127.0.0.1:${bridgePort}`;
  const leave = await fetch(`${base}/api/liveengine/leave`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: base}, body: JSON.stringify({userId: 'player-1'}),
  });
  assert.equal(leave.status, 200);
  assert.equal(users.has('player-1'), false);
  assert.equal(calls.at(-1).method, 'leave');
});

test('bridge reaps idle seats while retaining seats with heartbeats', async () => {
  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  const reaperChild = spawn(process.execPath, [bridge], {
    cwd: root,
    env: {...process.env, PORT: String(port), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: root,
      GOGH_IDLE_MS: '600', LIVEENGINE_URL: `http://127.0.0.1:${upstreamPort}`,
      LIVEENGINE_TOKEN: 'test-administrator-token-long-enough', LIVEENGINE_HUB_ID: 'gogh-reaper'},
    stdio: 'ignore',
  });
  const post = (action, userId) => fetch(`${base}/api/liveengine/${action}`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: base}, body: JSON.stringify({userId}),
  });
  try {
    let ready = false;
    for (let i = 0; i < 60; i++) {
      try { if ((await fetch(`${base}/health`)).ok) { ready = true; break; } } catch {}
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(ready, true);
    assert.equal((await post('join', 'idle-player')).status, 200);
    assert.equal((await post('join', 'active-player')).status, 200);
    let reaped = false;
    for (let i = 0; i < 7; i++) {
      await new Promise(resolve => setTimeout(resolve, 200));
      assert.equal((await post('heartbeat', 'active-player')).status, 200);
      if (!users.has('idle-player')) { reaped = true; break; }
    }
    assert.equal(reaped, true);
    assert.equal(users.has('active-player'), true);
    assert.equal(calls.some(call => call.method === 'leave' && call.args[0] === 'idle-player'), true);
    assert.equal(calls.some(call => call.method === 'leave' && call.args[0] === 'active-player'), false);
  } finally {
    reaperChild.kill('SIGTERM');
  }
});

test('bridge targets a nested central path for registration and calls', async () => {
  const seen = [];
  const nested = http.createServer(async (req, res) => {
    const json = (status, value) => { res.writeHead(status, {'Content-Type': 'application/json'}); res.end(JSON.stringify(value)); };
    if (req.url === '/v1/contract') return json(200, {protocol: 'liveengine.hub/v1'});
    if (req.url !== '/v1/route') return json(404, {error: 'unexpected route'});
    let raw = ''; for await (const chunk of req) raw += chunk;
    const dto = JSON.parse(raw); seen.push(dto);
    if (dto.op === 'list') return json(200, {hubs: []});
    if (dto.op === 'register') return json(201, dto.body);
    if (dto.op === 'call') {
      if (dto.body.method === 'getUser') return json(200, {result: null});
      if (dto.body.method === 'autoJoin') return json(200, {result: {ok: true, userId: dto.body.args[0], serverId: 'room'}});
      return json(200, {result: true});
    }
    return json(400, {error: 'unexpected operation'});
  });
  const nestedPort = await listen(nested);
  const port = await freePort();
  const proc = spawn(process.execPath, [bridge], {cwd: root, stdio: 'ignore', env: {...process.env,
    PORT: String(port), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: root,
    LIVEENGINE_URL: `http://127.0.0.1:${nestedPort}`, LIVEENGINE_TOKEN: 'test-administrator-token-long-enough',
    LIVEENGINE_HUB_ID: 'nested-game', LIVEENGINE_HUB_PATH: 'eu/games'}});
  try {
    const base = `http://127.0.0.1:${port}`;
    let ready = false;
    for (let i = 0; i < 60; i++) { try { if ((await fetch(`${base}/health`)).ok) { ready = true; break; } } catch {} await new Promise(resolve => setTimeout(resolve, 50)); }
    assert.ok(ready);
    const response = await fetch(`${base}/api/liveengine/join`, {method: 'POST', headers: {'Content-Type': 'application/json', Origin: base}, body: JSON.stringify({userId: 'nested-user'})});
    assert.equal(response.status, 200);
    assert.ok(seen.every(request => JSON.stringify(request.path) === JSON.stringify(['eu', 'games'])));
    assert.equal(seen.find(request => request.op === 'register').body.id, 'nested-game');
    assert.equal(seen.find(request => request.body.method === 'autoJoin').body.id, 'nested-game');
  } finally {
    proc.kill('SIGTERM');
    await new Promise(resolve => proc.once('exit', resolve));
    await new Promise(resolve => nested.close(resolve));
  }
});
