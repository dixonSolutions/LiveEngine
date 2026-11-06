// The real local end-to-end test for Gogh Strike on LiveEngine. No mocks: real liveengine-hub
// processes, the real bridge, the real game, and headless Chromium contexts that play it.
//
//   PLAYWRIGHT_PATH=/path/to/playwright CHROMIUM_PATH=/path/to/chrome \
//   GOGH_STRIKE_ROOT=../gogh-strike npm run test:e2e
//
// Everything listens on 127.0.0.1 on free ports and is stopped at the end, also on failure. One
// PASS or FAIL line is printed per scenario; any failure exits non-zero. Nothing is skipped: a
// scenario whose setup breaks fails.
//
// Chromium flags used (see WEBRTC_ARGS and GPU_ARGS below): the two WebRTC flags let contexts of one
// browser find local candidates, and the GPU flags make the game render fast enough to test.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {existsSync} from 'node:fs';
import net from 'node:net';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {LiveClient} from '../../lib/live-client.mjs';
import {freePort, startHub} from '../../test/support/hub-process.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const {HubClient} = require('../../lib/hubs.js');
const bridgeScript = path.resolve(here, 'bridge.mjs');
const gameRoot = path.resolve(process.env.GOGH_STRIKE_ROOT || path.join(here, '../../../gogh-strike'));
const WEBRTC_ARGS = ['--disable-features=WebRtcHideLocalIpsWithMdns', '--allow-loopback-in-peer-connection'];
// Software GL draws the town at under one frame a second here, which starves the page's WebRTC
// callbacks; on a machine with a render node, ANGLE over Vulkan uses the GPU, still headless.
// GOGH_E2E_CHROMIUM_ARGS replaces the whole list.
const GPU_ARGS = ['--use-gl=angle', '--use-angle=vulkan', '--enable-features=Vulkan', '--ignore-gpu-blocklist', '--enable-gpu'];
const CHROMIUM_ARGS = process.env.GOGH_E2E_CHROMIUM_ARGS ? process.env.GOGH_E2E_CHROMIUM_ARGS.split(' ').filter(Boolean)
  : [...WEBRTC_ARGS, ...(existsSync('/dev/dri/renderD128') ? GPU_ARGS : [])];
const secret = () => randomBytes(24).toString('hex');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, what, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
    await sleep(50);
  }
}

// ---- processes ----

const closers = [];
const onClose = fn => { closers.unshift(fn); };
async function closeAll() {
  for (const close of closers.splice(0)) await Promise.resolve().then(close).catch(() => {});
}

const centralToken = secret();
const remoteToken = secret();
const USER_TOKEN = `gogh-player-${secret()}`;
const AUTH_USER = 'gogh-e2e-auth-player';
const WRONG_TOKEN = `wrong-${secret()}`;

// A TCP proxy for the hub's live socket. Each connection is tagged with the User-Agent of the
// browser context that opened it, so one player's connection can be cut or held down alone.
async function startTaggedProxy(targetPort) {
  const open = new Set();
  const held = new Map();
  const server = net.createServer(client => {
    let upstream;
    const entry = {client, tag: ''};
    open.add(entry);
    const done = () => { open.delete(entry); client.destroy(); upstream?.destroy(); };
    client.once('data', chunk => {
      entry.tag = /user-agent: ([^\r\n]*)/i.exec(chunk.toString('latin1'))?.[1] || '';
      if ((held.get(entry.tag) || 0) > Date.now()) return done();
      upstream = net.connect(targetPort, '127.0.0.1');
      entry.upstream = upstream;
      upstream.on('error', done).on('close', done);
      upstream.write(chunk);
      client.pipe(upstream);
      upstream.pipe(client);
    });
    client.on('error', done).on('close', done);
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const proxy = {
    port: server.address().port,
    /** Cut every open connection whose browser sent `tag` as its User-Agent. Returns how many. */
    sever(tag) {
      let cut = 0;
      for (const entry of [...open]) if (entry.tag.includes(tag)) { entry.client.destroy(); entry.upstream?.destroy(); cut++; }
      return cut;
    },
    /** Refuse new connections for `tag` for `ms`. */
    hold(tag, ms) { held.set(tag, Date.now() + ms); },
    stop: () => new Promise(resolve => { for (const entry of open) { entry.client.destroy(); entry.upstream?.destroy(); } server.close(resolve); }),
  };
  onClose(() => proxy.stop());
  return proxy;
}

const bridges = [];
async function startBridge(port, hubUrl, token, env = {}) {
  const output = [];
  const child = spawn(process.execPath, [bridgeScript], {
    cwd: gameRoot, stdio: ['ignore', 'pipe', 'pipe'],
    env: {...process.env, PORT: String(port), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: gameRoot,
      LIVEENGINE_URL: hubUrl, LIVEENGINE_TOKEN: token, GOGH_REQUEST_LIMIT: '100000', ...env},
  });
  child.stdout.on('data', chunk => output.push(String(chunk)));
  child.stderr.on('data', chunk => output.push(String(chunk)));
  const bridge = {base: `http://127.0.0.1:${port}`, output: () => output.join('')};
  bridges.push(bridge);
  onClose(async () => { child.kill('SIGTERM'); await new Promise(resolve => child.once('exit', resolve)); });
  await until(async () => { try { return (await fetch(`${bridge.base}/health`)).ok; } catch { return false; } }, 'the bridge to start');
  bridge.post = async (route, body = {}) => {
    const response = await fetch(`${bridge.base}/api/liveengine/${route}`, {
      method: 'POST', headers: {'Content-Type': 'application/json', Origin: bridge.base}, body: JSON.stringify(body),
    });
    const text = await response.text();
    return {status: response.status, body: text ? JSON.parse(text) : null};
  };
  // Registers the game's schemas and the hub the first time a bridge is used.
  bridge.warm = async () => assert.equal((await bridge.post('discover', {kind: 'hub'})).status, 200, `bridge could not reach its hub\n${bridge.output()}`);
  return bridge;
}

// ---- browser ----

let browser;
const pageErrors = [];
const players = [];

/** What each open page shows, for a failure line. */
async function diagnose() {
  const lines = [];
  for (const player of players) {
    if (player.page.isClosed()) continue;
    const state = await player.page.evaluate(() => ({phase: window.vanGoghStrike?.state?.phase, paused: window.vanGoghStrike?.state?.paused, rules: window.vanGoghStrike?.state?.rulePhase, time: window.vanGoghStrike?.state?.time, intro: window.vanGoghStrike?.state?.intro, locked: window.vanGoghStrike?.state?.locked, fps: window.vanGoghStrike?.state?.fps, fatal: document.querySelector('#fatal-error')?.hidden === false ? document.querySelector('#fatal-error-message')?.textContent : '',
      toast: document.querySelector('#toast')?.textContent, session: window.__e2e?.session?.state})).catch(error => ({unreadable: error.message.slice(0, 80)}));
    lines.push(`${player.label}: ${JSON.stringify(state)} console=${JSON.stringify(player.consoleErrors.slice(-3))} errors=${JSON.stringify(player.errors.slice(-2))}`);
  }
  return lines.join(' || ');
}

async function loadChromium() {
  const candidates = [];
  try { candidates.push(createRequire(import.meta.url)('playwright')); } catch { /* not installed here */ }
  if (process.env.PLAYWRIGHT_PATH) candidates.push(await import(pathToFileURL(path.resolve(process.env.PLAYWRIGHT_PATH)).href));
  for (const candidate of candidates) {
    const chromium = candidate.chromium || candidate.default?.chromium;
    if (chromium) return chromium;
  }
  throw new Error('Playwright not found: install it or set PLAYWRIGHT_PATH to a module exporting chromium');
}

/** A player: its own browser context, so its own sessionStorage, localStorage and WebSocket. */
async function openPlayer(label, base, {userId, credential} = {}) {
  // A small viewport keeps the software-rendered game light enough for two pages at once.
  const context = await browser.newContext({userAgent: `gogh-e2e-${label}`, viewport: {width: 640, height: 400}, deviceScaleFactor: 1});
  const page = await context.newPage();
  // The game renders in software here, so its page can be slow to settle.
  page.setDefaultTimeout(90_000);
  const errors = [];
  page.on('pageerror', error => { errors.push(error.message); pageErrors.push(`${label}: ${error.message}`); });
  if (userId) await page.addInitScript(([key, id]) => sessionStorage.setItem(key, id), ['gogh-strike-liveengine-user', userId]);
  // Only the first load sets it, so a test can replace it and reload.
  if (credential) await page.addInitScript(([key, value]) => { if (!localStorage.getItem(key)) localStorage.setItem(key, value); }, ['gogh-strike-liveengine-credential', credential]);
  const consoleErrors = [];
  page.on('console', message => { if (message.type() === 'error' || message.type() === 'warning') consoleErrors.push(message.text().slice(0, 200)); });
  const player = {label, base, context, page, errors, consoleErrors};
  players.push(player);
  player.load = async () => {
    // `?test` exposes the game's diagnostic hooks: teleport and read actors.
    await page.goto(`${base}/?test`, {waitUntil: 'domcontentloaded'});
    await page.waitForSelector('#deploy-button:not(:disabled)', {timeout: 90_000});
    await page.locator('#liveengine-matchmaking').check();
  };
  await player.load();
  player.deploy = () => page.locator('#deploy-button').click();
  // The phase flips at once; the simulation (game time) starts after the intro, and only then does the game send state.
  player.playing = () => page.waitForFunction(() => window.vanGoghStrike?.state?.phase === 'playing' && window.vanGoghStrike.state.rulePhase === 'playing' && window.vanGoghStrike.state.time > 0.5, null, {timeout: 120_000});
  onClose(() => context.close());
  return player;
}

/** Listen on the live session in the page: frames by channel and route, transport changes, bad frames. */
async function instrument({page}) {
  await page.evaluate(async () => {
    const {getLiveSession} = await import('/src/liveengine-matchmaking.js');
    const session = getLiveSession();
    if (!session) throw new Error('the game has no live session');
    const log = window.__e2e = {session, counts: {}, transports: [], opens: [], heard: [], directFailed: []};
    session.on('message', frame => {
      const key = `${frame.channel}:${frame.via}`;
      log.counts[key] = (log.counts[key] || 0) + 1;
      log.heard.push({channel: frame.channel, from: frame.from, via: frame.via, payload: frame.payload});
      if (log.heard.length > 400) log.heard.shift();
    });
    session.on('transport', event => log.transports.push(event));
    session.on('open', event => log.opens.push(event));
    session.on('direct-failed', event => log.directFailed.push(event));
  });
}
// Why a direct link is not up, for a failure message.
const directWhy = ({page}) => page.evaluate(() => JSON.stringify({failed: window.__e2e.directFailed, links: [...window.__e2e.session.links.values()].map(l => ({ice: l.pc?.iceConnectionState, conn: l.pc?.connectionState, ch: l.channel?.readyState, failure: l.failure}))}));
const counts = ({page}) => page.evaluate(() => ({...window.__e2e.counts}));
const sessionInfo = ({page}) => page.evaluate(() => {
  const s = window.__e2e.session;
  return {id: s.id, state: s.state, peers: s.peers(), room: s.room, link: s._descriptor.link, ticket: s._descriptor.ticket,
    transport: Object.fromEntries(s.peers().map(peer => [peer, s.transportOf(peer)])), identity: s.identity};
});
const view = ({page}) => page.evaluate(() => {
  const t = window.vanGoghStrike.test;
  return {me: [t.player.position.x, t.player.position.z], remotes: t.actors.filter(a => a.remote).map(a => ({id: a.remoteId, x: a.position.x, z: a.position.z}))};
});
const distance = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);

/** Move the player far from where it stands. Returns the position it ended on. */
async function moveFar(player) {
  const before = (await view(player)).me;
  for (const [dx, dz] of [[16, 0], [-16, 0], [0, 16], [0, -16], [24, 24], [-24, -24]]) {
    await player.page.evaluate(([x, z]) => window.vanGoghStrike.test.teleport(x, z), [before[0] + dx, before[1] + dz]);
    const after = (await view(player)).me;
    if (distance(before, after) > 5) return after;
  }
  throw new Error(`${player.label} could not be moved away from ${before}`);
}

/** `watcher` shows `peerId` standing within `tolerance` of `target`. Returns what it saw. */
const sees = (watcher, peerId, target, what, tolerance = 2) => until(async () => {
  const {remotes} = await view(watcher);
  const remote = remotes.find(r => r.id === peerId);
  return remote && distance([remote.x, remote.z], target) < tolerance ? remote : null;
}, `${watcher.label} to see ${what}`, 15_000);

/** Each player moves and the other sees it, twice, so a single lucky frame cannot pass. */
async function seeEachOther(a, b, ids) {
  const moved = [];
  for (const [mover, watcher, id] of [[a, b, ids.a], [b, a, ids.b], [a, b, ids.a]]) {
    const before = (await view(watcher)).remotes.find(r => r.id === id);
    const target = await moveFar(mover);
    const seen = await sees(watcher, id, target, `${mover.label} at ${target.map(v => v.toFixed(1))}`);
    if (before) assert.ok(distance([before.x, before.z], target) > 3, `${mover.label} did not change position for ${watcher.label}`);
    moved.push(`${mover.label}->${watcher.label} ${seen.x.toFixed(1)},${seen.z.toFixed(1)}`);
  }
  return moved.join('; ');
}

// Players of a shared setup stay open for the scenarios that follow; the rest close with their scenario.
async function twoPlayers(labels, base, options = [{}, {}], shared = false) {
  const [a, b] = await Promise.all(labels.map((label, i) => openPlayer(label, base, options[i])));
  a.shared = b.shared = shared;
  return [a, b];
}

/** Deploy both. A peer pairing holds the first request until the second player arrives. */
async function deployBoth(a, b) {
  await a.deploy();
  await sleep(300);
  await b.deploy();
  await Promise.all([a.playing(), b.playing()]);
  await Promise.all([instrument(a), instrument(b)]);
  const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
  await until(async () => (await sessionInfo(a)).peers.includes(ib.id) && (await sessionInfo(b)).peers.includes(ia.id), 'both players to see each other in presence');
  return {a: ia.id, b: ib.id};
}

// ---- scenarios ----

const results = [];
const SCENARIO_MS = 420_000;
const only = process.env.GOGH_E2E_ONLY ? process.env.GOGH_E2E_ONLY.split(',') : null;
async function scenario(name, run) {
  const started = Date.now();
  const mark = players.length;
  const finish = () => Promise.all(players.slice(mark).filter(player => !player.shared).map(player => player.context.close().catch(() => {})));
  // A filtered run is for debugging: it says so and never exits 0.
  if (only && !only.includes(name.split(' ')[0])) { results.push({name, ok: false}); console.log(`SKIP ${name} | not selected by GOGH_E2E_ONLY`); return; }
  try {
    let timer;
    const detail = await Promise.race([run(), new Promise((resolve, reject) => { timer = setTimeout(() => reject(new Error(`scenario exceeded ${SCENARIO_MS / 1000}s`)), SCENARIO_MS); })]).finally(() => clearTimeout(timer));
    await finish();
    results.push({name, ok: true});
    console.log(`PASS ${name}${detail ? ` | ${detail}` : ''} (${((Date.now() - started) / 1000).toFixed(1)}s)`);
  } catch (error) {
    results.push({name, ok: false});
    const where = await diagnose();
    await finish();
    console.log(`FAIL ${name} | ${String(error?.message || error).replaceAll(centralToken, '[token]').replaceAll(remoteToken, '[token]')}${where ? `\n  pages: ${where}` : ''} (${((Date.now() - started) / 1000).toFixed(1)}s)`);
  }
}

// A setup that several scenarios share runs once. If it fails, each of them fails with its error.
const once = factory => { let promise; return () => (promise ||= factory()); };

async function main() {
  const chromium = await loadChromium();
  const ports = await Promise.all(Array.from({length: 8}, () => freePort()));
  const [portNested, portPeer, portRelay, portLinkA, portLinkB, portReconnect, portWatch, portSchema] = ports;
  const origins = ports.map(port => `http://127.0.0.1:${port}`).join(',');
  const common = {LIVEENGINE_LIVE_ALLOWED_ORIGINS: origins, LIVEENGINE_ALLOWED_REMOTE_HOSTS: '127.0.0.1', LIVEENGINE_ICE_SERVERS: '[]',
    // One bridge, the test and every player share the loopback address, so the default budget of 120 requests a minute is too small.
    LIVEENGINE_RATE_LIMIT_REQUESTS: '100000'};

  // Central service C, whose live socket sits behind a proxy so one player's connection can be cut.
  const centralPort = await freePort();
  const proxy = await startTaggedProxy(centralPort);
  const central = await startHub({...common, LIVEENGINE_TOKEN: centralToken, LIVEENGINE_BIND: `127.0.0.1:${centralPort}`,
    LIVEENGINE_PUBLIC_URL: `ws://127.0.0.1:${proxy.port}/v1/live`});
  onClose(() => central.stop());
  // A second, independent service for hub-to-hub.
  const remote = await startHub({...common, LIVEENGINE_TOKEN: remoteToken});
  onClose(() => remote.stop());
  const adminC = new HubClient(central.address, {token: centralToken});
  const adminR = new HubClient(remote.address, {token: remoteToken});

  browser = await chromium.launch({headless: true, args: CHROMIUM_ARGS, ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {})});
  onClose(() => browser.close());

  // 1. Central hub, a nested central and a hub created on demand with its own auth.
  await scenario('1 central, nested central and on-demand hub with its own auth', async () => {
    await adminC.contract();
    await adminC.register({id: 'region', mode: 'central'});
    const hubAuth = {kind: 'token', tokens: [{token: USER_TOKEN, identity: AUTH_USER, claims: {tier: 'gold'}}]};
    const bridge = await startBridge(portNested, central.address, centralToken, {
      LIVEENGINE_HUB_ID: 'gogh-auth', LIVEENGINE_HUB_PATH: 'region', GOGH_HUB_AUTH: JSON.stringify(hubAuth)});
    const before = await adminC.route(['region'], 'list');
    assert.ok(!(before.hubs || before).some(hub => hub.id === 'gogh-auth'), 'the hub exists before anyone asked for it');
    const player = await openPlayer('auth', bridge.base, {userId: AUTH_USER, credential: WRONG_TOKEN});
    try {
      await player.deploy();
      await player.page.waitForSelector('#fatal-error:not([hidden])', {timeout: 30_000});
      const message = await player.page.locator('#fatal-error-message').textContent();
      assert.match(message, /credential refused/, 'the browser did not show the refusal');
      assert.notEqual(await player.page.evaluate(() => window.vanGoghStrike.state.phase), 'playing');
      const created = (await adminC.route(['region'], 'list')).hubs.find(hub => hub.id === 'gogh-auth');
      assert.ok(created, 'the bridge did not create the hub on demand in the nested central');
      assert.equal((await adminC.list()).hubs.some(hub => hub.id === 'gogh-auth'), false, 'the hub leaked into the root registry');
      assert.equal((await adminC.route(['region'], 'get', {id: 'gogh-auth'})).auth.kind, 'token');
      assert.equal(await adminC.route(['region'], 'call', {id: 'gogh-auth', method: 'getUser', args: [AUTH_USER]}).then(r => r.result?.status ?? null), null, 'a refused player was paired anyway');
      // The good credential, same player id, same page after a reload.
      await player.page.evaluate(([key, value]) => localStorage.setItem(key, value), ['gogh-strike-liveengine-credential', USER_TOKEN]);
      await player.page.context().clearCookies();
      await player.load();
      await player.deploy();
      await player.playing();
      await instrument(player);
      const info = await sessionInfo(player);
      assert.deepEqual(info.identity, {identity: AUTH_USER, claims: {tier: 'gold'}});
      assert.equal(info.link, 'peer-hub');
      const toast = await player.page.locator('#toast').textContent();
      assert.match(toast, /Joined .* with 0 other players\./);
      const paired = await adminC.route(['region'], 'call', {id: 'gogh-auth', method: 'getUser', args: [AUTH_USER]});
      assert.equal(paired.result.status, 'paired');
      const tree = await adminC.tree(3);
      assert.ok(JSON.stringify(tree).includes('gogh-auth'), 'the tree under the central does not show the hub');
      assert.equal(player.errors.length, 0, `page errors: ${player.errors.join('; ')}`);
      return `hub gogh-auth under region, room ${info.room}, identity ${info.identity.identity}`;
    } finally { /* closed by scenario() */ }
  });

  // 2 and 3 share one pair of players: first they play over WebRTC, then the channel is cut.
  const peerPair = once(async () => {
    const bridge = await startBridge(portPeer, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-p2p', GOGH_LIVE_LINK: 'peer'});
    await bridge.warm();
    const [a, b] = await twoPlayers(['p2p-a', 'p2p-b'], bridge.base, [{}, {}], true);
    const ids = await deployBoth(a, b);
    return {a, b, ids, bridge};
  });

  await scenario('2 peer-peer over WebRTC: data channel open, frames carried, positions seen', async () => {
    const {a, b, ids} = await peerPair();
    assert.equal((await sessionInfo(a)).link, 'peer-peer');
    await until(async () => (await sessionInfo(a)).transport[ids.b] === 'direct' && (await sessionInfo(b)).transport[ids.a] === 'direct', 'both sides to report a direct transport', 30_000)
      .catch(async error => { throw new Error(`${error.message}; a=${await directWhy(a)} b=${await directWhy(b)}`); });
    for (const [player, peer] of [[a, ids.b], [b, ids.a]]) {
      const channel = await player.page.evaluate(peerId => {
        const link = window.__e2e.session.links.get(peerId);
        return {readyState: link.channel.readyState, label: link.channel.label, ice: link.pc.iceConnectionState, transport: window.__e2e.session.transportOf(peerId)};
      }, peer);
      assert.deepEqual([channel.readyState, channel.label, channel.transport], ['open', 'liveengine', 'direct'], `${player.label} data channel`);
    }
    const [beforeA, beforeB] = await Promise.all([counts(a), counts(b)]);
    await sleep(1500);
    const [afterA, afterB] = await Promise.all([counts(a), counts(b)]);
    const carried = {a: (afterA['state:direct'] || 0) - (beforeA['state:direct'] || 0), b: (afterB['state:direct'] || 0) - (beforeB['state:direct'] || 0)};
    assert.ok(carried.a >= 10 && carried.b >= 10, `the data channel carried too few state frames in 1.5 s: ${JSON.stringify(carried)}; counts a=${JSON.stringify(afterA)} b=${JSON.stringify(afterB)}`);
    const relayGrowth = (afterA['state:relay'] || 0) - (beforeA['state:relay'] || 0);
    assert.equal(relayGrowth, 0, 'state frames still arrived over the relay beside the open data channel');
    const moved = await seeEachOther(a, b, ids);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `direct state frames in 1.5 s a=${carried.a} b=${carried.b}; ${moved}`;
  });

  await scenario('3 forced fallback: data channel closed mid-game, traffic continues over the relay', async () => {
    const {a, b, ids} = await peerPair();
    await until(async () => (await sessionInfo(a)).transport[ids.b] === 'direct', 'a direct transport to start from', 30_000);
    const [beforeA, beforeB] = await Promise.all([counts(a), counts(b)]);
    await a.page.evaluate(peerId => window.__e2e.session.links.get(peerId).channel.close(), ids.b);
    await until(async () => (await sessionInfo(a)).transport[ids.b] === 'relay' && (await sessionInfo(b)).transport[ids.a] === 'relay', 'both sides to fall back to the relay');
    const events = await Promise.all([a, b].map(p => p.page.evaluate(() => window.__e2e.transports.map(t => t.transport))));
    assert.ok(events.every(list => list.includes('relay')), `no relay transport event: ${JSON.stringify(events)}`);
    const mid = await Promise.all([counts(a), counts(b)]);
    await sleep(1500);
    const [afterA, afterB] = await Promise.all([counts(a), counts(b)]);
    const relayed = {a: (afterA['state:relay'] || 0) - (mid[0]['state:relay'] || 0), b: (afterB['state:relay'] || 0) - (mid[1]['state:relay'] || 0)};
    assert.ok(relayed.a >= 10 && relayed.b >= 10, `too few state frames over the relay after the fallback: ${JSON.stringify(relayed)}`);
    assert.ok((afterA['state:direct'] || 0) - (mid[0]['state:direct'] || 0) <= 1 && (afterB['state:direct'] || 0) - (mid[1]['state:direct'] || 0) <= 1, 'direct frames kept arriving after the channel closed');
    const moved = await seeEachOther(a, b, ids);
    assert.ok((beforeA['state:direct'] || 0) > 0 && (beforeB['state:direct'] || 0) > 0);
    return `relay state frames in 1.5 s a=${relayed.a} b=${relayed.b}; ${moved}`;
  });

  // 4. peer-hub: the relay is the only path, and a service participant joins the room.
  await scenario('4 peer-hub over the relay with a service participant', async () => {
    const bridge = await startBridge(portRelay, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-relay'});
    await bridge.warm();
    const player = await openPlayer('relay', bridge.base);
    let service;
    try {
      await player.deploy();
      await player.playing();
      await instrument(player);
      const info = await sessionInfo(player);
      assert.equal(info.link, 'peer-hub');
      const descriptor = await adminC.pair({subject: {id: 'gogh-e2e-service', kind: 'hub', scopes: {game: 'gogh-strike'}}, target: {kind: 'hub', id: 'gogh-relay', requirements: {game: 'gogh-strike'}}});
      assert.equal(descriptor.link, 'hub-hub');
      service = new LiveClient(descriptor, {direct: false});
      const heard = [];
      service.on('message', frame => heard.push(frame));
      await service.connect();
      await until(async () => (await sessionInfo(player)).peers.includes('gogh-e2e-service') && service.peers().includes(info.id), 'presence between the page and the service');
      // The service plays an authoritative participant: it circles a point, as a valid state DTO.
      let n = 0;
      const me = (await view(player)).me;
      const ticker = setInterval(() => {
        const t = n / 8;
        service.send({n: n++, team: 1, role: 'vanguard', x: me[0] + 10 + 6 * Math.cos(t), y: 0, z: me[1] + 10 + 6 * Math.sin(t), yaw: t, pitch: 0, alive: true}, {channel: 'state'});
      }, 50);
      onClose(() => clearInterval(ticker));
      const seen = new Set();
      try {
        await until(async () => {
          const remote = (await view(player)).remotes.find(r => r.id === 'gogh-e2e-service');
          if (remote) seen.add(`${Math.round(remote.x)},${Math.round(remote.z)}`);
          return seen.size >= 4;
        }, 'the page to see the service participant move', 15_000);
      } finally { clearInterval(ticker); }
      assert.ok(heard.filter(f => f.channel === 'state' && f.from === info.id && f.via === 'relay').length >= 5, 'the service did not receive the page\'s state frames over the relay');
      const after = await sessionInfo(player);
      assert.equal(after.transport['gogh-e2e-service'], 'relay');
      assert.equal(service.transportOf(info.id), 'relay');
      assert.equal(await player.page.evaluate(() => window.__e2e.session.links.size), 0, 'a direct link exists on a peer-hub pairing');
      assert.equal((await counts(player))['state:direct'] || 0, 0);
      assert.equal(player.errors.length, 0, `page errors: ${player.errors.join('; ')}`);
      return `service frames from the page over the relay=${heard.filter(f => f.via === 'relay').length}; positions seen=${seen.size}`;
    } finally { service?.close(); }
  });

  // 5. hub-hub: two services, a link between them, one player on each.
  await scenario('5 hub-hub: two linked services, one player on each, seeing each other move', async () => {
    const hubId = 'gogh-link';
    const bridgeA = await startBridge(portLinkA, central.address, centralToken, {LIVEENGINE_HUB_ID: hubId});
    const bridgeB = await startBridge(portLinkB, remote.address, remoteToken, {LIVEENGINE_HUB_ID: hubId});
    await Promise.all([bridgeA.warm(), bridgeB.warm()]);
    const room = `pair:hub:${hubId}`;
    await adminC.call(hubId, 'upsertServer', [{id: room, capacity: 64}]);
    await adminR.call(hubId, 'upsertServer', [{id: room, capacity: 64}]);
    await adminR.call(hubId, 'upsertUser', [{id: 'link-service'}]);
    assert.equal((await adminR.call(hubId, 'join', ['link-service', room])).ok, true);
    const credential = await adminR.request(`/v1/hubs/${hubId}/link-credentials`, {method: 'POST', body: {}});
    await adminC.request('/v1/links', {method: 'POST', body: {id: 'gogh-link', address: remote.address, remoteToken: credential.token,
      hubId, userId: 'link-service', serverId: room, localHubId: hubId, localRoom: room}});
    onClose(() => adminC.request('/v1/links/gogh-link', {method: 'DELETE'}));
    await until(async () => (await adminC.request('/v1/links/gogh-link')).status === 'connected', 'the link to connect');
    const [a, b] = [await openPlayer('link-a', bridgeA.base), await openPlayer('link-b', bridgeB.base)];
    await a.deploy(); await b.deploy();
    await Promise.all([a.playing(), b.playing()]);
    await Promise.all([instrument(a), instrument(b)]);
    const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
    assert.notEqual(new URL(await a.page.evaluate(() => window.__e2e.session._descriptor.url)).port, new URL(await b.page.evaluate(() => window.__e2e.session._descriptor.url)).port, 'both players are on one service');
    assert.equal((await adminC.call(hubId, 'getUser', [ib.id])), null, 'the second player is a member of the first service');
    assert.equal((await adminR.call(hubId, 'getUser', [ia.id])), null, 'the first player is a member of the second service');
    await until(async () => (await sessionInfo(a)).peers.includes(ib.id) && (await sessionInfo(b)).peers.includes(ia.id), 'presence across the link');
    const moved = await seeEachOther(a, b, {a: ia.id, b: ib.id});
    const status = await adminC.request('/v1/links/gogh-link');
    assert.equal(status.status, 'connected');
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `${moved}; link ${JSON.stringify(status)}`;
  });

  // 6. Reconnect: cut one player's socket, hold it down, and bring it back.
  await scenario('6 reconnect: one player dropped mid-game returns with a fresh ticket and is seen again', async () => {
    const bridge = await startBridge(portReconnect, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-reconnect'});
    await bridge.warm();
    const [a, b] = await twoPlayers(['drop-a', 'drop-b'], bridge.base);
    await a.deploy(); await b.deploy();
    await Promise.all([a.playing(), b.playing()]);
    await Promise.all([instrument(a), instrument(b)]);
    const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
    await until(async () => (await sessionInfo(b)).peers.includes(ia.id), 'presence');
    await seeEachOther(a, b, {a: ia.id, b: ib.id});
    proxy.hold('gogh-e2e-drop-a', 2500);
    assert.equal(proxy.sever('gogh-e2e-drop-a'), 1, 'expected exactly one live socket for the dropped player');
    // The other player notices: presence leave, and the stand-in actor is released.
    await until(async () => !(await sessionInfo(b)).peers.includes(ia.id), 'the other player to see the leave', 10_000);
    await until(async () => !(await view(b)).remotes.some(r => r.id === ia.id), 'the stand-in actor to be released');
    assert.notEqual((await sessionInfo(a)).state, 'open');
    await until(async () => (await sessionInfo(a)).state === 'open', 'the dropped player to reconnect', 30_000);
    const info = await sessionInfo(a);
    assert.notEqual(info.ticket, ia.ticket, 'the reconnect reused the spent ticket');
    const reopened = await a.page.evaluate(() => window.__e2e.opens.map(o => o.reconnected));
    assert.ok(reopened.includes(true), 'the client did not report a reconnect');
    await until(async () => (await sessionInfo(b)).peers.includes(ia.id), 'the other player to see it return');
    const target = await moveFar(a);
    await sees(b, ia.id, target, 'the returned player at its new position');
    const moved = await seeEachOther(a, b, {a: ia.id, b: ib.id});
    assert.equal(b.errors.length, 0, `page errors: ${b.errors.join('; ')}`);
    return `reconnects=${reopened.filter(Boolean).length}; ${moved}`;
  });

  // 7. Live hub list in a page, from a watch ticket.
  await scenario('7 live hub list: a page with a watch ticket sees hubs created and removed', async () => {
    const bridge = await startBridge(portWatch, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-watch'});
    await bridge.warm();
    const context = await browser.newContext();
    onClose(() => context.close());
    const page = await context.newPage();
    // A page on the bridge's origin, which the hub's live origin list allows.
    await page.goto(`${bridge.base}/health`);
    await page.evaluate(async () => {
      const response = await fetch('/api/liveengine/watch', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({depth: 2})});
      const watch = await response.json();
      if (!response.ok) throw new Error(watch.error);
      const log = window.__watch = {frames: [], closed: false, url: watch.url};
      const socket = new WebSocket(watch.url);
      socket.addEventListener('open', () => socket.send(JSON.stringify({type: 'auth', ticket: watch.ticket, path: watch.path, depth: watch.depth})));
      socket.addEventListener('message', event => log.frames.push(String(event.data)));
      socket.addEventListener('close', () => { log.closed = true; });
      window.__watchSocket = socket;
    });
    const frames = () => page.evaluate(() => window.__watch.frames);
    const sawFrame = (type, id) => until(async () => (await frames()).find(text => { const f = JSON.parse(text); return f.type === type && text.includes(`"${id}"`); }), `a ${type} frame for ${id}`, 10_000);
    await until(async () => (await frames()).some(text => JSON.parse(text).type === 'snapshot'), 'the first snapshot', 10_000);
    const snapshot = JSON.parse((await frames()).find(text => JSON.parse(text).type === 'snapshot'));
    assert.ok(JSON.stringify(snapshot).includes('gogh-watch'), 'the snapshot does not list the game hub');
    await adminC.register({id: 'e2e-live-1', mode: 'internal', scopes: {game: 'gogh-strike'}});
    await sawFrame('added', 'e2e-live-1');
    await adminC.register({id: 'e2e-live-region', mode: 'central'});
    await adminC.route(['e2e-live-region'], 'register', {id: 'e2e-live-nested', mode: 'internal'});
    await sawFrame('added', 'e2e-live-nested');
    await adminC.remove('e2e-live-1');
    await sawFrame('removed', 'e2e-live-1');
    await adminC.route(['e2e-live-region'], 'delete', {id: 'e2e-live-nested'});
    await sawFrame('removed', 'e2e-live-nested');
    await adminC.remove('e2e-live-region');
    await sawFrame('removed', 'e2e-live-region');
    const all = (await frames()).join('\n');
    assert.ok(!all.includes(centralToken), 'the administrator token reached the page');
    assert.equal(await page.evaluate(() => window.__watch.closed), false, 'the watch socket closed');
    const types = (await frames()).map(text => JSON.parse(text).type);
    return `frames in the page: ${types.length} (${[...new Set(types)].join(', ')})`;
  });

  // 8. Schema rejection: a malformed frame is refused and the game carries on. It runs over the
  // relay: a direct frame never touches the hub, so only the hub can refuse it.
  const relayPair = once(async () => {
    const bridge = await startBridge(portSchema, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-schema'});
    await bridge.warm();
    const [a, b] = await twoPlayers(['schema-a', 'schema-b'], bridge.base, [{}, {}], true);
    return {a, b, ids: await deployBoth(a, b)};
  });
  await scenario('8 schema rejection: malformed frames are refused and play continues', async () => {
    const {a, b, ids} = await relayPair();
    // The hub closes a socket that sends a frame its channel schema refuses; the client then
    // reconnects with a fresh ticket. One frame at a time, so each refusal is seen on its own.
    const malformed = [
      [{n: 'one', x: 'far'}, 'state'],
      [{n: 5, team: 2, role: 'vanguard', x: 0, y: 0, z: 0, yaw: 0, pitch: 0, alive: true}, 'state'],
      [{n: 6, team: 0, role: 'vanguard', x: 0, y: 0, z: 0, yaw: 0, pitch: 0, alive: true, health: 99999}, 'state'],
      [{kind: 'shot', weapon: 'x'}, 'event'],
      [{kind: 'nonsense'}, 'event'],
    ];
    await b.page.evaluate(() => { window.__e2e.heard.length = 0; });
    const tickets = new Set([(await sessionInfo(a)).ticket]);
    for (const [payload, channel] of malformed) {
      const opens = await a.page.evaluate(() => window.__e2e.opens.length);
      await a.page.evaluate(([frame, name]) => window.__e2e.session.send(frame, {channel: name}), [payload, channel]);
      await until(() => a.page.evaluate(count => window.__e2e.opens.length > count && window.__e2e.session.state === 'open', opens), `the sender to reconnect after ${JSON.stringify(payload)}`, 30_000);
      const info = await sessionInfo(a);
      assert.ok(!tickets.has(info.ticket), 'the reconnect reused a spent ticket');
      tickets.add(info.ticket);
    }
    await sleep(500);
    const reconnects = await a.page.evaluate(() => window.__e2e.opens.filter(o => o.reconnected).length);
    assert.equal(reconnects, malformed.length, 'each refused frame should have closed the sender once');
    const sender = (await sessionInfo(a)).id;
    const bad = await b.page.evaluate(from => window.__e2e.heard.filter(f => f.from === from && (f.payload?.n === 'one' || f.payload?.team === 2 || f.payload?.health === 99999 || f.payload?.kind === 'nonsense' || (f.payload?.kind === 'shot' && !f.payload.origin))).length, sender);
    assert.equal(bad, 0, 'a malformed frame reached the other player');
    assert.equal((await sessionInfo(b)).state, 'open', 'the other player was disturbed');
    // And the game carries on: valid frames after the bad ones are delivered and seen.
    const moved = await seeEachOther(a, b, ids);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `${malformed.length} malformed frames refused; ${moved}`;
  });
}

for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => closeAll().finally(() => process.exit(130)));
try {
  await main();
} catch (error) {
  console.log(`FAIL setup | ${String(error?.stack || error).replaceAll(centralToken, '[token]').replaceAll(remoteToken, '[token]')}`);
  results.push({name: 'setup', ok: false});
} finally {
  await closeAll();
}
// A scenario that never ran (its setup broke) is a failure, not a skip.
for (let number = 1; number <= 8; number++) {
  if (results.some(result => result.name.startsWith(`${number} `))) continue;
  results.push({name: `${number}`, ok: false});
  console.log(`FAIL ${number} | did not run: setup failed`);
}
const failed = results.filter(result => !result.ok);
console.log(`${results.length - failed.length} passed, ${failed.length} failed of ${results.length} scenarios; page errors: ${pageErrors.length}`);
if (failed.length) process.exitCode = 1;
