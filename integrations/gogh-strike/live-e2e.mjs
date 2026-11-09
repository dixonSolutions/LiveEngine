// The real local end-to-end test for Gogh Strike on LiveEngine. No mocks: real liveengine-hub
// processes, the real bridge, the real game, and headless Chromium contexts that play it.
//
//   npm run test:e2e
//
// GOGH_E2E_SERVER=package runs every scenario against the game's own server-live.mjs, built only from the
// @dixonsolutions/liveengine package installed in the game (GOGH_STRIKE_ROOT must have it); the default is
// this checkout's bridge. A scenario that cannot run in package mode fails with the reason.
//
// The game is fetched once into .cache/ (upstream Gogh Strike at the validated commit plus gogh-strike.patch);
// GOGH_STRIKE_ROOT overrides it. Playwright: npm install --no-save playwright && npx playwright install chromium,
// or PLAYWRIGHT_PATH and CHROMIUM_PATH for an existing install. Without CHROMIUM_PATH, the newest installed
// Chromium that renders a text input is used (test/support/browser.mjs); the run prints which and why.
//
// Everything listens on 127.0.0.1 on free ports (scenario 14 uses the machine's LAN address, never a
// Tailscale one) and is stopped at the end, also on failure. One
// PASS or FAIL line is printed per scenario; any failure exits non-zero. Nothing is skipped: a
// scenario whose setup breaks fails.
//
// Chromium flags used (see WEBRTC_ARGS, MEMORY_ARGS and GPU_ARGS below): the two WebRTC flags let contexts of one
// browser find local candidates, and the GPU flags make the game render fast enough to test.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHmac, randomBytes} from 'node:crypto';
import {existsSync, readdirSync, readFileSync, readlinkSync} from 'node:fs';
import net from 'node:net';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {LiveClient} from '../../lib/live-client.mjs';
import {launchChromium, loadChromium} from '../../test/support/browser.mjs';
import {startCoturn} from '../../test/support/coturn.mjs';
import {connectPlainClient, selectedPair, sendPlain, startPlainPage} from '../../test/support/plain-page.mjs';
import {freePort, startHub} from '../../test/support/hub-process.mjs';
import {ensureGame, lanAddress} from './setup.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const {HubClient} = require('../../lib/hubs.js');
const serverMode = process.env.GOGH_E2E_SERVER || 'bridge';
if (!['bridge', 'package'].includes(serverMode)) { console.log(`FAIL setup | GOGH_E2E_SERVER must be "bridge" or "package", not "${serverMode}"`); process.exit(1); }
// GOGH_STRIKE_ROOT, else the patched upstream clone setup.mjs keeps in .cache/ (fetched on first use).
let gameRoot;
try { gameRoot = ensureGame(); }
catch (error) { console.log(`FAIL setup | ${error.message}`); process.exit(1); }
const bridgeScript = serverMode === 'package' ? path.join(gameRoot, 'server-live.mjs') : path.resolve(here, 'bridge.mjs');
if (serverMode === 'package') {
  // The version the game resolved is the one under test; without the package or the server file nothing can run.
  const installed = path.join(gameRoot, 'node_modules/@dixonsolutions/liveengine/package.json');
  if (!existsSync(bridgeScript) || !existsSync(installed)) { console.log(`FAIL setup | package mode needs ${bridgeScript} and the installed @dixonsolutions/liveengine in ${gameRoot} (npm install there)`); process.exit(1); }
  console.log(`INFO package mode: Gogh Strike resolved @dixonsolutions/liveengine ${JSON.parse(readFileSync(installed, 'utf8')).version} from ${path.dirname(installed)}`);
}
const WEBRTC_ARGS = ['--disable-features=WebRtcHideLocalIpsWithMdns', '--allow-loopback-in-peer-connection'];
// Software GL draws the town at under one frame a second here, which starves the page's WebRTC
// callbacks; on a machine with a render node, ANGLE over Vulkan uses the GPU, still headless.
// GOGH_E2E_CHROMIUM_ARGS replaces the whole list.
const GPU_ARGS = ['--use-gl=angle', '--use-angle=vulkan', '--enable-features=Vulkan', '--ignore-gpu-blocklist', '--enable-gpu'];
// Low-end device mode makes each game page about a third smaller (twenty pages at once were 9.7 GB of renderers, now 6.6 GB).
const MEMORY_ARGS = ['--enable-low-end-device-mode'];
const CHROMIUM_ARGS = process.env.GOGH_E2E_CHROMIUM_ARGS ? process.env.GOGH_E2E_CHROMIUM_ARGS.split(' ').filter(Boolean)
  : [...WEBRTC_ARGS, ...MEMORY_ARGS, ...(existsSync('/dev/dri/renderD128') ? GPU_ARGS : [])];
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

// Whatever a scenario starts (bridges, extra hubs, contexts, containers) is stopped when that scenario ends,
// so memory does not pile up across 26 scenarios. What a shared setup (`once`) or the run itself starts
// lives until the run ends.
const closers = [];
let scoped = null;
let sharedDepth = 0;
const onClose = fn => { (scoped && !sharedDepth ? scoped : closers).unshift(fn); };
async function runClosers(list) {
  for (const close of list.splice(0)) await Promise.resolve().then(close).catch(() => {});
}
const closeAll = () => runClosers(closers);

const centralToken = secret();
const remoteToken = secret();
const USER_TOKEN = `gogh-player-${secret()}`;
const AUTH_USER = 'gogh-e2e-auth-player';
const WRONG_TOKEN = `wrong-${secret()}`;
const authToken = secret();

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
async function startBridge(port, hubUrl, token, env = {}, bindHost = '127.0.0.1') {
  const output = [];
  const child = spawn(process.execPath, [bridgeScript], {
    cwd: gameRoot, stdio: ['ignore', 'pipe', 'pipe'],
    env: {...process.env, PORT: String(port), HOST: bindHost, GOGH_STRIKE_ROOT: gameRoot,
      LIVEENGINE_URL: hubUrl, LIVEENGINE_TOKEN: token, GOGH_REQUEST_LIMIT: '100000', ...env},
  });
  // A failed spawn must fail its scenario, not take the whole run and its cleanup with it.
  child.once('error', error => output.push(`bridge failed to start: ${error.message}`));
  child.stdout.on('data', chunk => output.push(String(chunk)));
  child.stderr.on('data', chunk => output.push(String(chunk)));
  const bridge = {base: `http://${bindHost}:${port}`, output: () => output.join('')};
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

/** A player: its own browser context, so its own sessionStorage, localStorage and WebSocket. */
async function openPlayer(label, base, {userId, credential, online = false, browser: host = browser} = {}) {
  // A small viewport keeps the software-rendered game light enough for two pages at once.
  const context = await host.newContext({userAgent: `gogh-e2e-${label}`, viewport: {width: 640, height: 400}, deviceScaleFactor: 1});
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
  const player = {label, base, context, page, errors, consoleErrors, requests: []};
  page.on('request', request => player.requests.push(request.url()));
  players.push(player);
  player.load = async () => {
    // `?test` exposes the game's diagnostic hooks: teleport and read actors.
    await page.goto(`${base}/?test`, {waitUntil: 'domcontentloaded'});
    await page.waitForSelector('#deploy-button:not(:disabled)', {timeout: 90_000});
    // The online panel replaces the checkbox: a player who uses it never ticks it.
    if (!online) await page.locator('#liveengine-matchmaking').check();
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


// ---- the front screen's "Play online" panel, driven by clicking and typing ----

const phaseOf = ({page}) => page.locator('#online-state').getAttribute('data-phase');
const waitPhase = (player, want, ms = 30_000) => player.page.waitForFunction(phase => document.querySelector('#online-state')?.dataset.phase === phase, want, {timeout: ms})
  .catch(async error => { throw new Error(`${player.label} never reached the "${want}" state (now "${await phaseOf(player)}": ${await panelText(player)}); ${error.message.split('\n')[0]}`); });
const panelText = ({page}) => page.locator('#online-panel').innerText().then(text => text.replace(/\s+/g, ' ').trim());
const matchText = ({page}) => page.locator('#online-match').textContent();

/** Type `address` into the panel the way a player does: clear the field, type, press Connect. */
async function typeAddress(player, address) {
  const field = player.page.locator('#online-address');
  await field.click();
  await field.fill('');
  await field.pressSequentially(address);
  await player.page.locator('#online-connect').click();
}
async function connectPanel(player, address) {
  await typeAddress(player, address);
  await waitPhase(player, 'connected');
  await player.page.waitForFunction(() => /ping \d+ ms/.test(document.querySelector('#online-status')?.textContent || ''), null, {timeout: 20_000});
}
const findMatch = player => player.page.locator('#online-find').click();
/** Both sides say matched and, in plain words, how they are connected. */
async function reachDirect(player) {
  await waitPhase(player, 'matched');
  await player.page.waitForFunction(() => document.querySelector('#online-match')?.textContent.includes('Direct connection (peer to peer)'), null, {timeout: 40_000})
    .catch(async () => { throw new Error(`${player.label} did not reach a direct connection; the panel says: ${await matchText(player)}`); });
}
/** Press Play on both pages and wait until the game runs; the matched session is already open. */
async function playBoth(a, b) {
  await Promise.all([a.deploy(), b.deploy()]);
  await Promise.all([a.playing(), b.playing()]);
  await Promise.all([instrument(a), instrument(b)]);
  const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
  assert.equal(ia.room, ib.room, 'the two matched players are in different rooms');
  await until(async () => (await sessionInfo(a)).peers.includes(ib.id) && (await sessionInfo(b)).peers.includes(ia.id), 'both players to see each other in presence');
  await until(async () => (await sessionInfo(a)).transport[ib.id] === 'direct' && (await sessionInfo(b)).transport[ia.id] === 'direct', 'a direct WebRTC transport on both sides', 30_000)
    .catch(async error => { throw new Error(`${error.message}; a=${await directWhy(a)} b=${await directWhy(b)}`); });
  return {a: ia.id, b: ib.id};
}
const userIdOf = ({page}) => page.evaluate(() => sessionStorage.getItem('gogh-strike-liveengine-user'));
/** What the hub API itself reports, over plain HTTP, as a page would see it. */
async function apiPing(bridge) {
  const response = await fetch(`${bridge.base}/api/liveengine/ping`);
  assert.equal(response.status, 200, 'ping failed');
  return response.json();
}
/** One state frame from `from` reaches `to` while the pair is idle in the lobby. */
async function exchange(from, to, n) {
  const target = (await sessionInfo(to)).id;
  const mark = await to.page.evaluate(() => window.__e2e.heard.length);
  await from.page.evaluate(count => window.__e2e.session.send({n: count, team: 0, role: 'vanguard', x: 1, y: 0, z: 1, yaw: 0, pitch: 0, alive: true}, {channel: 'state'}), n);
  const sender = (await sessionInfo(from)).id;
  const frame = await until(() => to.page.evaluate(([count, id, seq]) => window.__e2e.heard.slice(count).find(f => f.from === id && f.payload?.n === seq), [mark, sender, n]), `${to.label} to receive the frame ${n} from ${from.label}`, 10_000);
  assert.ok(target && frame, 'the frame did not arrive');
  return frame.via;
}

// ---- password sign-in: the panel's sign-in area and the hub API's admin console ----

const secrets = new Set();
/** A throwaway password, made per run and masked in any failure line. */
function password() { const value = secret(); secrets.add(value); return value; }
const mask = text => [...secrets].reduce((out, value) => out.replaceAll(value, '[secret]'), String(text));
const shotDir = process.env.GOGH_E2E_SHOTS;
/** Save a screenshot when GOGH_E2E_SHOTS names a directory. */
async function shot(target, name) { if (shotDir) await target.screenshot({path: path.join(shotDir, `auth-${name}.png`)}); }
/** Click into a field and type into it, key by key, as typeAddress does: this Chromium ignores fill with a value. */
async function enter(field, value) {
  await field.click();
  await field.fill('');
  await field.pressSequentially(value);
  assert.equal(await field.inputValue(), value, 'the field did not take what was typed');
}
async function signIn(player, username, secretWord) {
  await enter(player.page.locator('#signin-username'), username);
  await enter(player.page.locator('#signin-password'), secretWord);
  await player.page.locator('#signin-submit').click();
}
const signedInAs = (player, name, role) => player.page.waitForFunction(text => document.querySelector('#signedin-as')?.textContent === text, `Signed in as ${name} (${role})`, {timeout: 30_000})
  .catch(async error => { throw new Error(`${player.label} never showed "Signed in as ${name} (${role})": ${await panelText(player)}; ${error.message.split('\n')[0]}`); });
const waitError = (player, pattern, ms = 30_000) => player.page.waitForFunction(([source, flags]) => new RegExp(source, flags).test(document.querySelector('#online-error')?.hidden ? '' : document.querySelector('#online-error')?.textContent || ''), [pattern.source, pattern.flags], {timeout: ms})
  .catch(async error => { throw new Error(`${player.label} never showed an error matching ${pattern}: ${await panelText(player)}; ${error.message.split('\n')[0]}`); });
/** Open the admin console from the signed-in admin's link. It opens in a new tab of the same browser. */
async function openConsole(player) {
  const [popup] = await Promise.all([player.context.waitForEvent('page'), player.page.locator('#admin-link').click()]);
  popup.setDefaultTimeout(30_000);
  popup.on('dialog', dialog => dialog.accept()); // the console asks before disabling, deleting, kicking or resetting a password
  popup.on('pageerror', error => { player.errors.push(`console: ${error.message}`); pageErrors.push(`${player.label} console: ${error.message}`); });
  await popup.waitForLoadState('domcontentloaded');
  await popup.getByRole('heading', {name: 'Hub administration'}).waitFor();
  return popup;
}
const accountRow = (popup, name) => popup.locator('#accounts tr').filter({has: popup.getByRole('cell', {name, exact: true})});
async function createAccount(popup, username, secretWord, role) {
  await enter(popup.locator('input[name=username]'), username);
  await enter(popup.locator('input[name=password]'), secretWord);
  await popup.locator('select[name=role]').selectOption(role);
  await popup.getByRole('button', {name: 'Create account'}).click();
  await popup.waitForFunction(() => /Account created/.test(document.querySelector('#message')?.textContent || ''), null, {timeout: 20_000})
    .catch(async error => { throw new Error(`the console did not confirm creating ${username}; it says "${await popup.locator('#message').textContent()}"; ${error.message.split('\n')[0]}`); });
}
/** Kick a player from the console's connection list, refreshing until their connection shows. */
async function kick(popup, user) {
  const button = popup.getByRole('button', {name: `Kick ${user}`, exact: true});
  await until(async () => { await popup.locator('#refresh-connections').click(); await sleep(250); return button.isVisible(); }, `${user} in the console's connection list`, 20_000);
  await button.click();
  await popup.waitForFunction(() => /Connection kicked/.test(document.querySelector('#message')?.textContent || ''), null, {timeout: 20_000});
}

// ---- scenarios ----

const results = [];
const SCENARIO_MS = 420_000;
const only = process.env.GOGH_E2E_ONLY ? process.env.GOGH_E2E_ONLY.split(',') : null;
async function scenario(name, run, release = []) {
  const started = Date.now();
  const mark = players.length;
  // A filtered run is for debugging: it says so and never exits 0.
  if (only && !only.includes(name.split(' ')[0])) { results.push({name, ok: false}); console.log(`SKIP ${name} | not selected by GOGH_E2E_ONLY`); return; }
  scoped = [];
  const own = scoped;
  // `release` names shared setups this is the last scenario to use: their players close now.
  const finish = async () => {
    scoped = null;
    for (const setup of release) { const shared = await setup().catch(() => null); for (const player of [shared?.a, shared?.b]) if (player) { player.shared = false; await player.context.close().catch(() => {}); } }
    await Promise.all(players.slice(mark).filter(player => !player.shared).map(player => player.context.close().catch(() => {})));
    await runClosers(own);
  };
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
    console.log(`FAIL ${name} | ${mask(String(error?.message || error).replaceAll(centralToken, '[token]').replaceAll(remoteToken, '[token]').replaceAll(authToken, '[token]'))}${where ? `\n  pages: ${where}` : ''} (${((Date.now() - started) / 1000).toFixed(1)}s)`);
  }
}

// A setup that several scenarios share runs once. If it fails, each of them fails with its error.
const once = factory => { let promise; return () => (promise ||= (async () => { sharedDepth++; try { return await factory(); } finally { sharedDepth--; } })()); };

async function main() {
  const chromium = await loadChromium();
  const ports = await Promise.all(Array.from({length: 21}, () => freePort()));
  const [portNested, portPeer, portRelay, portLinkA, portLinkB, portReconnect, portWatch, portSchema,
    portUi, portFour, portCancel, portPage, portApi, portOther, portWrongPath, portCorrect, portClosed, portSeat, portCapacity, portExternal, portSoft] = ports;
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

  // CHROMIUM_PATH if set, else the newest installed Chromium that renders a text input (the choice and the builds skipped are printed).
  let chosen;
  ({browser, choice: chosen} = await launchChromium(chromium, {args: CHROMIUM_ARGS}).catch(error => { throw new Error(`Chromium could not start: ${error.message}`); }));
  onClose(() => browser.close());

  // 1. Central hub, a nested central and a hub created on demand with its own auth.
  await scenario('seat impersonation: a third browser cannot renew or remove a peer learned from presence', async () => {
    const bridge = await startBridge(portSeat, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-seat'});
    await bridge.warm();
    const contexts = await Promise.all([0, 1, 2].map(() => browser.newContext()));
    for (const context of contexts) onClose(() => context.close());
    const pages = await Promise.all(contexts.map(context => context.newPage()));
    await Promise.all(pages.map(page => page.goto(`${bridge.base}/health`)));
    const ids = ['seat-browser-a', 'seat-browser-b', 'seat-browser-attacker'];
    for (let i = 0; i < pages.length; i++) {
      await pages[i].evaluate(async userId => {
        const {connectLive} = await import('/api/liveengine/live-client.mjs');
        window.seatTest = await connectLive({pairUrl: '/api/liveengine/pair', renewUrl: '/api/liveengine/renew', body: {userId}, direct: false});
      }, ids[i]);
    }
    await until(() => pages[2].evaluate(id => window.seatTest.peers().includes(id), ids[0]), 'attacker to learn victim id from presence');
    const room = (await adminC.call('gogh-seat', 'getUser', [ids[0]])).serverId;
    await pages[0].reload();
    await pages[0].evaluate(async userId => {
      const {connectLive} = await import('/api/liveengine/live-client.mjs');
      window.seatTest = await connectLive({pairUrl: '/api/liveengine/pair', renewUrl: '/api/liveengine/renew', body: {userId}, direct: false});
    }, ids[0]);
    assert.equal(await pages[0].evaluate(() => window.seatTest.state), 'open');
    assert.equal((await adminC.call('gogh-seat', 'getUser', [ids[0]])).serverId, room);
    const refused = await pages[2].evaluate(async userId => {
      const {connectLive} = await import('/api/liveengine/live-client.mjs');
      try { await connectLive({pairUrl: '/api/liveengine/pair', body: {userId}, direct: false}); return 200; }
      catch (error) { return error.status; }
    }, ids[0]);
    assert.equal(refused, 403);
    const statuses = await pages[2].evaluate(async () => {
      const userId = window.seatTest.peers().find(id => id === 'seat-browser-a');
      const results = [];
      for (const route of ['renew', 'leave']) {
        const response = await fetch(`/api/liveengine/${route}`, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({userId})});
        results.push(response.status);
      }
      return results;
    });
    assert.deepEqual(statuses, [403, 403]);
    assert.equal(await pages[0].evaluate(() => window.seatTest.state), 'open');
    assert.ok(await pages[1].evaluate(id => window.seatTest.peers().includes(id), ids[0]));
    assert.equal((await adminC.call('gogh-seat', 'getUser', [ids[0]])).status, 'paired');
    for (const page of pages) await page.evaluate(() => window.seatTest.leave());
    return 'both unauthorized requests refused; victim remains connected and seated';
  });

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
  }, [peerPair]);

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
  }, [relayPair]);
  // ---- Play online: the front screen, a hub API and real peer-to-peer matchmaking ----
  // Each of these drives the lobby panel by clicking and typing. Nothing reaches into the page to
  // pair or connect for the flow under test; the helpers only read what the panel and the session show.

  await scenario('9 front screen: two users connect, find a match, reach a direct connection and see each other move', async () => {
    const bridge = await startBridge(portUi, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-ui', GOGH_LIVE_LINK: 'peer'});
    const [a, b] = await twoPlayers(['ui-a', 'ui-b'], bridge.base, [{online: true}, {online: true}]);
    assert.equal(await a.page.locator('#online-address').inputValue(), bridge.base, 'the address does not default to the page origin');
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    assert.match(await panelText(a), /Connected to gogh-ui · ping \d+ ms · \d+ players? waiting/);
    await a.page.waitForFunction(() => document.querySelector('#online-hubs')?.textContent.includes('gogh-ui'), null, {timeout: 20_000});
    await findMatch(a);
    await waitPhase(a, 'searching');
    assert.match(await matchText(a), /Waiting for another player/);
    await until(async () => (await apiPing(bridge)).waiting === 1, 'the hub API to count one waiting player');
    await b.page.waitForFunction(() => /· 1 player waiting/.test(document.querySelector('#online-status')?.textContent || ''), null, {timeout: 20_000});
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    assert.equal((await apiPing(bridge)).waiting, 0);
    const ids = await playBoth(a, b);
    const moved = await seeEachOther(a, b, ids);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `room ${(await sessionInfo(a)).room}, direct on both sides; ${moved}`;
  });

  await scenario('10 four users at once: everyone matched, nobody in two rooms, every pair exchanges data, nobody left waiting', async () => {
    // GOGH_E2E_USERS tries a larger crowd.
    const crowd = Number(process.env.GOGH_E2E_USERS) || 4;
    const bridge = await startBridge(portFour, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-four', GOGH_LIVE_LINK: 'peer', GOGH_MATCH_CAPACITY: String(crowd)});
    const users = await Promise.all(Array.from({length: crowd}, (_, index) => `crowd-${index}`).map(label => openPlayer(label, bridge.base, {online: true})));
    await Promise.all(users.map(user => connectPanel(user, bridge.base)));
    await Promise.all(users.map(findMatch));
    await Promise.all(users.map(reachDirect));
    await Promise.all(users.map(instrument));
    const infos = await Promise.all(users.map(sessionInfo));
    // The hub seats a peer that pairs with an already seated one in that peer's room, so four players
    // may share one room or split into pairs. Either way each is in exactly one room and sees its roommates.
    const rooms = new Map();
    infos.forEach((info, index) => rooms.set(info.room, [...(rooms.get(info.room) || []), index]));
    assert.equal([...rooms.values()].flat().length, crowd, 'a player is in no room or in two');
    assert.equal(new Set(infos.map(info => info.id)).size, crowd, 'two pages share a player id');
    for (const [room, members] of rooms) {
      assert.ok(members.length >= 2, `room ${room} holds a lone player`);
      const seated = (await adminC.call('gogh-four', 'getServer', [room])).members;
      assert.deepEqual([...seated].sort(), members.map(index => infos[index].id).sort(), `the hub's members of ${room} differ from the pages'`);
    }
    for (const [index, info] of infos.entries()) {
      const mates = rooms.get(info.room).filter(other => other !== index).map(other => infos[other].id).sort();
      assert.deepEqual([...info.peers].sort(), mates, `${users[index].label} does not see exactly its roommates`);
      const member = await adminC.call('gogh-four', 'getUser', [info.id]);
      assert.deepEqual([member.status, member.serverId], ['paired', info.room], `${users[index].label} is not seated in exactly its own room`);
    }
    const via = [];
    let n = 100;
    for (const members of rooms.values()) {
      for (const from of members) for (const to of members) if (from !== to) via.push(await exchange(users[from], users[to], n++));
    }
    assert.ok(via.every(kind => kind === 'direct'), `frames did not travel over the data channel: ${via}`);
    const ping = await apiPing(bridge);
    assert.deepEqual([ping.waiting, ping.seats], [0, crowd], 'players are left waiting or seats are missing');
    for (const user of users) assert.match(await matchText(user), /Matched with .*Direct connection/);
    assert.equal(users.reduce((sum, user) => sum + user.errors.length, 0), 0, 'page errors');
    return `rooms ${[...rooms.values()].map(members => members.length).join('+')}, ${via.length} pairwise frames ${[...new Set(via)].join('/')}, waiting ${ping.waiting}, seats ${ping.seats}`;
  });

  await scenario('11 a user with no partner waits, cancels and leaves no seat behind; a later pair is unaffected', async () => {
    const bridge = await startBridge(portCancel, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-cancel', GOGH_LIVE_LINK: 'peer'});
    const lone = await openPlayer('cancel-lone', bridge.base, {online: true});
    await connectPanel(lone, bridge.base);
    await findMatch(lone);
    await waitPhase(lone, 'searching');
    const loneId = await userIdOf(lone);
    assert.ok(loneId, 'the lone player has no id');
    await until(async () => (await apiPing(bridge)).waiting === 1, 'the hub API to count the waiting player');
    assert.ok(await adminC.call('gogh-cancel', 'getUser', [loneId]), 'the waiting player is not a candidate on the hub');
    await lone.page.waitForFunction(() => /· 1 player waiting/.test(document.querySelector('#online-status')?.textContent || ''), null, {timeout: 20_000});
    await lone.page.locator('#online-cancel').click();
    await waitPhase(lone, 'connected');
    assert.match(await matchText(lone), /Find match pairs you/);
    await until(async () => { const ping = await apiPing(bridge); return ping.waiting === 0 && ping.seats === 0; }, 'the cancelled player to leave no seat in the hub API');
    await until(async () => (await adminC.call('gogh-cancel', 'getUser', [loneId])) === null, 'the cancelled player to leave the hub');
    // A later pair is unaffected: it matches with each other, never with the one who cancelled.
    const [a, b] = await twoPlayers(['cancel-a', 'cancel-b'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await Promise.all([instrument(a), instrument(b)]);
    const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
    assert.deepEqual([ia.peers, ib.peers], [[ib.id], [ia.id]], 'the pair was matched with someone else');
    assert.equal(ia.room, ib.room);
    assert.notEqual(await userIdOf(lone), ia.id);
    const via = [await exchange(a, b, 7), await exchange(b, a, 8)];
    assert.deepEqual(via, ['direct', 'direct']);
    const ping = await apiPing(bridge);
    assert.deepEqual([ping.waiting, ping.seats], [0, 2]);
    assert.equal(await adminC.call('gogh-cancel', 'getUser', [loneId]), null, 'the cancelled player came back');
    assert.equal(lone.errors.length + a.errors.length + b.errors.length, 0, 'page errors');
    return `cancelled ${loneId} left nothing; pair ${ia.id}/${ib.id} matched directly; seats ${ping.seats}`;
  });

  await scenario('12 the hub API is on a different origin from the page', async () => {
    const pageBase = `http://127.0.0.1:${portPage}`;
    const apiBase = `http://127.0.0.1:${portApi}`;
    const otherBase = `http://127.0.0.1:${portOther}`;
    // The page's own bridge only serves the game; the API lives on another port and allows that one page origin.
    const pageServer = await startBridge(portPage, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-unused'});
    const apiServer = await startBridge(portApi, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-xorigin', GOGH_LIVE_LINK: 'peer', GOGH_ALLOWED_ORIGINS: pageBase});
    const otherServer = await startBridge(portOther, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-unused-2'});
    assert.equal(pageServer.base, pageBase);
    assert.equal(apiServer.base, apiBase);
    const [a, b] = await twoPlayers(['x-a', 'x-b'], pageBase, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, apiBase), connectPanel(b, apiBase)]);
    await a.page.waitForFunction(() => document.querySelector('#online-hubs')?.textContent.includes('gogh-xorigin'), null, {timeout: 20_000});
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    const ids = await playBoth(a, b);
    const moved = await seeEachOther(a, b, ids);
    for (const player of [a, b]) {
      assert.ok(player.requests.some(url => url.startsWith(`${apiBase}/api/liveengine/pair`)), `${player.label} never called the other origin`);
      assert.ok(!player.requests.some(url => url.startsWith(`${pageBase}/api/liveengine/`)), `${player.label} called its own origin's API`);
      assert.ok(player.requests.some(url => url === `${apiBase}/api/liveengine/live-client.mjs`), `${player.label} did not load LiveClient from the API origin`);
    }
    // A page the API does not list is refused by the browser's own CORS check, and the panel says so.
    const blocked = await openPlayer('x-blocked', otherBase, {online: true});
    await typeAddress(blocked, apiBase);
    await blocked.page.waitForSelector('#online-error:not([hidden])', {timeout: 20_000});
    const error = await blocked.page.locator('#online-error').textContent();
    assert.match(error, /Could not reach a hub API at .*allows this page/);
    assert.equal(await phaseOf(blocked), 'idle');
    assert.equal(await blocked.page.locator('#online-find').isVisible(), false);
    assert.equal(otherServer.base, otherBase);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `page ${pageBase} -> API ${apiBase}; ${moved}; unlisted origin refused`;
  });

  await scenario('13 a wrong hub API address shows a clear error and the user can correct it', async () => {
    const bridge = await startBridge(portCorrect, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-correct', GOGH_LIVE_LINK: 'peer'});
    const player = await openPlayer('wrong', bridge.base, {online: true});
    const error = () => player.page.locator('#online-error');
    // 1. Nothing listens there.
    await typeAddress(player, `http://127.0.0.1:${portClosed}`);
    await player.page.waitForSelector('#online-error:not([hidden])', {timeout: 20_000});
    assert.match(await error().textContent(), new RegExp(`Could not reach a hub API at http://127\\.0\\.0\\.1:${portClosed}/api/liveengine\\. Check the address and that the server is running`));
    assert.equal(await phaseOf(player), 'idle');
    assert.equal(await player.page.locator('#online-body').isVisible(), false, 'the match controls show without a connection');
    assert.equal(await player.page.locator('#online-connect').isEnabled(), true, 'the user cannot try again');
    // 2. A server answers, but it is not the hub API.
    await typeAddress(player, `${bridge.base}/nowhere`);
    await player.page.waitForFunction(() => /not as a hub API/.test(document.querySelector('#online-error')?.textContent || ''), null, {timeout: 20_000});
    assert.match(await error().textContent(), /answered, but not as a hub API \(HTTP 404\)\. Check the address, including any path\./);
    assert.equal(await phaseOf(player), 'idle');
    // 3. Not an address at all.
    await typeAddress(player, 'http://');
    await player.page.waitForFunction(() => /is not an address/.test(document.querySelector('#online-error')?.textContent || ''), null, {timeout: 20_000});
    // 4. The user corrects it and connects; the error goes away and the address is remembered.
    await typeAddress(player, bridge.base);
    await waitPhase(player, 'connected');
    assert.equal(await error().isHidden(), true, 'the error stayed after a good connect');
    await player.page.waitForFunction(() => /Connected to gogh-correct · ping \d+ ms/.test(document.querySelector('#online-status')?.textContent || ''), null, {timeout: 20_000});
    await player.page.reload({waitUntil: 'domcontentloaded'});
    await player.page.waitForSelector('#deploy-button:not(:disabled)', {timeout: 90_000});
    assert.equal(await player.page.locator('#online-address').inputValue(), bridge.base, 'the address was not remembered');
    assert.equal(player.errors.length, 0, `page errors: ${player.errors.join('; ')}`);
    return 'unreachable, not-a-hub-API and malformed addresses each explained; corrected address connected and remembered';
  });

  await scenario('14 scenario 9 with everything bound to the LAN address instead of loopback', async () => {
    const lan = lanAddress();
    const [hubPort, pagePort] = await Promise.all([freePort(), freePort()]);
    const lanToken = secret();
    const lanBase = `http://${lan}:${pagePort}`;
    const lanHub = await startHub({LIVEENGINE_TOKEN: lanToken, LIVEENGINE_BIND: `${lan}:${hubPort}`, LIVEENGINE_PUBLIC_URL: `ws://${lan}:${hubPort}/v1/live`,
      LIVEENGINE_LIVE_ALLOWED_ORIGINS: lanBase, LIVEENGINE_ICE_SERVERS: '[]', LIVEENGINE_RATE_LIMIT_REQUESTS: '100000'});
    onClose(() => lanHub.stop());
    assert.equal(lanHub.address, `http://${lan}:${hubPort}`);
    const bridge = await startBridge(pagePort, lanHub.address, lanToken, {LIVEENGINE_HUB_ID: 'gogh-lan', GOGH_LIVE_LINK: 'peer'}, lan);
    assert.equal(bridge.base, lanBase);
    const [a, b] = await twoPlayers(['lan-a', 'lan-b'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    const ids = await playBoth(a, b);
    const moved = await seeEachOther(a, b, ids);
    const descriptorUrl = await a.page.evaluate(() => window.__e2e.session._descriptor.url);
    assert.equal(new URL(descriptorUrl).hostname, lan, 'the hub handed out a non-LAN address');
    for (const player of [a, b]) assert.ok(player.requests.every(url => !/^https?:\/\/127\.0\.0\.1/.test(url)), `${player.label} used the loopback address`);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `LAN ${lan}, hub ${descriptorUrl}, direct on both sides; ${moved}`;
  });

  await scenario('15 capacity two: five users form two rooms and the waiting user pairs with a sixth', async () => {
    const bridge = await startBridge(portCapacity, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-capacity', GOGH_LIVE_LINK: 'peer', GOGH_MATCH_CAPACITY: '2'});
    const users = await Promise.all(Array.from({length: 5}, (_, i) => openPlayer(`capacity-${i}`, bridge.base, {online: true})));
    await Promise.all(users.map(user => connectPanel(user, bridge.base)));
    await Promise.all(users.map(findMatch));
    await until(async () => { const ping = await apiPing(bridge); return ping.waiting === 1 && ping.seats === 4; }, 'four matched players and one waiting', 25000);
    // The API counts a seat before the page has shown it.
    await until(async () => (await Promise.all(users.map(phaseOf))).filter(phase => phase === 'matched').length === 4, 'four panels to show a match');
    const matched = [], waiting = [];
    for (const user of users) ((await phaseOf(user)) === 'matched' ? matched : waiting).push(user);
    assert.equal(matched.length, 4);
    assert.equal(waiting.length, 1);
    await Promise.all(matched.map(reachDirect));
    await Promise.all(matched.map(instrument));
    const infos = await Promise.all(matched.map(sessionInfo));
    const rooms = new Map();
    infos.forEach(info => rooms.set(info.room, (rooms.get(info.room) || 0) + 1));
    assert.deepEqual([...rooms.values()].sort(), [2, 2]);
    for (const room of rooms.keys()) assert.equal((await adminC.call('gogh-capacity', 'getServer', [room])).members.length, 2);
    const sixth = await openPlayer('capacity-5', bridge.base, {online: true});
    await connectPanel(sixth, bridge.base);
    await findMatch(sixth);
    await Promise.all([reachDirect(waiting[0]), reachDirect(sixth)]);
    await Promise.all([instrument(waiting[0]), instrument(sixth)]);
    const [a, b] = await Promise.all([sessionInfo(waiting[0]), sessionInfo(sixth)]);
    assert.equal(a.room, b.room);
    assert.ok(!rooms.has(a.room));
    assert.equal((await adminC.call('gogh-capacity', 'getServer', [a.room])).members.length, 2);
    return 'three rooms of two; the waiting player matched with the sixth';
  });

  // ---- Password sign-in: accounts, roles and admin tools, driven through the front screen and the admin console ----
  // One extra service owns the accounts for 16 to 21 and 23; 22 uses the second service ("remote"), which owns an external hub's accounts.
  const authPorts = await Promise.all(Array.from({length: 9}, () => freePort()));
  const authSetup = once(async () => {
    const service = await startHub({LIVEENGINE_TOKEN: authToken, LIVEENGINE_PASSWORD_ITERATIONS: '100000', LIVEENGINE_ALLOWED_REMOTE_HOSTS: '127.0.0.1',
      LIVEENGINE_LIVE_ALLOWED_ORIGINS: authPorts.map(port => `http://127.0.0.1:${port}`).join(','), LIVEENGINE_ICE_SERVERS: '[]', LIVEENGINE_RATE_LIMIT_REQUESTS: '100000'});
    onClose(() => service.stop());
    const admin = new HubClient(service.address, {token: authToken});
    const adminPassword = password();
    await admin.createAccount({username: 'root-e2e', password: adminPassword, role: 'admin'});
    return {service, admin, adminPassword};
  });
  const authBridge = async (port, hubId, env = {}) => {
    const {service} = await authSetup();
    return startBridge(port, service.address, authToken, {LIVEENGINE_HUB_ID: hubId, GOGH_HUB_AUTH_PASSWORD: '1', GOGH_LIVE_LINK: 'peer', ...env});
  };
  /** An account made through the API, as an admin would have made it in the console. Returns its password. */
  const makeUser = async (name, role = 'user', admin) => {
    const secretWord = password();
    await (admin || (await authSetup()).admin).createAccount({username: name, password: secretWord, role});
    return secretWord;
  };
  /** The admin signs in on the front screen and opens the console from its link. */
  async function adminConsole(label, bridge) {
    const {adminPassword} = await authSetup();
    const admin = await openPlayer(label, bridge.base, {online: true});
    await connectPanel(admin, bridge.base);
    await signIn(admin, 'root-e2e', adminPassword);
    await signedInAs(admin, 'root-e2e', 'admin');
    const popup = await openConsole(admin);
    return {admin, popup};
  }

  await scenario('16 sign-in: an admin creates accounts in the console, a user signs in, a wrong password is refused, a user cannot open the console', async () => {
    const bridge = await authBridge(authPorts[0], 'gogh-auth-a');
    const {adminPassword} = await authSetup();
    const admin = await openPlayer('auth-admin', bridge.base, {online: true});
    await admin.page.setViewportSize({width: 1000, height: 900});
    await connectPanel(admin, bridge.base);
    // Signed out: the sign-in area is there, the hub says what it needs and Find match waits for a sign-in.
    assert.equal(await admin.page.locator('#signin-form').isVisible(), true, 'no sign-in form');
    assert.equal(await admin.page.locator('#signedin').isVisible(), false);
    assert.equal(await admin.page.locator('#online-find').isDisabled(), true, 'Find match is open to a signed-out player');
    assert.match(await matchText(admin), /Sign in to find a match/);
    await shot(admin.page.locator('#online-panel'), 'signed-out');
    // The wrong password, and an unknown user, get the same plain words.
    await signIn(admin, 'root-e2e', password());
    await waitError(admin, /Wrong username or password/);
    await signIn(admin, 'nobody-e2e', password());
    await waitError(admin, /^Wrong username or password\.$/);
    assert.equal(await admin.page.locator('#signedin').isVisible(), false);
    await signIn(admin, 'root-e2e', adminPassword);
    await signedInAs(admin, 'root-e2e', 'admin');
    assert.equal(await admin.page.locator('#admin-link').isVisible(), true, 'an admin sees no link to the console');
    assert.equal(await admin.page.locator('#online-error').isHidden(), true, 'the error stayed after signing in');
    // The hub API keeps the hub session in an HttpOnly cookie; the page never holds it.
    assert.equal(await admin.page.evaluate(() => document.cookie), '', 'a cookie is readable by the page');
    const cookie = (await admin.context.cookies()).find(entry => entry.name === 'liveengine_session');
    assert.ok(cookie?.httpOnly && cookie.sameSite === 'Strict', `session cookie: ${JSON.stringify(cookie && {...cookie, value: '[value]'})}`);
    await shot(admin.page.locator('#online-panel'), 'signed-in');
    // The console: create two accounts.
    const popup = await openConsole(admin);
    const aliceSecret = password(), bobSecret = password();
    await createAccount(popup, 'alice-a', aliceSecret, 'user');
    await createAccount(popup, 'bob-a', bobSecret, 'admin');
    await accountRow(popup, 'alice-a').waitFor();
    assert.match(await accountRow(popup, 'bob-a').innerText(), /bob-a\s+Yes\s+admin/);
    await shot(popup, 'admin-console');
    // A password under ten characters is not accepted: the form will not send it and no account appears.
    await enter(popup.locator('input[name=username]'), 'short-a');
    await enter(popup.locator('input[name=password]'), 'short');
    await popup.getByRole('button', {name: 'Create account'}).click();
    await sleep(500);
    assert.equal(await accountRow(popup, 'short-a').count(), 0, 'an account with a short password was created');
    // The new user signs in on the front screen. No admin link; the console refuses them.
    const alice = await openPlayer('auth-alice', bridge.base, {online: true});
    await connectPanel(alice, bridge.base);
    await signIn(alice, 'alice-a', aliceSecret);
    await signedInAs(alice, 'alice-a', 'user');
    assert.equal(await alice.page.locator('#admin-link').isVisible(), false, 'a user sees the console link');
    const probe = await alice.context.newPage();
    const refused = await probe.goto(`${bridge.base}/api/liveengine/admin`);
    assert.equal(refused.status(), 403);
    assert.match(await probe.locator('body').innerText(), /Administrator access required/);
    const listed = await probe.evaluate(async () => (await fetch('/api/liveengine/admin/accounts')).status);
    assert.equal(listed, 403, 'a user listed accounts through the console route');
    const signedOutContext = await browser.newContext();
    onClose(() => signedOutContext.close());
    const anonymous = await signedOutContext.newPage();
    assert.equal((await anonymous.goto(`${bridge.base}/api/liveengine/admin`)).status(), 401);
    // Sign out puts the form back and drops the cookie.
    await alice.page.locator('#signout').click();
    await alice.page.waitForSelector('#signin-form:not([hidden])');
    assert.equal((await alice.context.cookies()).some(entry => entry.name === 'liveengine_session' && entry.value), false, 'the cookie survived signing out');
    assert.equal(await alice.page.evaluate(async () => (await fetch('/api/liveengine/me')).status), 401);
    assert.equal(admin.errors.length + alice.errors.length, 0, `page errors: ${[...admin.errors, ...alice.errors].join('; ')}`);
    return 'wrong password and unknown user refused alike; two accounts created in the console; user refused the console (403), anonymous 401; HttpOnly SameSite=Strict cookie';
  });

  await scenario('17 two signed-in users find a match peer to peer and each sees the other\'s username', async () => {
    const bridge = await authBridge(authPorts[1], 'gogh-auth-b');
    const [passA, passB] = [await makeUser('alice-b'), await makeUser('bob-b')];
    const [a, b] = await twoPlayers(['auth-b-alice', 'auth-b-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-b', passA);
    await signIn(b, 'bob-b', passB);
    await Promise.all([signedInAs(a, 'alice-b', 'user'), signedInAs(b, 'bob-b', 'user')]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    assert.match(await matchText(a), /Matched with bob-b \(user\)\./);
    assert.match(await matchText(b), /Matched with alice-b \(user\)\./);
    await Promise.all([instrument(a), instrument(b)]);
    const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
    assert.deepEqual([ia.id, ib.id], ['alice-b', 'bob-b'], 'the player ids are not the usernames');
    assert.deepEqual(ia.identity, {identity: 'alice-b', claims: {role: 'user'}});
    assert.equal(await a.page.evaluate(() => window.__e2e.session.identityOf('bob-b')?.identity), 'bob-b');
    assert.equal(await b.page.evaluate(() => window.__e2e.session.identityOf('alice-b')?.identity), 'alice-b');
    const via = [await exchange(a, b, 31), await exchange(b, a, 32)];
    assert.deepEqual(via, ['direct', 'direct'], 'frames did not travel peer to peer');
    // Without a session the hub API refuses to pair, whatever id is asked for.
    const unsigned = await bridge.post('pair', {userId: 'mallory-b'});
    assert.equal(unsigned.status, 403, `an unsigned pair answered ${unsigned.status}: ${JSON.stringify(unsigned.body)}`);
    const ids = await playBoth(a, b);
    const moved = await seeEachOther(a, b, ids);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `alice-b and bob-b matched directly, each shown by name; ${moved}`;
  });

  await scenario('18 the admin kicks one player in the console: they see they were removed and can rejoin', async () => {
    const bridge = await authBridge(authPorts[2], 'gogh-auth-c');
    const [passA, passB] = [await makeUser('alice-c'), await makeUser('bob-c')];
    const {popup} = await adminConsole('auth-c-admin', bridge);
    const [a, b] = await twoPlayers(['auth-c-alice', 'auth-c-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-c', passA);
    await signIn(b, 'bob-c', passB);
    await Promise.all([signedInAs(a, 'alice-c', 'user'), signedInAs(b, 'bob-c', 'user')]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await Promise.all([instrument(a), instrument(b)]);
    await kick(popup, 'alice-c');
    await waitError(a, /You were removed from the match by an admin/);
    await waitPhase(a, 'connected');
    assert.equal(await a.page.evaluate(() => window.__e2e.session.state), 'closed');
    // It stays down: no reconnect, and she is still signed in.
    const before = a.requests.filter(url => /\/(pair|renew)$/.test(url)).length;
    await sleep(3000);
    assert.equal(a.requests.filter(url => /\/(pair|renew)$/.test(url)).length, before, 'the kicked player reconnected by itself');
    assert.equal(await a.page.evaluate(() => window.__e2e.session.state), 'closed');
    assert.equal(await a.page.locator('#signedin-as').textContent(), 'Signed in as alice-c (user)');
    await until(async () => !(await sessionInfo(b)).peers.includes('alice-c'), 'bob to see alice leave');
    // She can rejoin: Find match pairs her with bob again.
    await findMatch(a);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await instrument(a);
    await until(async () => (await sessionInfo(b)).peers.includes('alice-c'), 'bob to see alice return');
    assert.equal(await a.page.locator('#online-error').isHidden(), true, 'the removal notice stayed after rejoining');
    const via = [await exchange(a, b, 41), await exchange(b, a, 42)];
    assert.deepEqual(via, ['direct', 'direct']);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `alice-c removed (no reconnect, still signed in), then matched with bob-c again; frames ${via.join('/')}`;
  });

  await scenario('19 the admin disables one player: thrown out with the disabled message, cannot sign in, can after re-enabling', async () => {
    const bridge = await authBridge(authPorts[3], 'gogh-auth-d');
    const [passA, passB] = [await makeUser('alice-d'), await makeUser('bob-d')];
    const {popup} = await adminConsole('auth-d-admin', bridge);
    const [a, b] = await twoPlayers(['auth-d-alice', 'auth-d-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-d', passA);
    await signIn(b, 'bob-d', passB);
    await Promise.all([signedInAs(a, 'alice-d', 'user'), signedInAs(b, 'bob-d', 'user')]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await Promise.all([instrument(a), instrument(b)]);
    await accountRow(popup, 'alice-d').getByRole('button', {name: 'Disable'}).click();
    await popup.waitForFunction(() => /Account updated/.test(document.querySelector('#message')?.textContent || ''));
    await waitError(a, /Your account was disabled by an admin/);
    await a.page.waitForSelector('#signin-form:not([hidden])', {timeout: 20_000});
    assert.equal(await a.page.evaluate(() => window.__e2e.session.state), 'closed');
    assert.equal(await a.page.locator('#online-find').isDisabled(), true, 'a disabled player can search');
    assert.match(await a.page.locator('#online-error').textContent(), /disabled by an admin/, 'the notice was replaced');
    await until(async () => !(await sessionInfo(b)).peers.includes('alice-d'), 'bob to see alice leave');
    // She cannot sign in, and the page says so without saying why.
    await signIn(a, 'alice-d', passA);
    await waitError(a, /^Wrong username or password\.$/);
    assert.equal(await a.page.locator('#signedin').isVisible(), false);
    // Re-enabled, she signs in and plays with bob again.
    await accountRow(popup, 'alice-d').getByRole('button', {name: 'Enable'}).click();
    await popup.waitForFunction(() => /Account updated/.test(document.querySelector('#message')?.textContent || ''));
    await signIn(a, 'alice-d', passA);
    await signedInAs(a, 'alice-d', 'user');
    await findMatch(a);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await instrument(a);
    const via = [await exchange(a, b, 51), await exchange(b, a, 52)];
    assert.deepEqual(via, ['direct', 'direct']);
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `alice-d thrown out with the disabled message and signed out, refused at sign-in, back after enabling; frames ${via.join('/')}`;
  });

  await scenario('20 a password changed in one tab signs the other tab out and closes its live connection', async () => {
    const bridge = await authBridge(authPorts[4], 'gogh-auth-e');
    const [passA, passB] = [await makeUser('alice-e'), await makeUser('bob-e')];
    const [a, b] = await twoPlayers(['auth-e-alice', 'auth-e-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-e', passA);
    await signIn(b, 'bob-e', passB);
    await Promise.all([signedInAs(a, 'alice-e', 'user'), signedInAs(b, 'bob-e', 'user')]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await Promise.all([instrument(a), instrument(b)]);
    // A second tab of the same browser: it shares the sign-in.
    const tab = await a.context.newPage();
    tab.setDefaultTimeout(90_000);
    const tabErrors = [];
    tab.on('pageerror', error => tabErrors.push(error.message));
    await tab.goto(`${bridge.base}/?test`, {waitUntil: 'domcontentloaded'});
    await tab.waitForSelector('#deploy-button:not(:disabled)');
    await tab.locator('#online-connect').click();
    await tab.waitForFunction(() => /^Signed in as alice-e \(user\)$/.test(document.querySelector('#signedin-as')?.textContent || ''), null, {timeout: 30_000});
    // A wrong current password is refused and changes nothing.
    const fresh = password();
    await tab.locator('#password-change summary').click();
    await enter(tab.locator('#password-current'), password());
    await enter(tab.locator('#password-next'), fresh);
    await tab.locator('#password-submit').click();
    await tab.waitForFunction(() => /current password is wrong/.test(document.querySelector('#online-error')?.textContent || ''), null, {timeout: 20_000});
    assert.equal(await a.page.evaluate(() => window.__e2e.session.state), 'open', 'a refused change closed the match connection');
    await enter(tab.locator('#password-current'), passA);
    await tab.locator('#password-submit').click();
    await tab.waitForSelector('#signin-form:not([hidden])', {timeout: 20_000});
    assert.match(await tab.locator('#account-note').textContent(), /Password changed\. Sign in again/);
    // The first tab is signed out and its live connection is closed, with a reason in plain words.
    await a.page.waitForSelector('#signin-form:not([hidden])', {timeout: 30_000});
    await until(async () => (await a.page.evaluate(() => window.__e2e.session.state)) !== 'open', 'the first tab\'s live connection to close', 20_000);
    await waitError(a, /password or role was changed, so you were signed out/);
    await waitPhase(a, 'connected');
    assert.equal(await a.page.evaluate(async () => (await fetch('/api/liveengine/me')).status), 401, 'the old session still works');
    await until(async () => !(await sessionInfo(b)).peers.includes('alice-e'), 'bob to see alice leave');
    // The old password no longer signs in; the new one does, and she matches with bob again.
    await signIn(a, 'alice-e', passA);
    await waitError(a, /^Wrong username or password\.$/);
    await signIn(a, 'alice-e', fresh);
    await signedInAs(a, 'alice-e', 'user');
    // Her seat went with her session; she searches afresh and joins bob's room again.
    await findMatch(a);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    await instrument(a);
    const via = [await exchange(a, b, 71), await exchange(b, a, 72)];
    assert.deepEqual(via, ['direct', 'direct']);
    assert.equal(tabErrors.length + a.errors.length + b.errors.length, 0, `page errors: ${[...tabErrors, ...a.errors, ...b.errors].join('; ')}`);
    return 'wrong current password refused; after the change the other tab was signed out and closed, the old password was refused and the new one signed in and rejoined bob';
  });

  await scenario('21 the admin resets a password while the player is mid-match: the connection closes and the old session is dead', async () => {
    const bridge = await authBridge(authPorts[5], 'gogh-auth-f');
    const [passA, passB] = [await makeUser('alice-f'), await makeUser('bob-f')];
    const {popup} = await adminConsole('auth-f-admin', bridge);
    const [a, b] = await twoPlayers(['auth-f-alice', 'auth-f-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-f', passA);
    await signIn(b, 'bob-f', passB);
    await Promise.all([signedInAs(a, 'alice-f', 'user'), signedInAs(b, 'bob-f', 'user')]);
    await findMatch(a);
    await waitPhase(a, 'searching');
    await findMatch(b);
    await Promise.all([reachDirect(a), reachDirect(b)]);
    const ids = await playBoth(a, b);
    await seeEachOther(a, b, ids);
    const reset = password();
    const row = accountRow(popup, 'alice-f');
    await enter(row.getByLabel('New password for alice-f'), reset);
    await row.getByRole('button', {name: 'Reset password'}).click();
    await popup.waitForFunction(() => /Password reset/.test(document.querySelector('#message')?.textContent || ''));
    await until(async () => (await a.page.evaluate(() => window.__e2e.session.state)) !== 'open', 'alice\'s live connection to close', 20_000);
    await waitError(a, /password or role was changed, so you were signed out/, 30_000);
    await until(async () => (await a.page.evaluate(async () => (await fetch('/api/liveengine/me')).status)) === 401, 'the old session to stop working', 20_000);
    await until(async () => !(await sessionInfo(b)).peers.includes('alice-f'), 'bob to see alice leave');
    const {admin} = await authSetup();
    await assert.rejects(admin.login('alice-f', passA), /invalid username or password/, 'the old password still signs in');
    assert.equal((await admin.login('alice-f', reset)).account.username, 'alice-f');
    assert.equal(await b.page.evaluate(() => window.__e2e.session.state), 'open', 'bob was disturbed');
    return 'alice-f lost her connection and her session when her password was reset in the console; old password refused, new one works; bob stayed connected';
  });

  await scenario('22 an external hub with password auth through the central: accounts live on the remote service and sign-in works end to end', async () => {
    const hubId = 'gogh-ext-pw';
    const net = await import(pathToFileURL(path.join(gameRoot, 'src/liveengine-net.js')).href);
    // The remote service owns the hub, its auth and its accounts; the central only lists it.
    const known = new Set(((await adminR.listSchemas())?.schemas || []).map(entry => entry.ref));
    for (const schema of net.LIVE_SCHEMAS) if (!known.has(`${schema.id}@${schema.version}`)) await adminR.createSchema(schema);
    await adminR.register({id: hubId, mode: 'internal', scopes: {game: 'gogh-strike'}, channels: net.LIVE_CHANNELS, auth: {kind: 'password'}});
    const [passA, passB] = [await makeUser('alice-g', 'user', adminR), await makeUser('bob-g', 'user', adminR)];
    const bridge = await startBridge(portExternal, central.address, centralToken, {LIVEENGINE_HUB_ID: hubId, LIVEENGINE_HUB_MODE: 'external',
      LIVEENGINE_REMOTE_ADDRESS: remote.address, LIVEENGINE_REMOTE_TOKEN: remoteToken, GOGH_HUB_AUTH_PASSWORD: '1'});
    await bridge.warm();
    assert.equal((await adminC.list()).hubs.find(hub => hub.id === hubId)?.mode, 'external', 'the central does not list the hub as external');
    await assert.rejects(adminC.login('alice-g', passA), /invalid username or password/, 'the central knows the remote service\'s account');
    assert.equal((await bridge.post('pair', {userId: 'alice-g'})).status, 403, 'an unsigned player paired');
    const [a, b] = await twoPlayers(['auth-g-alice', 'auth-g-bob'], bridge.base, [{online: true}, {online: true}]);
    await Promise.all([connectPanel(a, bridge.base), connectPanel(b, bridge.base)]);
    await signIn(a, 'alice-g', 'wrong-password-' + password());
    await waitError(a, /^Wrong username or password\.$/);
    await signIn(a, 'alice-g', passA);
    await signIn(b, 'bob-g', passB);
    await Promise.all([signedInAs(a, 'alice-g', 'user'), signedInAs(b, 'bob-g', 'user')]);
    await findMatch(a);
    await findMatch(b);
    await Promise.all([waitPhase(a, 'matched'), waitPhase(b, 'matched')]);
    await Promise.all([instrument(a), instrument(b)]);
    const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
    assert.deepEqual([ia.id, ib.id], ['alice-g', 'bob-g']);
    assert.deepEqual(ia.identity, {identity: 'alice-g', claims: {role: 'user'}});
    assert.equal(ia.room, ib.room);
    await until(async () => (await sessionInfo(a)).peers.includes('bob-g') && (await sessionInfo(b)).peers.includes('alice-g'), 'both players to see each other');
    await a.page.waitForFunction(() => /bob-g \(user\)/.test(document.querySelector('#online-match')?.textContent || ''), null, {timeout: 20_000});
    const url = new URL(await a.page.evaluate(() => window.__e2e.session._descriptor.url));
    assert.equal(`${url.protocol}//${url.host}`, remote.address.replace(/^http/, 'ws'), 'the player did not connect to the remote service');
    const via = [await exchange(a, b, 61), await exchange(b, a, 62)];
    assert.equal(a.errors.length + b.errors.length, 0, `page errors: ${[...a.errors, ...b.errors].join('; ')}`);
    return `accounts on the remote service only; alice-g and bob-g signed in, matched on ${url.host}, identities verified, frames ${via.join('/')}`;
  });

  /** `crowd` signed-in users at once, matched into rooms of five, all exchanging data under their own names. */
  const crowdScenario = (number, crowd, portIndex, tag) => scenario(`${number} ${crowd} users signed in at once, matched into rooms, every one exchanging data and shown under their own name`, async () => {
    const hubId = `gogh-auth-${tag}`;
    const bridge = await authBridge(authPorts[portIndex], hubId, {GOGH_MATCH_CAPACITY: '5'});
    const names = Array.from({length: crowd}, (_, index) => `crowd-${tag}-${index}`);
    const secrets = new Map();
    for (const name of names) secrets.set(name, await makeUser(name));
    const users = await Promise.all(names.map(name => openPlayer(`auth-${name}`, bridge.base, {online: true})));
    await Promise.all(users.map(user => connectPanel(user, bridge.base)));
    await Promise.all(users.map((user, index) => signIn(user, names[index], secrets.get(names[index]))));
    await Promise.all(users.map((user, index) => signedInAs(user, names[index], 'user')));
    await Promise.all(users.map(findMatch));
    await Promise.all(users.map(reachDirect));
    await Promise.all(users.map(instrument));
    const infos = await Promise.all(users.map(sessionInfo));
    infos.forEach((info, index) => assert.equal(info.id, names[index], `${users[index].label} plays as ${info.id}`));
    const rooms = new Map();
    infos.forEach((info, index) => rooms.set(info.room, [...(rooms.get(info.room) || []), index]));
    assert.equal([...rooms.values()].flat().length, crowd, 'a player is in no room or in two');
    for (const [room, members] of rooms) {
      assert.ok(members.length >= 2, `room ${room} holds a lone player`);
      const seated = (await (await authSetup()).admin.call(hubId, 'getServer', [room])).members;
      assert.deepEqual([...seated].sort(), members.map(index => names[index]).sort(), `the hub's members of ${room} differ from the pages'`);
    }
    const via = [];
    let n = 200;
    for (const [index, info] of infos.entries()) {
      const mates = rooms.get(info.room).filter(other => other !== index).map(other => names[other]).sort();
      assert.deepEqual([...info.peers].sort(), mates, `${users[index].label} does not see exactly its roommates`);
      // Every roommate is shown under its own verified name, in the panel and in the session.
      const text = await matchText(users[index]);
      for (const mate of mates) {
        assert.ok(text.includes(`${mate} (user)`), `${users[index].label} does not show ${mate}: ${text}`);
        assert.equal(await users[index].page.evaluate(id => window.__e2e.session.identityOf(id)?.identity, mate), mate);
      }
      assert.equal(await users[index].page.locator('#signedin-as').textContent(), `Signed in as ${names[index]} (user)`);
    }
    for (const members of rooms.values()) for (const from of members) for (const to of members) if (from !== to) via.push(await exchange(users[from], users[to], n++));
    assert.ok(via.every(kind => kind === 'direct'), `frames did not travel over the data channel: ${via}`);
    const ping = await apiPing(bridge);
    assert.deepEqual([ping.waiting, ping.seats], [0, crowd]);
    assert.equal(users.reduce((sum, user) => sum + user.errors.length, 0), 0, 'page errors');
    return `rooms ${[...rooms.values()].map(members => members.length).join('+')}, ${via.length} pairwise frames ${[...new Set(via)].join('/')}, ${crowd} names shown`;
  });
  await crowdScenario(23, 10, 6, 'h');

  // ---- 24 and 25: WebRTC on a plain page (no game): through a real TURN server, and without a GPU ----

  /** Two peers on a hub, paired the way an app server would. Returns both descriptors. */
  async function pairPlain(admin, ids) {
    const group = `plain-${secret().slice(0, 8)}`;
    for (const id of ids) await admin.call('plain', 'upsertUser', [{id, scopes: {group}}]);
    const request = id => ({subject: {id, scopes: {group}}, target: {kind: 'peer', requirements: {group}, capacity: 2}, waitMs: 15_000});
    return Promise.all(ids.map(id => admin.pair(request(id))));
  }
  /** A hub for plain-page tests: one internal hub called "plain", the plain page's origin allowed. */
  async function plainHub(origin, env = {}) {
    const token = secret();
    const hub = await startHub({LIVEENGINE_TOKEN: token, LIVEENGINE_LIVE_ALLOWED_ORIGINS: origin, LIVEENGINE_RATE_LIMIT_REQUESTS: '100000', ...env});
    onClose(() => hub.stop());
    const admin = new HubClient(hub.address, {token});
    await admin.register({id: 'plain', mode: 'internal', scopes: {tier: 'open'}});
    return {hub, admin};
  }
  const directBoth = async (pages, ids, why) => until(() => Promise.all(pages.map((page, i) => page.evaluate(peer => window.__client.transportOf(peer), ids[1 - i]))).then(kinds => kinds.every(kind => kind === 'direct')),
    'a direct WebRTC transport on both plain pages', 30_000).catch(async error => {
    const state = await Promise.all(pages.map((page, i) => page.evaluate(peer => { const l = window.__client.links.get(peer); return {ice: l?.pc?.iceConnectionState, ch: l?.channel?.readyState, failure: l?.failure, events: window.__events}; }, ids[1 - i]).catch(e => e.message)));
    throw new Error(`${error.message}${why ? ` (${why})` : ''}; ${JSON.stringify(state)}`);
  });
  /** Gather candidates with `server` as the only ICE server, relay only. Resolves with the candidates and any gathering errors. */
  const gatherRelay = (page, server) => page.evaluate(async iceServer => {
    const pc = new RTCPeerConnection({iceServers: [iceServer], iceTransportPolicy: 'relay'});
    const candidates = [];
    const errors = [];
    pc.onicecandidate = event => { if (event.candidate) candidates.push(event.candidate.candidate); };
    pc.onicecandidateerror = event => errors.push({code: event.errorCode, text: event.errorText});
    pc.createDataChannel('probe');
    await pc.setLocalDescription(await pc.createOffer());
    await new Promise(resolve => { pc.onicegatheringstatechange = () => { if (pc.iceGatheringState === 'complete') resolve(); }; setTimeout(resolve, 10_000); });
    pc.close();
    return {candidates: candidates.filter(c => / typ relay /.test(c)).length, errors};
  }, server);
  /** A TURN REST credential as the hub documents it: username `<expiry>:<user>:<n>`, credential base64 HMAC-SHA1 of it. */
  const turnCredential = (secretWord, expiry, user = 'expiry-probe') => {
    const username = `${expiry}:${user}:${Math.floor(Math.random() * 1e9)}`;
    return {username, credential: createHmac('sha1', secretWord).update(username).digest('base64')};
  };

  await scenario('24 TURN: two browsers forced to relay-only reach each other through a real coturn, and its credentials expire', async () => {
    const turnSecret = secret();
    secrets.add(turnSecret);
    const coturn = await startCoturn({secret: turnSecret}); // fails, never skips, if the image cannot be pulled or run
    onClose(() => coturn.stop());
    const plain = await startPlainPage();
    onClose(() => plain.stop());
    const {hub, admin} = await plainHub(plain.origin, {LIVEENGINE_ICE_SERVERS: JSON.stringify([{urls: coturn.url}]), LIVEENGINE_TURN_SECRET: turnSecret});
    const ids = ['turn-a', 'turn-b'];
    const contexts = await Promise.all(ids.map(() => browser.newContext()));
    try {
      const pages = await Promise.all(contexts.map(context => context.newPage()));
      for (const page of pages) page.on('pageerror', error => pageErrors.push(`turn: ${error.message}`));
      const descriptors = await pairPlain(admin, ids);
      await Promise.all(pages.map((page, i) => connectPlainClient(page, plain.origin, descriptors[i], {iceTransportPolicy: 'relay', direct: true, directTimeoutMs: 25_000})));

      // The hub hands each client its own short-lived TURN credential: expiry an hour ahead, HMAC-SHA1 of the username.
      const issued = await Promise.all(pages.map(page => page.evaluate(() => window.__client.iceServers)));
      const now = Math.floor(Date.now() / 1000);
      issued.forEach((servers, i) => {
        assert.equal(servers.length, 1, 'the welcome frame should carry exactly the configured ICE server');
        const [server] = servers;
        assert.equal(server.urls, coturn.url);
        const [expiry, user, nonce] = server.username.split(':');
        assert.equal(user, ids[i], 'the username names the user');
        assert.ok(/^\d+$/.test(nonce), 'the username ends with a random number');
        assert.ok(Math.abs(Number(expiry) - (now + 3600)) < 60, `the credential should expire an hour from now, not at ${expiry} (now ${now})`);
        assert.equal(server.credential, createHmac('sha1', turnSecret).update(server.username).digest('base64'), 'the credential is not the HMAC-SHA1 of the username');
      });
      assert.notEqual(issued[0][0].username, issued[1][0].username, 'two connections shared one credential');

      await directBoth(pages, ids, 'relay-only, so the path must be coturn');
      const pairs = await Promise.all(pages.map((page, i) => selectedPair(page, ids[1 - i])));
      const [low, high] = coturn.relayPorts;
      pairs.forEach((pair, i) => {
        assert.equal(pair.local?.type, 'relay', `${ids[i]} selected a ${pair.local?.type} local candidate: ${JSON.stringify(pair)}`);
        assert.equal(pair.remote?.type, 'relay', `${ids[i]} selected a ${pair.remote?.type} remote candidate: ${JSON.stringify(pair)}`);
        assert.equal(pair.local.relayProtocol, 'udp');
        assert.equal(pair.local.address, '127.0.0.1');
        assert.ok(pair.local.port >= low && pair.local.port <= high, `${ids[i]}'s relay address ${pair.local.address}:${pair.local.port} is not in coturn's relay range ${low}-${high}`);
        assert.ok(pair.local.url?.includes(`127.0.0.1:${coturn.port}`), `${ids[i]}'s relay candidate does not name coturn: ${pair.local.url}`);
        assert.ok(pair.gathered.length > 0 && pair.gathered.every(c => c.type === 'relay'), `${ids[i]} gathered a non-relay candidate: ${JSON.stringify(pair.gathered)}`);
      });

      // Frames flow in both directions, over the data channel (so over coturn), not the hub relay.
      const via = [];
      for (let n = 1; n <= 5; n++) {
        via.push(await sendPlain(pages[0], pages[1], ids[0], ids[1], {n}, until));
        via.push(await sendPlain(pages[1], pages[0], ids[1], ids[0], {n: 100 + n}, until));
      }
      assert.ok(via.every(kind => kind === 'direct'), `frames did not use the data channel: ${via}`);
      const after = await Promise.all(pages.map((page, i) => selectedPair(page, ids[1 - i])));
      after.forEach((pair, i) => assert.ok(pair.bytesSent > 0 && pair.bytesReceived > 0, `${ids[i]}'s selected pair carried no bytes: ${JSON.stringify(pair)}`));

      // The TURN server saw the allocations of both browsers.
      const logs = await coturn.logs();
      for (const id of ids) assert.ok(new RegExp(`user <\\d+:${id}:\\d+>: incoming packet ALLOCATE processed, success`).test(logs), `the coturn log shows no successful allocation for ${id}\n${logs.split('\n').slice(-10).join('\n')}`);

      // Expiry: coturn accepts a fresh credential and refuses an expired one, a short-lived one once it lapses, and a wrong secret.
      const at = Math.floor(Date.now() / 1000);
      const server = credential => ({urls: coturn.url, ...credential});
      const fresh = await gatherRelay(pages[0], issued[0][0]);
      assert.ok(fresh.candidates > 0, `the hub-issued credential gets no relay candidate: ${JSON.stringify(fresh)}`);
      const expired = await gatherRelay(pages[0], server(turnCredential(turnSecret, at - 120)));
      assert.equal(expired.candidates, 0, 'coturn allocated with an expired credential');
      assert.ok(expired.errors.some(error => error.code === 401), `coturn did not answer an expired credential with 401: ${JSON.stringify(expired.errors)}`);
      const forged = await gatherRelay(pages[0], server({...turnCredential(turnSecret, at + 3600), credential: createHmac('sha1', secret()).update('x').digest('base64')}));
      assert.equal(forged.candidates, 0, 'coturn allocated with a credential signed by another secret');
      const expiry = Math.floor(Date.now() / 1000) + 5;
      const shortLived = server(turnCredential(turnSecret, expiry));
      const early = await gatherRelay(pages[0], shortLived);
      assert.ok(early.candidates > 0, `a credential five seconds from expiry was refused: ${JSON.stringify(early)}`);
      await until(() => Math.floor(Date.now() / 1000) > expiry + 1, 'the short-lived credential to lapse', 15_000);
      const late = await gatherRelay(pages[0], shortLived);
      assert.equal(late.candidates, 0, 'coturn allocated with the same credential after its expiry');
      assert.ok(late.errors.some(error => error.code === 401), `the lapsed credential was not refused with 401: ${JSON.stringify(late.errors)}`);

      // Control: relay-only with no TURN server cannot connect directly. The policy is applied, and the hub relay still carries frames.
      const bare = await plainHub(plain.origin, {LIVEENGINE_ICE_SERVERS: '[]'});
      const controlIds = ['turn-c', 'turn-d'];
      const controlContexts = await Promise.all(controlIds.map(() => browser.newContext()));
      try {
        const controlPages = await Promise.all(controlContexts.map(context => context.newPage()));
        const controlDescriptors = await pairPlain(bare.admin, controlIds);
        await Promise.all(controlPages.map((page, i) => connectPlainClient(page, plain.origin, controlDescriptors[i], {iceTransportPolicy: 'relay', direct: true, directTimeoutMs: 4000})));
        await until(() => controlPages[0].evaluate(() => window.__events.some(event => event.directFailed)), 'the relay-only link without TURN to give up', 15_000);
        assert.equal(await controlPages[0].evaluate(peer => window.__client.transportOf(peer), controlIds[1]), 'relay', 'a relay-only link with no TURN server went direct');
        assert.equal(await sendPlain(controlPages[0], controlPages[1], controlIds[0], controlIds[1], {n: 900}, until), 'relay');
      } finally { await Promise.all(controlContexts.map(context => context.close().catch(() => {}))); }

      assert.equal(pageErrors.filter(error => error.startsWith('turn:')).length, 0, 'page errors');
      const types = pairs.map(pair => `${pair.local.type}/${pair.remote.type}`).join(' and ');
      return `selected pair ${types} over ${pairs[0].local.relayProtocol} via ${coturn.url} (relay ${pairs[0].local.address}:${pairs[0].local.port} and ${pairs[1].local.address}:${pairs[1].local.port}), every gathered candidate relay, ${via.length} frames ${[...new Set(via)].join('/')}; credential: expires +3600 s, hub-issued accepted, expired (401), forged, and lapsed after 5 s (401) refused; control without TURN stayed on the hub relay`;
    } finally {
      await Promise.all(contexts.map(context => context.close().catch(() => {})));
    }
  });

  // Browser processes that hold a GPU device open (a record: the GPU process opens the render node to read device info even with the GPU disabled).
  function gpuHolders(markerArg) {
    const parents = new Map();
    const commands = new Map();
    for (const entry of readdirSync('/proc')) {
      if (!/^\d+$/.test(entry)) continue;
      try {
        const stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
        parents.set(Number(entry), Number(stat.slice(stat.lastIndexOf(')') + 2).split(' ')[1]));
        commands.set(Number(entry), readFileSync(`/proc/${entry}/cmdline`, 'utf8'));
      } catch { /* the process ended */ }
    }
    const root = [...commands].find(([, command]) => command.includes(markerArg))?.[0];
    if (!root) return {processes: 0, holders: []};
    const tree = new Set([root]);
    for (let grew = true; grew;) { grew = false; for (const [pid, parent] of parents) if (tree.has(parent) && !tree.has(pid)) { tree.add(pid); grew = true; } }
    const holders = [];
    for (const pid of tree) {
      try { for (const fd of readdirSync(`/proc/${pid}/fd`)) { const target = readlinkSync(`/proc/${pid}/fd/${fd}`); if (/^\/dev\/(dri|nvidia|kfd)/.test(target)) holders.push(`${pid} ${/--type=([a-z-]+)/.exec(commands.get(pid))?.[1] || 'browser'}:${target}`); } } catch { /* not ours to read */ }
    }
    return {processes: tree.size, holders};
  }

  // Flags that turn the GPU off and leave Chromium on its software paths (SwiftShader for WebGL, software compositing).
  const NO_GPU_ARGS = ['--disable-gpu', '--disable-gpu-compositing', '--disable-accelerated-2d-canvas', '--disable-gpu-rasterization', '--disable-features=Vulkan', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'];

  await scenario('25 no GPU: two LiveClients on a plain page pair peer to peer and exchange data in a Chromium with the GPU disabled', async () => {
    const marker = `--e2e-no-gpu=${secret().slice(0, 12)}`;
    const soft = await chromium.launch({headless: true, args: [...WEBRTC_ARGS, ...NO_GPU_ARGS, marker], ...(chosen.executablePath ? {executablePath: chosen.executablePath} : {})});
    onClose(() => soft.close());
    const plain = await startPlainPage();
    onClose(() => plain.stop());
    const {admin} = await plainHub(plain.origin, {LIVEENGINE_ICE_SERVERS: '[]'});
    const ids = ['soft-a', 'soft-b'];
    const contexts = await Promise.all(ids.map(() => soft.newContext()));
    try {
      const pages = await Promise.all(contexts.map(context => context.newPage()));
      const descriptors = await pairPlain(admin, ids);
      await Promise.all(pages.map((page, i) => connectPlainClient(page, plain.origin, descriptors[i], {direct: true, directTimeoutMs: 25_000})));
      // The page really is on software rendering: WebGL is absent or SwiftShader, and no browser process has a GPU device open.
      const gl = await pages[0].evaluate(() => {
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('webgl');
        if (!context) return 'no WebGL';
        const info = context.getExtension('WEBGL_debug_renderer_info');
        return info ? context.getParameter(info.UNMASKED_RENDERER_WEBGL) : context.getParameter(context.RENDERER);
      });
      assert.ok(/no WebGL|swiftshader|software|llvmpipe/i.test(gl), `the page is not on software rendering: ${gl}`);
      // Chromium's own report: canvas and compositing are software only.
      const status = await contexts[0].newPage().then(async report => {
        await report.goto('chrome://gpu');
        await sleep(2000);
        const text = await report.evaluate(() => { const out = []; const walk = node => { if (node.nodeType === 3 && node.textContent.trim()) out.push(node.textContent.trim()); if (node.shadowRoot) walk(node.shadowRoot); node.childNodes?.forEach(walk); }; walk(document.body); return out.join('\n'); });
        await report.close();
        return Object.fromEntries(['Canvas', 'Compositing'].map(feature => [feature, new RegExp(`${feature}:\\n([^\\n]*)`).exec(text)?.[1] ?? 'not reported']));
      });
      assert.ok(/^Software only/.test(status.Canvas) && /^Software only/.test(status.Compositing), `Chromium reports ${JSON.stringify(status)}`);
      // The GPU process may still open the render node to read device info; rendering does not use it. Recorded, not asserted.
      const gpu = gpuHolders(marker);
      await directBoth(pages, ids);
      const via = [];
      for (let n = 1; n <= 5; n++) {
        via.push(await sendPlain(pages[0], pages[1], ids[0], ids[1], {n}, until));
        via.push(await sendPlain(pages[1], pages[0], ids[1], ids[0], {n: 100 + n}, until));
      }
      assert.ok(via.every(kind => kind === 'direct'), `frames did not use the data channel: ${via}`);
      const pairs = await Promise.all(pages.map((page, i) => selectedPair(page, ids[1 - i])));
      pairs.forEach((pair, i) => assert.notEqual(pair.local?.type, 'relay', `${ids[i]} went through a relay: ${JSON.stringify(pair)}`));
      const record = await recordSoftwareGame(soft).catch(error => `not recorded: ${error.message.slice(0, 120)}`);
      console.log(`INFO game on software rendering (not a pass or fail) | ${record}`);
      return `renderer "${gl}"; chrome://gpu: canvas "${status.Canvas}", compositing "${status.Compositing}" (${gpu.processes} browser processes${gpu.holders.length ? `, ${gpu.holders.join(' ')} opened a DRM node for device info only` : ', no DRM node open'}); selected pair ${pairs.map(pair => `${pair.local.type}/${pair.remote.type}`).join(' and ')}, ${via.length} frames ${[...new Set(via)].join('/')}`;
    } finally {
      await Promise.all(contexts.map(context => context.close().catch(() => {})));
      await soft.close().catch(() => {});
    }
  });

  /** What the full game does on software rendering. Never throws on a bad result: it describes it. */
  async function recordSoftwareGame(soft) {
    const bridge = await startBridge(portSoft, central.address, centralToken, {LIVEENGINE_HUB_ID: 'gogh-soft', GOGH_LIVE_LINK: 'peer'});
    await bridge.warm();
    const started = Date.now();
    const seconds = () => `${((Date.now() - started) / 1000).toFixed(0)}s`;
    const notes = [];
    const run = async () => {
      const [a, b] = await Promise.all(['soft-game-a', 'soft-game-b'].map(label => openPlayer(label, bridge.base, {browser: soft})));
      notes.push(`both lobbies loaded in ${seconds()}`);
      await a.deploy();
      await sleep(300);
      await b.deploy();
      await Promise.all([a.playing(), b.playing()]);
      notes.push(`both reached play in ${seconds()}`);
      await Promise.all([instrument(a), instrument(b)]);
      await sleep(5000);
      const fps = await Promise.all([a, b].map(player => player.page.evaluate(() => window.vanGoghStrike?.state?.fps)));
      const [ia, ib] = await Promise.all([sessionInfo(a), sessionInfo(b)]);
      notes.push(`fps reported ${fps.map(value => (typeof value === 'number' ? value.toFixed(1) : value)).join('/')}`);
      notes.push(`transport ${ia.transport[ib.id] ?? 'none'}/${ib.transport[ia.id] ?? 'none'}`);
      const [countsA, countsB] = await Promise.all([counts(a), counts(b)]);
      notes.push(`frames heard a=${JSON.stringify(countsA)} b=${JSON.stringify(countsB)}`);
    };
    const outcome = await Promise.race([run().then(() => 'ok', error => `stopped: ${error.message.split('\n')[0].slice(0, 100)}`), sleep(200_000).then(() => 'still not playing after 200s')]);
    const where = outcome === 'ok' ? '' : `; pages then: ${(await diagnose().catch(() => '')).replace(/ console=.*?(?= \|\||$)/g, '').slice(0, 500)}`;
    return `${outcome}; ${notes.join('; ') || 'nothing reached'}${where}`;
  }

  await crowdScenario(26, 20, 7, 'j');

}

for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => closeAll().finally(() => process.exit(130)));
try {
  await main();
} catch (error) {
  console.log(`FAIL setup | ${mask(String(error?.stack || error).replaceAll(centralToken, '[token]').replaceAll(remoteToken, '[token]').replaceAll(authToken, '[token]'))}`);
  results.push({name: 'setup', ok: false});
} finally {
  await closeAll();
}
// A scenario that never ran (its setup broke) is a failure, not a skip.
for (let number = 1; number <= 26; number++) {
  if (results.some(result => result.name.startsWith(`${number} `))) continue;
  results.push({name: `${number}`, ok: false});
  console.log(`FAIL ${number} | did not run: setup failed`);
}
const failed = results.filter(result => !result.ok);
console.log(`${results.length - failed.length} passed, ${failed.length} failed of ${results.length} scenarios; page errors: ${pageErrors.length}`);
if (failed.length) process.exitCode = 1;
