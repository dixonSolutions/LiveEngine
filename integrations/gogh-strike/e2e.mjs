import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import fs from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {createRequire} from 'node:module';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../../../gogh-strike');
const bridgeScript = path.resolve(here, 'bridge.mjs');
const require = createRequire(import.meta.url);
const {HubClient} = require('../../lib/hubs.js');
const suffix = `${Date.now().toString(36)}-${process.pid}`;
const readSecret = async name => {
  const file = process.env[`${name}_FILE`];
  const value = file ? await fs.readFile(file, 'utf8') : process.env[name];
  assert.ok(value?.trim(), `Set ${name} or ${name}_FILE`);
  return value.trim();
};
const port = Number(process.env.GOGH_BRIDGE_PORT || await new Promise(resolve => {
  const server = net.createServer();
  server.listen(0, '127.0.0.1', () => { const selected = server.address().port; server.close(() => resolve(selected)); });
}));
const apiUrl = process.env.LIVEENGINE_URL || 'http://127.0.0.1:8787';
const centralToken = await readSecret('LIVEENGINE_TOKEN');
// The screenshot goes under TMPDIR unless GOGH_SCREENSHOT says otherwise.
const screenshot = process.env.GOGH_SCREENSHOT || path.join(os.tmpdir(), 'liveengine-gogh-lobby.png');
const hubId = process.env.LIVEENGINE_HUB_ID || `gogh-e2e-${suffix}`;
const serverId = process.env.GOGH_MATCH_SERVER_ID || `gogh-e2e-room-${suffix}`;
const mode = process.env.LIVEENGINE_HUB_MODE || 'internal';
const central = new HubClient(apiUrl, {token: centralToken});
let remote, remoteHubId, remoteToken = '', remoteCreated = false, centralHubCreated = false, bridge;
let browser;

async function waitForBridge(base) {
  for (let attempt = 0; attempt < 80; attempt++) {
    try { if ((await fetch(`${base}/health`)).ok) return; } catch {}
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('Local bridge failed to start');
}

try {
  await central.contract();
  const initialHubs = await central.list();
  const listedHubs = Array.isArray(initialHubs) ? initialHubs : initialHubs.hubs || [];
  centralHubCreated = !listedHubs.some(hub => hub.id === hubId);

  let remoteAddress = '';
  if (mode === 'external') {
    const remoteClientUrl = process.env.LIVEENGINE_REMOTE_URL || '';
    assert.ok(remoteClientUrl, 'Set LIVEENGINE_REMOTE_URL when LIVEENGINE_HUB_MODE=external');
    remoteAddress = process.env.LIVEENGINE_REMOTE_ADDRESS || remoteClientUrl;
    remoteToken = await readSecret('LIVEENGINE_REMOTE_TOKEN');
    remote = new HubClient(remoteClientUrl, {token: remoteToken});
    await remote.contract();
    const remoteList = await remote.list();
    const remoteHubs = Array.isArray(remoteList) ? remoteList : remoteList.hubs || [];
    remoteHubId = process.env.LIVEENGINE_REMOTE_HUB_ID || `gogh-e2e-remote-${suffix}`;
    if (!remoteHubs.some(hub => hub.id === remoteHubId)) {
      await remote.register({id: remoteHubId, mode: 'internal', scopes: {game: 'gogh-strike'}});
      remoteCreated = true;
    }
  } else {
    remote = central;
    remoteHubId = hubId;
  }

  bridge = spawn(process.execPath, [bridgeScript], {
    cwd: root, stdio: 'ignore',
    env: {
      ...process.env,
      PORT: String(port), HOST: '127.0.0.1', GOGH_STRIKE_ROOT: process.env.GOGH_STRIKE_ROOT || root,
      LIVEENGINE_URL: apiUrl, LIVEENGINE_TOKEN: centralToken, LIVEENGINE_HUB_ID: hubId,
      LIVEENGINE_HUB_MODE: mode,
      ...(mode === 'external' ? {
        LIVEENGINE_REMOTE_ADDRESS: remoteAddress, LIVEENGINE_REMOTE_HUB_ID: remoteHubId,
        LIVEENGINE_REMOTE_TOKEN: remoteToken,
      } : {}),
      GOGH_MATCH_SERVER_ID: serverId,
    },
  });
  const base = `http://127.0.0.1:${port}`;
  await waitForBridge(base);

  const userId = `gogh-api-${suffix}`;
  const joinedResponse = await fetch(`${base}/api/liveengine/join`, {
    method: 'POST', headers: {'Content-Type': 'application/json', Origin: base},
    body: JSON.stringify({userId, team: 0, role: 'vanguard'}),
  });
  const joined = await joinedResponse.json();
  assert.equal(joinedResponse.status, 200, `Matchmaking bridge error: ${joined.error || joined.reason || joinedResponse.status}`);
  assert.equal(joined.ok, true);
  assert.equal(joined.serverId, serverId);
  const gameUser = await remote.call(remoteHubId, 'getUser', [userId]);
  const gameServer = await remote.call(remoteHubId, 'getServer', [serverId]);
  assert.equal(gameUser?.status, 'paired');
  assert.ok(gameServer?.members?.includes(userId));
  console.log(`service join ok: ${mode} hub ${hubId} -> room ${serverId}`);

  if (process.env.PLAYWRIGHT_PATH) {
    const playwrightModule = await import(pathToFileURL(path.resolve(process.env.PLAYWRIGHT_PATH)).href);
    const chromium = playwrightModule.chromium || playwrightModule.default?.chromium;
    assert.ok(chromium, 'PLAYWRIGHT_PATH must resolve to a Playwright module exporting chromium');
    browser = await chromium.launch({headless: true, ...(process.env.CHROMIUM_PATH ? {executablePath: process.env.CHROMIUM_PATH} : {})});
    const page = await browser.newPage();
    // LAN pages run outside a secure context, where crypto.randomUUID is unavailable.
    await page.addInitScript(() => {
      try { Object.defineProperty(Crypto.prototype, 'randomUUID', {value: undefined, configurable: true}); } catch {}
    });
    const pageErrors = [];
    page.on('pageerror', error => pageErrors.push(error.message));
    await page.goto(`${base}/`, {waitUntil: 'domcontentloaded'});
    await page.waitForSelector('#deploy-button:not(:disabled)', {timeout: 90_000});
    await page.locator('#liveengine-matchmaking').check();
    await page.screenshot({path: screenshot, fullPage: true});
    await page.locator('#deploy-button').click();
    await page.waitForFunction(() => window.vanGoghStrike?.state?.phase === 'playing', {timeout: 45_000});
    const browserState = await page.evaluate(() => ({
      phase: window.vanGoghStrike.state.phase,
      toast: document.querySelector('#toast')?.textContent || '',
      userId: sessionStorage.getItem('gogh-strike-liveengine-user'),
    }));
    assert.match(browserState.toast, /Joined .* with \d+ other players?\./);
    assert.equal(pageErrors.length, 0, `Browser errors: ${pageErrors.join('; ')}`);
    const browserUser = await remote.call(remoteHubId, 'getUser', [browserState.userId]);
    assert.equal(browserUser?.status, 'paired');
    console.log(`browser flow ok: phase ${browserState.phase}; screenshot ${screenshot}`);
  } else {
    console.log('browser flow skipped; set PLAYWRIGHT_PATH to enable it');
  }
} catch (error) {
  // Do not print request payloads or credential-bearing environment values.
  const safeMessage = String(error.message || 'unknown error')
    .replaceAll(centralToken, '[redacted token]')
    .replaceAll(remoteToken, '[redacted token]');
  console.error(`Gogh Strike integration failed: ${safeMessage}`);
  process.exitCode = 1;
} finally {
  if (browser) await browser.close().catch(() => {});
  bridge?.kill('SIGTERM');
  if (bridge) await new Promise(resolve => bridge.once('exit', resolve));
  if (centralHubCreated) await central.remove(hubId).catch(() => {});
  if (remoteCreated && remote) await remote.remove(remoteHubId).catch(() => {});
}
