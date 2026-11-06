// npm run dev:gogh [-- --lan] [-- --no-build]
//
// Builds the debug hub from this checkout, starts it and the Node API (lib/hub-api.mjs through
// integrations/gogh-strike/bridge.mjs) beside the Gogh Strike game, and prints the URLs. Nothing
// here uses the published package. Keys are generated into .dev/keys.json (gitignored). Ctrl-C
// stops both processes. Loopback by default; --lan binds the machine's LAN address instead
// (never a Tailscale one) so another device on the network can open the game.
import {spawn, spawnSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {existsSync, mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {ensureGame, lanAddress} from '../integrations/gogh-strike/setup.mjs';
import {freePort} from '../test/support/hub-process.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = new Set(process.argv.slice(2));
const suffix = process.platform === 'win32' ? '.exe' : '';
const hubBinary = path.join(root, 'target', 'debug', `liveengine-hub${suffix}`);

if (!args.has('--no-build')) {
  console.log('Building the debug hub (cargo build --locked --bin liveengine-hub)…');
  const built = spawnSync('cargo', ['build', '--locked', '--bin', 'liveengine-hub'], {cwd: root, stdio: 'inherit'});
  if (built.error?.code === 'ENOENT') { console.error('cargo is not installed. Install Rust from https://rustup.rs and run this again.'); process.exit(1); }
  if (built.status !== 0) process.exit(built.status || 1);
}
if (!existsSync(hubBinary)) { console.error(`${hubBinary} does not exist; run without --no-build.`); process.exit(1); }

let game;
try { game = ensureGame(); }
catch (error) { console.error(error.message); process.exit(1); }

// Local keys, made once. They protect a hub that only this machine (or this LAN) reaches.
const keyFile = path.join(root, '.dev', 'keys.json');
mkdirSync(path.dirname(keyFile), {recursive: true});
if (!existsSync(keyFile)) writeFileSync(keyFile, JSON.stringify({adminToken: randomBytes(24).toString('hex')}, null, 2), {mode: 0o600});
const {adminToken} = JSON.parse(readFileSync(keyFile, 'utf8'));

let host = '127.0.0.1';
try { if (args.has('--lan')) host = lanAddress(); }
catch (error) { console.error(error.message); process.exit(1); }
const pick = async name => (Number(process.env[name]) || await freePort());
const hubPort = await pick('DEV_GOGH_HUB_PORT');
const pagePort = await pick('DEV_GOGH_PORT');
const origins = [`http://${host}:${pagePort}`, `http://127.0.0.1:${pagePort}`, `http://localhost:${pagePort}`].join(',');

const children = [];
const stop = code => {
  for (const child of children) child.kill('SIGTERM');
  setTimeout(() => process.exit(code), 500).unref();
};
const run = (name, command, commandArgs, env) => {
  const child = spawn(command, commandArgs, {cwd: root, env: {...process.env, ...env}, stdio: ['ignore', 'inherit', 'inherit']});
  children.push(child);
  child.on('exit', (code, signal) => { if (!signal) { console.error(`${name} exited (${code}); stopping.`); stop(code || 1); } });
};

run('hub', hubBinary, ['serve'], {LIVEENGINE_TOKEN: adminToken, LIVEENGINE_BIND: `${host}:${hubPort}`, LIVEENGINE_PUBLIC_URL: `ws://${host}:${hubPort}/v1/live`,
  LIVEENGINE_LIVE_ALLOWED_ORIGINS: origins, LIVEENGINE_ICE_SERVERS: '[]', LIVEENGINE_RATE_LIMIT_REQUESTS: '100000'});
for (let i = 0; i < 100; i++) {
  try { if ((await fetch(`http://${host}:${hubPort}/health`)).ok) break; } catch { /* not listening yet */ }
  await new Promise(resolve => setTimeout(resolve, 100));
}
run('bridge', process.execPath, [path.join(root, 'integrations/gogh-strike/bridge.mjs')], {PORT: String(pagePort), HOST: host, GOGH_STRIKE_ROOT: game,
  LIVEENGINE_URL: `http://${host}:${hubPort}`, LIVEENGINE_TOKEN: adminToken, LIVEENGINE_HUB_ID: 'gogh-dev', GOGH_LIVE_LINK: 'peer', GOGH_REQUEST_LIMIT: '1000'});

console.log(`
Gogh Strike on this checkout of LiveEngine
  Game and hub API:  http://${host}:${pagePort}/        (hub API address for "Play online": http://${host}:${pagePort})
  Hub (debug build): http://${host}:${hubPort}/
  Game files:        ${game}
  Keys:              ${keyFile}
Open the game in two browser tabs, press Connect and then Find match in each. Ctrl-C stops everything.`);
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => stop(0));
