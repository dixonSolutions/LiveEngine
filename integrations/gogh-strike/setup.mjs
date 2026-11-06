// Gets the game the integration is tested with: upstream Gogh Strike at the validated commit, with
// gogh-strike.patch applied, in a gitignored cache directory. It needs git and network access once.
//
//   node integrations/gogh-strike/setup.mjs        prints the directory (and prepares it if needed)
//
// GOGH_STRIKE_ROOT overrides everything: that checkout is used as it is and never touched. The
// clone is read-only: this file never pushes, and the cache is rebuilt from scratch if it is stale.
import {spawnSync} from 'node:child_process';
import {existsSync, mkdirSync, rmSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
export const UPSTREAM = process.env.GOGH_STRIKE_UPSTREAM || 'https://github.com/petergpt/gogh-strike';
export const COMMIT = '2285e640097d9b12bcc42af72d5e8d0b37082f51';
const patch = path.join(here, 'gogh-strike.patch');
export const cacheDir = path.resolve(here, '../../.cache/gogh-strike');

function git(args, options = {}) {
  const result = spawnSync('git', args, {encoding: 'utf8', ...options});
  if (result.error) throw new Error(result.error.code === 'ENOENT' ? 'git is not installed or not on PATH; install git, or set GOGH_STRIKE_ROOT to a patched Gogh Strike checkout' : result.error.message);
  return result;
}

const patched = dir => git(['-C', dir, 'apply', '--reverse', '--check', patch]).status === 0;
const atCommit = dir => git(['-C', dir, 'rev-parse', 'HEAD']).stdout.trim() === COMMIT;

/** The directory of the game to test. Clones and patches it the first time. */
export function ensureGame() {
  if (process.env.GOGH_STRIKE_ROOT) return path.resolve(process.env.GOGH_STRIKE_ROOT);
  if (existsSync(path.join(cacheDir, '.git')) && atCommit(cacheDir) && patched(cacheDir)) return cacheDir;
  rmSync(cacheDir, {recursive: true, force: true});
  mkdirSync(cacheDir, {recursive: true});
  console.error(`Fetching Gogh Strike ${COMMIT.slice(0, 7)} from ${UPSTREAM} into ${cacheDir} (read-only)…`);
  for (const args of [['init', '--quiet'], ['remote', 'add', 'origin', UPSTREAM], ['fetch', '--quiet', '--depth', '1', 'origin', COMMIT], ['checkout', '--quiet', '--detach', 'FETCH_HEAD']]) {
    const step = git(['-C', cacheDir, ...args], {stdio: ['ignore', 'inherit', 'inherit']});
    if (step.status !== 0) {
      rmSync(cacheDir, {recursive: true, force: true});
      throw new Error(`git ${args[0]} failed while fetching ${UPSTREAM} at ${COMMIT}. Check the network and that the commit is reachable, or set GOGH_STRIKE_ROOT to a patched Gogh Strike checkout.`);
    }
  }
  const applied = git(['-C', cacheDir, 'apply', patch], {encoding: 'utf8'});
  if (applied.status !== 0) {
    rmSync(cacheDir, {recursive: true, force: true});
    throw new Error(`gogh-strike.patch does not apply to upstream ${COMMIT.slice(0, 7)}: ${applied.stderr.trim()}`);
  }
  return cacheDir;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { console.log(ensureGame()); }
  catch (error) { console.error(error.message); process.exit(1); }
}

/** The first IPv4 address of a real interface: not loopback, not Tailscale (by name or by its 100.64.0.0/10 range). */
export function lanAddress() {
  for (const [name, entries] of Object.entries(os.networkInterfaces())) {
    if (/^(lo|tailscale)/i.test(name)) continue;
    for (const entry of entries || []) {
      if (entry.family !== 'IPv4' || entry.internal) continue;
      const [first, second] = entry.address.split('.').map(Number);
      if (first === 127 || (first === 100 && second >= 64 && second <= 127)) continue;
      return entry.address;
    }
  }
  throw new Error('this machine has no non-loopback, non-Tailscale IPv4 address to bind to');
}
