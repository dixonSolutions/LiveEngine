'use strict';
// Release tarballs include supported prebuilt binaries. Source checkouts build with Cargo.
//
// A published package is told apart by having no `.git`: npm never packs one. There a binary
// that is present is always used, never rebuilt. In a source checkout the addon is rebuilt when
// it is missing, older than the Rust sources (Cargo.toml, Cargo.lock, build.rs, src/), cannot be
// loaded, or reports another version than package.json, so `npm ci` never keeps a stale build.
const { existsSync, readdirSync, statSync } = require('node:fs');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const root = path.join(__dirname, '..');
const libc = process.platform === 'linux' ? (process.report.getReport().header.glibcVersionRuntime ? '-gnu' : '-musl') : '';
const binary = path.join(root, 'native', `liveengine.${process.platform}-${process.arch}${libc}.node`);

function newestSource() {
  let newest = 0;
  const visit = file => {
    let stat;
    try { stat = statSync(file); } catch { return; }
    if (stat.isDirectory()) { for (const entry of readdirSync(file)) visit(path.join(file, entry)); }
    else newest = Math.max(newest, stat.mtimeMs);
  };
  for (const file of ['Cargo.toml', 'Cargo.lock', 'build.rs', 'src']) visit(path.join(root, file));
  return newest;
}

function staleReason() {
  if (!existsSync(binary)) return 'is missing';
  if (!existsSync(path.join(root, '.git'))) return null;
  if (statSync(binary).mtimeMs < newestSource()) return 'is older than the Rust sources';
  const { version } = require(path.join(root, 'package.json'));
  let built;
  try { built = require(binary).nativeVersion?.(); } catch { return 'cannot be loaded'; }
  if (built !== version) return `was built from ${built ? `version ${built}` : 'an older version'}, not ${version}`;
  return null;
}

const reason = staleReason();
if (reason) {
  console.log(`LiveEngine: the native addon ${reason}; building it with Cargo.`);
  const result = spawnSync(process.execPath, [path.join(__dirname, 'build.js')], { stdio: 'inherit' });
  process.exit(result.status || (result.error ? 1 : 0));
}
