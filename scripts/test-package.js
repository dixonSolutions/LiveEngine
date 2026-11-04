'use strict';
const assert = require('node:assert/strict');
const { mkdtempSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const tarball = path.resolve(process.argv[2] || `dixonsolutions-liveengine-${require('../package.json').version}.tgz`);
const directory = mkdtempSync(path.join(tmpdir(), 'liveengine-install-'));
try {
  const result = spawnSync('npm', ['install', '--prefix', directory, '--no-audit', '--no-fund', '--foreground-scripts', tarball], { stdio: 'inherit' });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, 'package installation failed');
  const installed = path.join(directory, 'node_modules', '@dixonsolutions', 'liveengine');
  const { LiveEngine, HubClient } = require(installed);
  const engine = new LiveEngine();
  engine.upsertServer({ id: 'packaged-room' });
  engine.upsertUser({ id: 'packaged-user' });
  assert.equal(engine.autoJoin('packaged-user').ok, true);
  assert.equal(typeof HubClient, 'function');
  const cli = spawnSync(process.execPath, [path.join(installed, 'bin', 'liveengine-hub.js')], { env: { ...process.env, LIVEENGINE_TOKEN: 'too-short' }, encoding: 'utf8' });
  assert.equal(cli.status, 1);
  assert.match(cli.stderr, /must be at least 24 bytes/);
  console.log('Packed npm artifact passed installation, native matchmaking, client export and standalone host checks.');
} finally { rmSync(directory, { recursive: true, force: true }); }
