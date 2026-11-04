'use strict';
// Release tarballs include supported prebuilt binaries. Source checkouts build with Cargo.
const { existsSync } = require('node:fs');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const libc = process.platform === 'linux' ? (process.report.getReport().header.glibcVersionRuntime ? '-gnu' : '-musl') : '';
const binary = path.join(__dirname, '..', 'native', `liveengine.${process.platform}-${process.arch}${libc}.node`);
if (!existsSync(binary)) {
  const result = spawnSync(process.execPath, [path.join(__dirname, 'build.js')], { stdio: 'inherit' });
  process.exit(result.status || (result.error ? 1 : 0));
}
