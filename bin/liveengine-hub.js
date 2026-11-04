#!/usr/bin/env node
'use strict';
const { spawn } = require('node:child_process');
const { existsSync } = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const libc = process.platform === 'linux' ? (process.report.getReport().header.glibcVersionRuntime ? '-gnu' : '-musl') : '';
const prebuilt = path.join(root, 'native', `liveengine-hub.${process.platform}-${process.arch}${libc}${process.platform === 'win32' ? '.exe' : ''}`);
const executable = existsSync(prebuilt) ? prebuilt : path.join(root, 'target', 'release', process.platform === 'win32' ? 'liveengine-hub.exe' : 'liveengine-hub');
const child = existsSync(executable)
  ? spawn(executable, process.argv.slice(2), { stdio: 'inherit' })
  : spawn('cargo', ['run', '--locked', '--release', '--manifest-path', path.join(root, 'Cargo.toml'), '--bin', 'liveengine-hub', '--', ...process.argv.slice(2)], { stdio: 'inherit' });
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => child.kill(signal));
child.on('error', error => { console.error(`Cannot start Rust hub host: ${error.message}`); process.exitCode = 1; });
child.on('exit', (code, signal) => { if (signal) process.kill(process.pid, signal); else process.exitCode = code || 0; });
