// Test support: a real liveengine-hub process on an ephemeral port, and a TCP proxy that can cut it.
import { spawn } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
export const ADMIN_TOKEN = 'live-client-test-administrator-token-0123456789';
// Ports other work on this machine uses; never bind or probe them.
const RESERVED = new Set([8787, 8788, 8967, 9222, 9225]);

/** The hub binary: LIVEENGINE_HUB_BIN, else the newest of native/ and target/ builds. */
export function hubBinary() {
  if (process.env.LIVEENGINE_HUB_BIN) return process.env.LIVEENGINE_HUB_BIN;
  const libc = process.platform === 'linux' ? (process.report.getReport().header.glibcVersionRuntime ? '-gnu' : '-musl') : '';
  const suffix = process.platform === 'win32' ? '.exe' : '';
  const candidates = [
    path.join(root, 'native', `liveengine-hub.${process.platform}-${process.arch}${libc}${suffix}`),
    path.join(root, 'target', 'release', `liveengine-hub${suffix}`),
    path.join(root, 'target', 'debug', `liveengine-hub${suffix}`),
  ].filter(existsSync);
  if (candidates.length === 0) throw new Error('No liveengine-hub binary found; run `npm run build:hub`');
  return candidates.sort((a, b) => statSync(b).mtimeMs - statSync(a).mtimeMs)[0];
}

export async function freePort() {
  for (;;) {
    const server = net.createServer();
    const port = await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => resolve(server.address().port));
    });
    await new Promise(resolve => server.close(resolve));
    if (!RESERVED.has(port)) return port;
  }
}

/** Start a hub. `env` adds or overrides LIVEENGINE_* settings. Call `stop()` when done. */
export async function startHub(env = {}) {
  // A caller that needs the port in advance (for a proxy in front) passes LIVEENGINE_BIND.
  const port = env.LIVEENGINE_BIND ? Number(env.LIVEENGINE_BIND.split(':').pop()) : await freePort();
  // `LIVEENGINE_BIND` may name another address (a LAN test); the readiness check follows it.
  const bindHost = env.LIVEENGINE_BIND ? env.LIVEENGINE_BIND.slice(0, env.LIVEENGINE_BIND.lastIndexOf(':')) : '127.0.0.1';
  const output = [];
  const child = spawn(hubBinary(), ['serve'], {
    env: {
      ...process.env,
      LIVEENGINE_TOKEN: ADMIN_TOKEN,
      LIVEENGINE_BIND: `127.0.0.1:${port}`,
      LIVEENGINE_PUBLIC_URL: `ws://127.0.0.1:${port}/v1/live`,
      LIVEENGINE_ICE_SERVERS: JSON.stringify([{ urls: 'stun:stun.example.test:3478' }]),
      ...env,
    },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.stderr.on('data', chunk => output.push(String(chunk)));
  let exited = false;
  child.on('exit', () => { exited = true; });
  const address = `http://${bindHost}:${port}`;
  const deadline = Date.now() + 15000;
  for (;;) {
    if (exited) throw new Error(`hub exited early: ${output.join('')}`);
    try {
      if ((await fetch(`${address}/health`)).ok) break;
    } catch { /* not listening yet */ }
    if (Date.now() > deadline) { child.kill(); throw new Error(`hub did not start: ${output.join('')}`); }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  return {
    port,
    address,
    wsUrl: `ws://${bindHost}:${port}/v1/live`,
    output: () => output.join(''),
    async stop() {
      if (exited) return;
      await new Promise(resolve => { child.once('exit', resolve); child.kill('SIGTERM'); });
    },
  };
}

/** A TCP proxy in front of a port: `sever()` cuts every connection, `pause()` refuses new ones. */
export async function startProxy(targetPort) {
  const sockets = new Set();
  let server;
  const open = () => new Promise((resolve, reject) => {
    server = net.createServer(client => {
      const upstream = net.connect(targetPort, '127.0.0.1');
      for (const socket of [client, upstream]) {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
        socket.on('error', () => socket.destroy());
      }
      client.pipe(upstream);
      upstream.pipe(client);
    });
    server.once('error', reject);
    server.listen(proxy.port || 0, '127.0.0.1', () => { proxy.port = server.address().port; resolve(); });
  });
  const proxy = {
    port: 0,
    sever() { for (const socket of sockets) socket.destroy(); },
    async pause() {
      proxy.sever();
      await new Promise(resolve => server.close(resolve));
    },
    resume: open,
    async stop() { proxy.sever(); await new Promise(resolve => server.close(resolve)); },
  };
  await open();
  return proxy;
}
