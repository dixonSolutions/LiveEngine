// Test support: a real coturn TURN server in a podman container, on loopback, for browser tests.
//
// It uses coturn's shared-secret mode (use-auth-secret), the mode docs/hosting.md and the
// peer-to-peer guide tell operators to use with LIVEENGINE_TURN_SECRET. Host networking is used
// only so that loopback is the same loopback for the container and the browsers; the listener and the
// relay both bind 127.0.0.1 explicitly, on a random port and a small random relay range. The container
// has its own memory cap (memory-swap equal, so no swap) and is removed by stop().
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import dgram from 'node:dgram';
import net from 'node:net';

export const COTURN_IMAGE = process.env.LIVEENGINE_TEST_COTURN_IMAGE || 'docker.io/coturn/coturn:4.6.3';
const RELAY_PORTS = 24;
const RESERVED = new Set([8787, 8788, 8967, 9222, 9225]);

function podman(args, { timeoutMs = 120_000 } = {}) {
  return new Promise(resolve => {
    execFile('podman', args, { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
      resolve({ ok: !error, code: error?.code, stdout: String(stdout), stderr: String(stderr), error });
    });
  });
}

/** Make sure the image is local, pulling it once if it is not. Throws with the reason if it cannot. */
export async function ensureCoturnImage(image = COTURN_IMAGE) {
  if ((await podman(['image', 'exists', image])).ok) return image;
  const pulled = await podman(['pull', '--quiet', image], { timeoutMs: 600_000 });
  if (!pulled.ok) {
    const why = (pulled.stderr.trim() || pulled.error?.message || 'unknown error').split('\n').slice(-3).join(' | ');
    throw new Error(`the coturn image ${image} is not here and could not be pulled: ${why}`);
  }
  return image;
}

const udpFree = port => new Promise(resolve => {
  const socket = dgram.createSocket('udp4');
  socket.once('error', () => resolve(false));
  socket.bind(port, '127.0.0.1', () => socket.close(() => resolve(true)));
});
const tcpFree = port => new Promise(resolve => {
  const server = net.createServer();
  server.once('error', () => resolve(false));
  server.listen(port, '127.0.0.1', () => server.close(() => resolve(true)));
});

/** A listening port free on both UDP and TCP, and a contiguous free UDP range for relays; none reserved. */
async function pickPorts() {
  for (let attempt = 0; attempt < 200; attempt++) {
    const listen = 20_000 + Math.floor(Math.random() * 20_000);
    const first = 41_000 + Math.floor(Math.random() * 20_000);
    const range = Array.from({ length: RELAY_PORTS }, (_, i) => first + i);
    if (RESERVED.has(listen) || range.some(port => RESERVED.has(port))) continue;
    if (!(await udpFree(listen)) || !(await tcpFree(listen))) continue;
    if ((await Promise.all(range.map(udpFree))).every(Boolean)) return { listen, min: first, max: first + RELAY_PORTS - 1 };
  }
  throw new Error('no free port range for coturn');
}

/** Start coturn with `secret` as its shared secret. Returns `{url, port, relayPorts, realm, logs(), stop()}`. */
export async function startCoturn({ secret, realm = 'liveengine.test', image = COTURN_IMAGE } = {}) {
  await ensureCoturnImage(image);
  const { listen, min, max } = await pickPorts();
  const name = `liveengine-e2e-coturn-${randomBytes(4).toString('hex')}`;
  const run = await podman(['run', '-d', '--name', name, '--memory=1g', '--memory-swap=1g', '--pids-limit=256', '--network=host',
    '--security-opt=no-new-privileges', image,
    '-n', '-v', '--log-file=stdout', '--no-cli', '--no-tls', '--no-dtls', '--fingerprint', '--use-auth-secret', `--static-auth-secret=${secret}`,
    `--realm=${realm}`, `--listening-ip=127.0.0.1`, `--listening-port=${listen}`, `--relay-ip=127.0.0.1`, `--min-port=${min}`, `--max-port=${max}`,
    '--allow-loopback-peers', '--no-multicast-peers', '--user-quota=20', '--total-quota=100']);
  const stop = async () => { await podman(['rm', '-f', '-t', '1', name]); };
  if (!run.ok) { await stop(); throw new Error(`coturn did not start: ${(run.stderr.trim() || run.error?.message).split('\n').slice(-2).join(' | ')}`); }
  const logs = async () => { const out = await podman(['logs', name]); return out.stdout + out.stderr; };
  const deadline = Date.now() + 30_000;
  for (;;) {
    const ready = await new Promise(resolve => {
      const socket = net.connect(listen, '127.0.0.1');
      socket.once('connect', () => { socket.destroy(); resolve(true); });
      socket.once('error', () => resolve(false));
    });
    if (ready) break;
    const state = await podman(['inspect', '--format', '{{.State.Running}}', name]);
    if (state.stdout.trim() !== 'true') { const output = await logs(); await stop(); throw new Error(`coturn exited: ${output.trim().split('\n').slice(-3).join(' | ')}`); }
    if (Date.now() > deadline) { const output = await logs(); await stop(); throw new Error(`coturn did not listen on ${listen}: ${output.trim().split('\n').slice(-3).join(' | ')}`); }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  return { name, port: listen, url: `turn:127.0.0.1:${listen}?transport=udp`, relayPorts: [min, max], realm, logs, stop };
}
