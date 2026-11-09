// Test support: a plain web page with no game, served from its own loopback origin, on which browser
// tests run real LiveClients. The page is a text input and the LiveClient module, nothing else.
import { readFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { freePort } from './hub-process.mjs';

const clientFile = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'lib', 'live-client.mjs');
const PAGE = '<!doctype html><meta charset="utf-8"><title>LiveEngine plain page</title><input id="t" type="text" autofocus>';

/** Serve the page and lib/live-client.mjs. Returns `{origin, port, stop()}`; pass `port` to allow it in the hub's origins. */
export async function startPlainPage(port) {
  port ??= await freePort();
  const server = http.createServer(async (request, response) => {
    if (request.url === '/live-client.mjs') {
      response.writeHead(200, { 'Content-Type': 'text/javascript' }).end(await readFile(clientFile));
    } else if (request.url === '/' || request.url === '/index.html') {
      response.writeHead(200, { 'Content-Type': 'text/html' }).end(PAGE);
    } else response.writeHead(404).end();
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  return { origin: `http://127.0.0.1:${port}`, port, stop: () => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); }) };
}

/** Load the plain page in `page` and connect a LiveClient from `descriptor` as `window.__client`, hearing messages into `window.__heard`. */
export async function connectPlainClient(page, origin, descriptor, options = {}) {
  await page.goto(`${origin}/`, { waitUntil: 'domcontentloaded' });
  await page.evaluate(async ([desc, opts]) => {
    const { LiveClient } = await import('/live-client.mjs');
    const client = new LiveClient(desc, opts);
    window.__client = client;
    window.__heard = [];
    window.__events = [];
    client.on('message', frame => window.__heard.push({ from: frame.from, via: frame.via, payload: frame.payload }));
    client.on('transport', event => window.__events.push({ transport: event.transport, reason: event.reason }));
    client.on('direct-failed', event => window.__events.push({ directFailed: event.reason ?? true }));
    await client.connect();
  }, [descriptor, options]);
}

/** The selected ICE candidate pair of the link to `peerId`, and every local candidate the page gathered, from getStats. */
export function selectedPair(page, peerId) {
  return page.evaluate(async peer => {
    const link = window.__client.links.get(peer);
    const stats = await link.pc.getStats();
    const byId = new Map();
    stats.forEach(report => byId.set(report.id, report));
    let pair;
    stats.forEach(report => { if (report.type === 'transport' && report.selectedCandidatePairId) pair = byId.get(report.selectedCandidatePairId); });
    if (!pair) stats.forEach(report => { if (report.type === 'candidate-pair' && report.nominated && report.state === 'succeeded') pair = report; });
    const describe = candidate => candidate && {
      type: candidate.candidateType, protocol: candidate.protocol, relayProtocol: candidate.relayProtocol, address: candidate.address ?? candidate.ip,
      port: candidate.port, url: candidate.url,
    };
    const gathered = [];
    stats.forEach(report => { if (report.type === 'local-candidate') gathered.push(describe(report)); });
    return pair ? { local: describe(byId.get(pair.localCandidateId)), remote: describe(byId.get(pair.remoteCandidateId)), bytesSent: pair.bytesSent, bytesReceived: pair.bytesReceived, gathered } : { gathered };
  }, peerId);
}

/** Send `payload` to `peerId` from one plain page and wait for it on another. Resolves with the transport it arrived on. */
export async function sendPlain(from, to, fromId, toId, payload, until) {
  const mark = await to.evaluate(() => window.__heard.length);
  const sent = await from.evaluate(([peer, body]) => window.__client.send(body, { to: peer }), [toId, payload]);
  if (!sent) throw new Error('the client did not accept the frame');
  const frame = await until(() => to.evaluate(([count, sender, n]) => window.__heard.slice(count).find(f => f.from === sender && f.payload?.n === n), [mark, fromId, payload.n]), `${toId} to hear frame ${payload.n} from ${fromId}`, 10_000);
  return frame.via;
}
