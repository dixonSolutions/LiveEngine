'use strict';

// A small, dependency-free user hub web interface.
//
// It keeps the administrative token server-side and exposes only the
// matchmaking operations a player needs.
//
//   LIVEENGINE_URL=http://127.0.0.1:8787 \
//   LIVEENGINE_TOKEN=... \
//   LIVEENGINE_HUB_ID=user-hub \
//   node examples/user-hub/server.js

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { HubClient } = require('../../lib/hubs');

const port = Number(process.env.PORT || 8970);
const host = process.env.HOST || '127.0.0.1';
const hubUrl = process.env.LIVEENGINE_URL || 'http://127.0.0.1:8787';
const hubId = process.env.LIVEENGINE_HUB_ID || 'user-hub';
const tokenFile = process.env.LIVEENGINE_TOKEN_FILE;
const token = tokenFile
  ? fs.readFileSync(tokenFile, 'utf8').trim()
  : process.env.LIVEENGINE_TOKEN;

if (!token) {
  console.error('Set LIVEENGINE_TOKEN or LIVEENGINE_TOKEN_FILE');
  process.exit(1);
}

const hub = new HubClient(hubUrl, { token, timeoutMs: 10_000 });
const publicDir = path.join(__dirname, 'public');
const allowedHosts = new Set([`127.0.0.1:${port}`, `localhost:${port}`]);
const userIdPattern = /^[A-Za-z0-9_.-]{1,64}$/;

let ready = false;
let readyPromise = null;

function ensureReady() {
  if (ready) return Promise.resolve();
  if (!readyPromise) {
    readyPromise = (async () => {
      await hub.contract();
      const listed = await hub.list();
      const hubs = Array.isArray(listed) ? listed : listed?.hubs || [];
      if (!hubs.some(entry => entry.id === hubId)) {
        try {
          await hub.register({
            id: hubId,
            mode: 'internal',
            scopes: { purpose: 'user-hub-interface' },
          });
        } catch (error) {
          if (error.status !== 409) throw error;
        }
      }
      ready = true;
    })().finally(() => { readyPromise = null; });
  }
  return readyPromise;
}

function sendJson(response, status, body) {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
  });
  response.end(payload);
}

function fail(response, status, message) {
  sendJson(response, status, { error: message });
}

async function readJson(request, limit = 64 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error('request body too large'), { status: 413 });
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw Object.assign(new Error('invalid JSON body'), { status: 400 });
  }
}

function checkRequest(request) {
  if (!allowedHosts.has(String(request.headers.host || '').toLowerCase())) {
    throw Object.assign(new Error('invalid host'), { status: 403 });
  }
  const origin = request.headers.origin;
  if (origin && origin !== `http://${request.headers.host}`) {
    throw Object.assign(new Error('invalid origin'), { status: 403 });
  }
}

function parseScopes(value) {
  if (value === undefined || value === null) return {};
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw Object.assign(new Error('scopes must be an object'), { status: 400 });
  }
  return value;
}

async function stateFor(userId) {
  await ensureReady();
  const [user, servers, stats] = await Promise.all([
    userId ? hub.call(hubId, 'getUser', [userId]) : null,
    hub.call(hubId, 'listServers'),
    hub.call(hubId, 'stats'),
  ]);
  const server = user?.serverId
    ? await hub.call(hubId, 'getServer', [user.serverId])
    : null;
  return {
    hub: { id: hubId, url: hubUrl, protocol: 'liveengine.hub/v1' },
    user: user || null,
    server: server || null,
    servers: Array.isArray(servers) ? servers : [],
    stats,
  };
}

function routeApi(request, response, url) {
  return (async () => {
    if (request.method === 'GET' && url.pathname === '/api/config') {
      return sendJson(response, 200, {
        hubId,
        hubUrl,
        protocol: 'liveengine.hub/v1',
        ready,
      });
    }
    if (request.method === 'GET' && url.pathname === '/api/ping') {
      await ensureReady();
      const samples = Number(url.searchParams.get('samples') || 5);
      const ping = await hub.ping({ samples });
      return sendJson(response, 200, ping);
    }
    if (request.method === 'GET' && url.pathname === '/api/state') {
      const userId = String(url.searchParams.get('userId') || '');
      if (userId && !userIdPattern.test(userId)) return fail(response, 400, 'invalid userId');
      return sendJson(response, 200, await stateFor(userId));
    }
    if (request.method !== 'POST') {
      return fail(response, 405, 'method not allowed');
    }

    checkRequest(request);
    const body = await readJson(request);

    if (url.pathname === '/api/join') {
      const userId = String(body.userId || '');
      if (!userIdPattern.test(userId)) return fail(response, 400, 'invalid userId');
      await ensureReady();
      const scopes = parseScopes(body.scopes);
      await hub.call(hubId, 'upsertUser', [{
        id: userId,
        scopes,
        requirements: body.requirements,
      }]);
      if (body.autoMatch === false) {
        return sendJson(response, 200, { ok: true, userId, serverId: null });
      }
      const result = await hub.call(hubId, 'autoJoin', [userId]);
      if (!result?.ok) return sendJson(response, 409, result);
      const server = await hub.call(hubId, 'getServer', [result.serverId]);
      return sendJson(response, 200, { ...result, server });
    }

    if (url.pathname === '/api/join-room') {
      const userId = String(body.userId || '');
      const serverId = String(body.serverId || '');
      if (!userIdPattern.test(userId)) return fail(response, 400, 'invalid userId');
      if (!serverId) return fail(response, 400, 'serverId is required');
      await ensureReady();
      const result = await hub.call(hubId, 'join', [userId, serverId]);
      if (!result?.ok) return sendJson(response, 409, result);
      const server = await hub.call(hubId, 'getServer', [serverId]);
      return sendJson(response, 200, { ...result, server });
    }

    if (url.pathname === '/api/leave') {
      const userId = String(body.userId || '');
      if (!userIdPattern.test(userId)) return fail(response, 400, 'invalid userId');
      await ensureReady();
      return sendJson(response, 200, await hub.call(hubId, 'leave', [userId]));
    }

    if (url.pathname === '/api/rooms') {
      const id = String(body.id || '');
      const capacity = Number(body.capacity ?? 2);
      if (!id || id.length > 64) return fail(response, 400, 'invalid room id');
      if (!Number.isInteger(capacity) || capacity < 1 || capacity > 64) {
        return fail(response, 400, 'capacity must be an integer between 1 and 64');
      }
      await ensureReady();
      const created = await hub.call(hubId, 'upsertServer', [{
        id,
        capacity,
        minMembers: Number(body.minMembers || 0),
        scopes: parseScopes(body.scopes),
        requirements: body.requirements,
      }]);
      return sendJson(response, created ? 201 : 200, { id, created });
    }

    return fail(response, 404, 'not found');
  })().catch(error => {
    const status = error.status || (error.message?.includes('rate limit') ? 429 : 503);
    fail(response, status, error.message || 'hub request failed');
  });
}

function serveStatic(request, response, url) {
  const file = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
  const resolved = path.resolve(publicDir, file);
  if (!resolved.startsWith(publicDir + path.sep)) {
    response.writeHead(403);
    return response.end('forbidden');
  }
  fs.readFile(resolved, (error, data) => {
    if (error) {
      response.writeHead(404);
      return response.end('not found');
    }
    const type = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.svg': 'image/svg+xml',
    }[path.extname(resolved)] || 'application/octet-stream';
    response.writeHead(200, { 'content-type': type, 'cache-control': 'no-store' });
    response.end(data);
  });
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url, `http://${request.headers.host || `${host}:${port}`}`);
  if (url.pathname.startsWith('/api/')) return routeApi(request, response, url);
  if (request.method === 'GET') return serveStatic(request, response, url);
  fail(response, 405, 'method not allowed');
});

server.listen(port, host, () => {
  console.log(`LiveEngine user hub on http://${host}:${port}`);
  console.log(`Hub ${hubId} at ${hubUrl}`);
});
