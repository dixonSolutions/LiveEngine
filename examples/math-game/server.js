'use strict';
// HTTP + SSE front for the math game. Run: npm run example  (then open http://localhost:3131)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { MathGame, LEVELS } = require('./game');

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', () => { try { resolve(body ? JSON.parse(body) : {}); } catch (err) { reject(err); } });
    req.on('error', reject);
  });
}

function createServer({ seed = 0, log = console.log } = {}) {
  const game = new MathGame({ seed, log });
  const indexHtml = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));

  const json = (res, status, body) => {
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify(body));
  };

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    try {
      if (req.method === 'GET' && url.pathname === '/') {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        return res.end(indexHtml);
      }
      if (req.method === 'GET' && url.pathname === '/api/state') return json(res, 200, game.state());
      if (req.method === 'GET' && url.pathname === '/api/levels') return json(res, 200, LEVELS);
      if (req.method === 'GET' && url.pathname === '/api/tables') {
        const userId = url.searchParams.get('userId');
        const tables = userId ? game.engine.findServersForUser(userId, { limit: 20 }).map(m => game.tableView(m.id)) : game.state().tables;
        return json(res, 200, tables);
      }
      if (req.method === 'GET' && url.pathname === '/api/events') {
        const userId = url.searchParams.get('userId');
        if (!game.players.has(userId)) return json(res, 404, { error: 'unknown player' });
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive', 'access-control-allow-origin': '*' });
        const write = (msg) => res.write(`event: ${msg.type}\ndata: ${JSON.stringify(msg)}\n\n`);
        write({ type: 'hello', player: game.playerView(userId), user: game.engine.getUser(userId) });
        const unsubscribe = game.subscribe(userId, write);
        const ping = setInterval(() => res.write(': ping\n\n'), 15000);
        req.on('close', () => { clearInterval(ping); unsubscribe(); });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/join') {
        const body = await readJson(req);
        const player = game.addPlayer({ name: String(body.name || '').slice(0, 32), level: body.level || 'easy' });
        const seat = body.autoSeat === false ? null : game.seat(player.id);
        return json(res, 200, { player: game.playerView(player.id), seat, tables: game.engine.findServersForUser(player.id).map(m => game.tableView(m.id)) });
      }
      if (req.method === 'POST' && url.pathname === '/api/seat') {
        const { userId, tableId } = await readJson(req);
        const r = tableId ? game.engine.join(userId, tableId) : game.seat(userId);
        return json(res, r.ok ? 200 : 409, r);
      }
      if (req.method === 'POST' && url.pathname === '/api/answer') {
        const { userId, answer } = await readJson(req);
        return json(res, 200, game.answer(userId, answer));
      }
      if (req.method === 'POST' && url.pathname === '/api/leave') {
        const { userId } = await readJson(req);
        return json(res, 200, { removed: game.removePlayer(userId) });
      }
      if (req.method === 'POST' && url.pathname === '/api/bots') {
        const { count = 2, level } = await readJson(req);
        return json(res, 200, game.addBots(Math.min(Number(count) || 0, 500), level));
      }
      json(res, 404, { error: 'not found' });
    } catch (err) {
      json(res, 400, { error: err.message });
    }
  });

  server.game = game;
  server.on('close', () => game.close());
  return server;
}

module.exports = { createServer };

if (require.main === module) {
  const port = Number(process.env.PORT || 3131);
  createServer().listen(port, () => console.log(`math game on http://localhost:${port}  (POST /api/bots {"count":4} to add bots)`));
}
