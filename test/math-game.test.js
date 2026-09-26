'use strict';
// End-to-end: boot the example appliance, join two players over HTTP, follow SSE, play a full game.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { createServer } = require('../examples/math-game/server');
const { MathGame, makeQuestion, ROUNDS } = require('../examples/math-game/game');

function post(port, path, body) {
  return fetch(`http://127.0.0.1:${port}${path}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(r => r.json());
}

/** Minimal SSE client: resolves with a function that awaits the next event of a type. */
function sse(port, userId) {
  return new Promise((resolve, reject) => {
    const waiters = [];
    const seen = [];
    const pending = [];   // delivered but not yet consumed by next()
    const req = http.get(`http://127.0.0.1:${port}/api/events?userId=${userId}`, res => {
      let buf = '';
      res.setEncoding('utf8');
      res.on('data', chunk => {
        buf += chunk;
        let idx;
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, idx); buf = buf.slice(idx + 2);
          const data = frame.split('\n').find(l => l.startsWith('data: '));
          if (!data) continue;
          const msg = JSON.parse(data.slice(6));
          seen.push(msg);
          const w = waiters.findIndex(x => x.type === msg.type);
          if (w >= 0) waiters.splice(w, 1)[0].resolve(msg); else pending.push(msg);
        }
      });
      resolve({
        seen,
        next: (type, ms = 5000) => new Promise((res2, rej2) => {
          const p = pending.findIndex(m => m.type === type);
          if (p >= 0) return res2(pending.splice(p, 1)[0]);
          const t = setTimeout(() => rej2(new Error(`timeout waiting for ${type}; seen ${seen.map(m => m.type).join(',')}`)), ms);
          waiters.push({ type, resolve: m => { clearTimeout(t); res2(m); } });
        }),
        close: () => req.destroy(),
      });
    });
    req.on('error', reject);
  });
}

test('makeQuestion produces solvable questions per level', () => {
  for (const level of ['easy', 'medium', 'hard']) for (let i = 0; i < 50; i++) {
    const q = makeQuestion(level);
    assert.equal(typeof q.answer, 'number');
    assert.ok(q.text.length > 2);
  }
});

test('MathGame seats players on level tables and opens new tables when full', () => {
  const g = new MathGame({ seed: 4, tablesPerLevel: 1 });
  const ids = [];
  for (let i = 0; i < 5; i++) { const p = g.addPlayer({ name: `p${i}`, level: 'easy' }); ids.push(p.id); }
  const seats = ids.map(id => g.seat(id));
  assert.ok(seats.every(s => s.ok));
  const easyTables = g.engine.listServers().filter(t => t.scopes.level === 'easy');
  assert.equal(easyTables.length, 3);            // 1 preset + 2 opened on demand (5 players / 2 seats)
  assert.equal(g.games.size, 2);                  // two full tables started games
  assert.equal(g.engine.getUser(ids[4]).serverId, easyTables[2].id);
  g.close();
});

test('full game over HTTP + SSE between a human client and a bot', async () => {
  const server = createServer({ seed: 8, log: () => {} });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const port = server.address().port;
  let stream;
  try {
    const joined = await post(port, '/api/join', { name: 'Ada', level: 'medium' });
    assert.equal(joined.seat.ok, true);
    stream = await sse(port, joined.player.id);
    const bots = await post(port, '/api/bots', { count: 1, level: 'medium' });
    assert.equal(bots.seated.length, 1);
    await stream.next('game_start');
    let myWins = 0;
    for (let round = 1; round <= ROUNDS; round++) {
      const q = await stream.next('question');
      assert.equal(q.round, round);
      const answer = makeAnswer(q.text);
      const r = await post(port, '/api/answer', { userId: joined.player.id, answer });
      assert.equal(r.ok, true);
      const result = await stream.next('round_result');
      if (result.winner && result.winner.id === joined.player.id) myWins++;
    }
    const over = await stream.next('game_over');
    assert.equal(over.reason, 'complete');
    assert.equal(over.scores[joined.player.id], myWins);
    assert.ok(myWins >= 1, 'the human answered instantly and should win at least one round');
    const state = await fetch(`http://127.0.0.1:${port}/api/state`).then(r => r.json());
    assert.equal(state.stats.paired, 0, 'seats are freed after the game');
    assert.equal(state.stats.waiting, 1, 'the human is back in the waiting hub, the bot was removed');
    const reseat = await post(port, '/api/seat', { userId: joined.player.id });
    assert.equal(reseat.ok, true);
  } finally {
    if (stream) stream.close();
    server.closeAllConnections();
    await new Promise(r => server.close(r));
  }
});

// Evaluates the question text the server sends ("7 × 8 − 3").
function makeAnswer(text) {
  const expr = text.replace(/×/g, '*').replace(/−/g, '-');
  // eslint-disable-next-line no-new-func
  return Function(`"use strict"; return (${expr});`)();
}
