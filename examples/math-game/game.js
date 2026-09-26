'use strict';
// Math-game appliance built on LiveEngine. Pure Node, no dependencies.
//
//  - users   = players (humans over SSE, or generated bots)
//  - servers = tables of 2 seats, one per difficulty level
//  - fit     = player.level === table.level and a free seat
//  - server_ready (2 seated) starts a 5-round "first correct answer wins" game

const { LiveEngine } = require('../..');

const LEVELS = ['easy', 'medium', 'hard'];
const ROUNDS = 5;
const ROUND_MS = 12000;

function makeQuestion(level, rng = Math.random) {
  const r = (n) => 1 + Math.floor(rng() * n);
  if (level === 'easy') { const a = r(20), b = r(20); return { text: `${a} + ${b}`, answer: a + b }; }
  if (level === 'medium') {
    if (rng() < 0.5) { const a = r(12), b = r(12); return { text: `${a} × ${b}`, answer: a * b }; }
    const a = r(50), b = r(50); return { text: `${Math.max(a, b)} − ${Math.min(a, b)}`, answer: Math.abs(a - b) };
  }
  const a = r(12), b = r(12), c = r(30);
  return rng() < 0.5 ? { text: `${a} × ${b} + ${c}`, answer: a * b + c } : { text: `${a} × ${b} − ${c}`, answer: a * b - c };
}

class MathGame {
  constructor({ seed = 0, tablesPerLevel = 2, log = () => {} } = {}) {
    this.engine = new LiveEngine({ seed });
    this.log = log;
    this.players = new Map();    // userId -> { id, name, level, bot, sinks:Set<fn>, wins }
    this.games = new Map();      // tableId -> { round, question, answer, scores, timer, solved }
    this.tableSeq = 0;
    this.playerSeq = 0;

    for (const level of LEVELS) for (let i = 0; i < tablesPerLevel; i++) this.addTable(level);

    const e = this.engine;
    e.on('advertised', ev => this.send(ev.userId, 'advertised', { tables: ev.ids.map(id => this.tableView(id)) }));
    e.on('paired', ev => {
      const table = e.getServer(ev.serverId);
      this.send(ev.userId, 'seated', { table: this.tableView(table.id) });
      for (const m of table.members) if (m !== ev.userId) this.send(m, 'opponent_joined', { player: this.playerView(ev.userId) });
    });
    e.on('server_ready', ev => this.startGame(ev.serverId));
    e.on('unpaired', ev => {
      const game = this.games.get(ev.serverId);
      const table = e.getServer(ev.serverId);
      if (game && table) {
        for (const m of table.members) this.send(m, 'opponent_left', { player: this.playerView(ev.userId) });
        this.endGame(ev.serverId, 'opponent_left');
      }
    });
  }

  // ---- hub management -----------------------------------------------------

  addTable(level) {
    const id = `table-${level}-${++this.tableSeq}`;
    this.engine.upsertServer({ id, capacity: 2, minMembers: 2, scopes: { level, kind: 'math' }, requirements: { level } });
    return id;
  }

  addPlayer({ name, level = 'easy', bot = false, scopes = {} }) {
    if (!LEVELS.includes(level)) throw new Error(`level must be one of ${LEVELS.join(', ')}`);
    const id = `${bot ? 'bot' : 'p'}-${++this.playerSeq}`;
    this.players.set(id, { id, name: name || id, level, bot, sinks: new Set(), wins: 0, ...scopes });
    this.engine.joinHub({ id, scopes: { level, name: name || id, bot }, requirements: { kind: 'math' } });
    return this.players.get(id);
  }

  /** Seat a player at the fullest fitting table; open a new table for their level if none fits. */
  seat(userId) {
    const p = this.players.get(userId);
    if (!p) return { ok: false, reason: 'unknown_player' };
    let r = this.engine.pair(userId, { strategy: 'mostLoaded' });
    if (!r.ok && r.reason === 'no_fit') {
      this.addTable(p.level);
      r = this.engine.pair(userId, { strategy: 'mostLoaded' });
    }
    return r;
  }

  removePlayer(userId) {
    const p = this.players.get(userId);
    if (!p) return false;
    this.engine.removeUser(userId);
    this.players.delete(userId);
    return true;
  }

  /** Generate `count` bots with LiveEngine's random user generator, then seat them. */
  addBots(count, level) {
    const spec = {
      idPrefix: 'bot-',
      scopes: {
        level: level ? [level] : LEVELS,
        name: { type: 'string', prefix: 'Bot ' },
        bot: { type: 'bool', p: 1 },
        speedMs: { type: 'int', min: 700, max: 3500 },
        accuracy: { type: 'float', min: 0.55, max: 0.95 },
      },
      requirements: { kind: 'math' },
    };
    const made = this.engine.generateUsers(count, spec);
    for (const u of made) {
      this.players.set(u.id, { id: u.id, name: u.scopes.name, level: u.scopes.level, bot: true, sinks: new Set(), wins: 0, speedMs: u.scopes.speedMs, accuracy: u.scopes.accuracy });
      this.playerSeq = Math.max(this.playerSeq, Number(u.id.split('-')[1]) || 0);
    }
    const seated = [];
    for (const u of made) { const r = this.seat(u.id); if (r.ok) seated.push(u.id); }
    return { created: made.map(u => u.id), seated };
  }

  // ---- delivery -----------------------------------------------------------

  subscribe(userId, sink) {
    const p = this.players.get(userId);
    if (!p) return () => {};
    p.sinks.add(sink);
    return () => p.sinks.delete(sink);
  }

  send(userId, type, payload = {}) {
    const p = this.players.get(userId);
    if (!p) return;
    const msg = { type, at: Date.now(), ...payload };
    for (const sink of p.sinks) { try { sink(msg); } catch { /* dropped subscriber */ } }
    if (p.bot) this.botReact(p, msg);
  }

  broadcast(tableId, type, payload) {
    const t = this.engine.getServer(tableId);
    if (!t) return;
    for (const m of t.members) this.send(m, type, payload);
  }

  // ---- game loop ----------------------------------------------------------

  startGame(tableId) {
    if (this.games.get(tableId)) return;
    const t = this.engine.getServer(tableId);
    const scores = Object.fromEntries(t.members.map(m => [m, 0]));
    const game = { round: 0, question: null, answer: null, scores, timer: null, solved: false, level: t.scopes.level };
    this.games.set(tableId, game);
    this.log(`game start ${tableId}: ${t.members.map(m => this.players.get(m)?.name).join(' vs ')}`);
    this.broadcast(tableId, 'game_start', { table: this.tableView(tableId), rounds: ROUNDS });
    this.nextRound(tableId);
  }

  nextRound(tableId) {
    const game = this.games.get(tableId);
    if (!game) return;
    clearTimeout(game.timer);
    if (game.round >= ROUNDS) return this.endGame(tableId, 'complete');
    game.round += 1;
    const q = makeQuestion(game.level);
    game.question = q.text; game.answer = q.answer; game.solved = false;
    game.timer = setTimeout(() => {
      this.broadcast(tableId, 'round_result', { round: game.round, winner: null, answer: game.answer, scores: game.scores });
      this.nextRound(tableId);
    }, ROUND_MS);
    if (game.timer.unref) game.timer.unref();
    this.broadcast(tableId, 'question', { round: game.round, rounds: ROUNDS, text: q.text, deadline: Date.now() + ROUND_MS });
  }

  answer(userId, value) {
    const u = this.engine.getUser(userId);
    if (!u || !u.serverId) return { ok: false, reason: 'not_seated' };
    const game = this.games.get(u.serverId);
    if (!game || game.solved) return { ok: false, reason: 'no_open_question' };
    if (Number(value) !== game.answer) { this.send(userId, 'wrong', { round: game.round }); return { ok: true, correct: false }; }
    game.solved = true;
    game.scores[userId] = (game.scores[userId] || 0) + 1;
    this.broadcast(u.serverId, 'round_result', { round: game.round, winner: this.playerView(userId), answer: game.answer, scores: game.scores });
    const t = setTimeout(() => this.nextRound(u.serverId), 1200);
    if (t.unref) t.unref();
    return { ok: true, correct: true };
  }

  endGame(tableId, reason) {
    const game = this.games.get(tableId);
    if (!game) return;
    clearTimeout(game.timer);
    this.games.delete(tableId);
    const ranked = Object.entries(game.scores).sort((a, b) => b[1] - a[1]);
    const winner = ranked.length && (ranked.length === 1 || ranked[0][1] !== ranked[1][1]) ? ranked[0][0] : null;
    if (winner && this.players.get(winner)) this.players.get(winner).wins += 1;
    const table = this.engine.getServer(tableId);
    const members = table ? [...table.members] : [];
    this.log(`game over ${tableId}: ${reason}, winner ${winner ? this.players.get(winner)?.name : 'none'}`);
    for (const m of members) this.send(m, 'game_over', { reason, winner: winner ? this.playerView(winner) : null, scores: game.scores });
    // Free the seats: the table goes back to "open" and is advertised to waiting players again.
    for (const m of members) {
      this.engine.leave(m);
      if (this.players.get(m)?.bot) this.removePlayer(m);
    }
    this.engine.advertiseAll({ limit: 5 });
  }

  // ---- bots ---------------------------------------------------------------

  botReact(bot, msg) {
    if (msg.type !== 'question') return;
    const delay = bot.speedMs || 1500;
    const t = setTimeout(() => {
      if (!this.players.has(bot.id)) return;
      const u = this.engine.getUser(bot.id);
      if (!u || !u.serverId) return;
      const game = this.games.get(u.serverId);
      if (!game || game.round !== msg.round || game.solved) return;
      const right = Math.random() < (bot.accuracy ?? 0.75);
      this.answer(bot.id, right ? game.answer : game.answer + 1);
    }, delay);
    if (t.unref) t.unref();
  }

  // ---- views --------------------------------------------------------------

  playerView(id) {
    const p = this.players.get(id);
    return p ? { id, name: p.name, level: p.level, bot: p.bot, wins: p.wins } : { id };
  }

  tableView(id) {
    const t = this.engine.getServer(id);
    if (!t) return null;
    const game = this.games.get(id);
    return { id, level: t.scopes.level, status: t.status, free: t.free, members: t.members.map(m => this.playerView(m)), inGame: !!game, round: game?.round || 0 };
  }

  state() {
    return {
      stats: this.engine.stats(),
      tables: this.engine.listServers().map(t => this.tableView(t.id)),
      waiting: this.engine.listUsers('waiting').map(u => this.playerView(u.id)),
      games: this.games.size,
    };
  }

  close() {
    for (const [id] of this.games) this.endGame(id, 'shutdown');
    this.engine.stopAutoMatch();
  }
}

module.exports = { MathGame, makeQuestion, LEVELS, ROUNDS, ROUND_MS };
