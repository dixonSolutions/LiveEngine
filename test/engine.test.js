'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { LiveEngine, NativeEngine } = require('..');

const chessRoom = (id, rating) => ({
  id, capacity: 2, minMembers: 2,
  scopes: { game: 'chess', rating },
  requirements: { game: 'chess', rating: { between: [rating - 200, rating + 200] } },
});

test('native module loads', () => {
  assert.equal(typeof NativeEngine, 'function');
  const e = new NativeEngine(1);
  assert.deepEqual(e.stats().users, 0);
});

test('users and servers round-trip through the binding', () => {
  const e = new LiveEngine({ seed: 1 });
  assert.equal(e.upsertUser({ id: 'u1', scopes: { rating: 1500, region: 'eu', premium: true, tags: ['a', 'b'] }, requirements: { game: 'chess' } }), true);
  assert.equal(e.upsertUser({ id: 'u1', scopes: { rating: 1600 } }), false);
  const u = e.getUser('u1');
  assert.deepEqual(u.scopes, { rating: 1600 });
  assert.equal(u.status, 'waiting');
  assert.equal(u.serverId, null);
  assert.equal(e.upsertServer(chessRoom('r1', 1500)), true);
  const s = e.getServer('r1');
  assert.equal(s.capacity, 2);
  assert.equal(s.free, 2);
  assert.deepEqual(s.requirements.rules.map(r => r.op).sort(), ['between', 'eq']);
  assert.equal(e.getUser('nope'), null);
});

test('requirement shorthand, array and ruleset forms are equivalent', () => {
  const e = new LiveEngine();
  e.upsertUser({ id: 'u', scopes: { rating: 1500, region: 'eu' } });
  e.upsertServer({ id: 'a', requirements: { rating: { between: [1400, 1600] }, region: ['eu', 'us'] } });
  e.upsertServer({ id: 'b', requirements: [{ key: 'rating', op: 'gte', value: 1400 }, { key: 'region', op: 'in', value: ['eu'] }] });
  e.upsertServer({ id: 'c', requirements: { rules: [{ key: 'region', op: 'eq', value: 'us' }, { key: 'rating', op: 'eq', value: 1500 }], match: 'any' } });
  e.upsertServer({ id: 'd', requirements: { region: 'us' } });
  assert.deepEqual(e.findServersForUser('u').map(m => m.id), ['a', 'b', 'c']);
  assert.throws(() => e.upsertServer({ id: 'x', requirements: { rating: { bogus: 1 } } }), /unknown requirement op/);
  assert.throws(() => e.upsertServer({ id: 'x', requirements: { rating: { between: 5 } } }), /needs an array/);
});

test('pairing lifecycle emits events in order', () => {
  const e = new LiveEngine({ seed: 3 });
  const types = [];
  e.on('event', ev => types.push(ev.type));
  e.upsertServer(chessRoom('r1', 1500));
  e.upsertUser({ id: 'a', scopes: { game: 'chess', rating: 1450 } });
  e.upsertUser({ id: 'b', scopes: { game: 'chess', rating: 1650 } });
  e.upsertUser({ id: 'c', scopes: { game: 'chess', rating: 2400 } });
  const results = e.matchAll();
  assert.equal(results.length, 2);
  assert.equal(e.getServer('r1').status, 'full');
  assert.deepEqual(types, ['server_added', 'server_available', 'user_added', 'user_added', 'user_added', 'paired', 'paired', 'server_full', 'server_ready']);
  assert.deepEqual(e.join('c', 'r1'), { ok: false, reason: 'full', userId: 'c', serverId: 'r1' });
  assert.equal(e.leave('a').ok, true);
  assert.equal(e.getServer('r1').status, 'open');
  assert.equal(e.getUser('a').status, 'waiting');
  assert.equal(e.pair('c').reason, 'no_fit');
  const again = e.pair('a');
  assert.equal(again.ok, true);
  assert.equal(again.server.id, 'r1');
  assert.deepEqual(again.server.members, ['b', 'a']);
});

test('server_ready carries the member list once', () => {
  const e = new LiveEngine();
  const ready = [];
  e.on('server_ready', ev => ready.push(ev));
  e.upsertServer({ id: 'tbl', capacity: 4, minMembers: 2 });
  for (const id of ['p1', 'p2', 'p3']) { e.upsertUser({ id }); e.autoJoin(id); }
  assert.equal(ready.length, 1);
  assert.deepEqual(ready[0].ids, ['p1', 'p2']);
});

test('advertise pushes fitting servers back to the user hub', () => {
  const e = new LiveEngine();
  const ads = [];
  e.on('advertised', ev => ads.push(ev));
  e.upsertServers([{ id: 's1', scopes: { mode: 'ranked' } }, { id: 's2', scopes: { mode: 'casual' } }, { id: 's3', scopes: { mode: 'ranked' }, status: 'closed' }]);
  const { created, advertised } = e.joinHub({ id: 'u', requirements: { mode: 'ranked' } });
  assert.equal(created, true);
  assert.equal(advertised, 1);
  assert.deepEqual(ads[0], { type: 'advertised', seq: ads[0].seq, userId: 'u', ids: ['s1'] });
  e.setServerStatus('s3', 'open');
  assert.equal(e.advertiseAll(), 2);
  assert.deepEqual(ads[1].ids, ['s1', 's3']);
});

test('strategies: first, leastLoaded, mostLoaded, affinity, random', () => {
  const e = new LiveEngine({ seed: 11 });
  e.upsertServers([1200, 1500, 1800].map(r => ({ id: `r${r}`, capacity: 5, scopes: { rating: r } })));
  e.upsertUser({ id: 'x', scopes: { rating: 1550 } });
  e.upsertUser({ id: 'filler' }); e.join('filler', 'r1200');
  assert.deepEqual(e.findServersForUser('x').map(m => m.id), ['r1200', 'r1500', 'r1800']);
  assert.deepEqual(e.findServersForUser('x', { strategy: 'leastLoaded' }).map(m => m.id), ['r1500', 'r1800', 'r1200']);
  assert.deepEqual(e.findServersForUser('x', { strategy: 'mostLoaded', limit: 1 }).map(m => m.id), ['r1200']);
  assert.deepEqual(e.findServersForUser('x', { affinity: ['rating'] }).map(m => m.id), ['r1500', 'r1800', 'r1200']);
  assert.deepEqual(e.findServersForUser('x', { affinity: { rating: 1 } })[0], { id: 'r1500', score: 50, load: 0 });
  const rnd = e.findServersForUser('x', { strategy: 'random' });
  assert.equal(rnd.length, 3);
  assert.deepEqual(rnd.map(m => m.id).sort(), ['r1200', 'r1500', 'r1800']);
});

test('mirror query: users for a server, FIFO order, only waiting users', () => {
  const e = new LiveEngine();
  e.upsertServer({ id: 's', capacity: 3, requirements: { level: { gte: 2 } } });
  e.upsertUsers([{ id: 'a', scopes: { level: 3 } }, { id: 'b', scopes: { level: 1 } }, { id: 'c', scopes: { level: 2 } }, { id: 'd', scopes: { level: 9 }, status: 'idle' }]);
  assert.deepEqual(e.findUsersForServer('s').map(m => m.id), ['a', 'c']);
  e.join('a', 's');
  assert.deepEqual(e.findUsersForServer('s').map(m => m.id), ['c']);
  e.setUserStatus('d', 'waiting');
  assert.deepEqual(e.findUsersForServer('s').map(m => m.id), ['c', 'd']);
});

test('removing a server frees its members; removing a user frees its slot', () => {
  const e = new LiveEngine();
  const unpaired = [];
  e.on('unpaired', ev => unpaired.push(ev.reason));
  e.upsertServer({ id: 's', capacity: 2 });
  e.upsertUsers([{ id: 'a' }, { id: 'b' }]);
  e.matchAll();
  assert.equal(e.getServer('s').status, 'full');
  e.removeUser('a');
  assert.equal(e.getServer('s').status, 'open');
  assert.deepEqual(e.getServer('s').members, ['b']);
  e.removeServer('s');
  assert.equal(e.getUser('b').status, 'waiting');
  assert.deepEqual(unpaired, ['user_removed', 'server_removed']);
});

test('generateUsers honours the spec and is deterministic per seed', () => {
  const spec = {
    idPrefix: 'bot-',
    scopes: { rating: { type: 'int', min: 1000, max: 2000 }, region: ['eu', 'us', 'apac'], premium: { type: 'bool', p: 0.25 }, name: { type: 'string', prefix: 'Bot ' }, tags: { type: 'tags', values: ['a', 'b', 'c', 'd'], count: 2 } },
    requirements: { game: 'chess' },
  };
  const a = new LiveEngine({ seed: 99 }).generateUsers(20, spec);
  const b = new LiveEngine({ seed: 99 }).generateUsers(20, spec);
  assert.equal(a.length, 20);
  assert.equal(a[0].id, 'bot-1');
  assert.equal(a[0].scopes.name, 'Bot 1');
  assert.deepEqual(a.map(u => u.scopes.rating), b.map(u => u.scopes.rating));
  for (const u of a) {
    assert.ok(u.scopes.rating >= 1000 && u.scopes.rating <= 2000);
    assert.ok(['eu', 'us', 'apac'].includes(u.scopes.region));
    assert.equal(typeof u.scopes.premium, 'boolean');
    assert.equal(u.scopes.tags.length, 2);
    assert.deepEqual(u.requirements.rules, [{ key: 'game', op: 'eq', value: 'chess' }]);
  }
  assert.throws(() => new LiveEngine().generateUsers(1, { scopes: { x: { type: 'wat' } } }), /unknown generator type/);
});

test('scales: 2k servers, 20k generated users, one matchAll', () => {
  const e = new LiveEngine({ seed: 5, autoEmit: false });
  const regions = ['eu', 'us', 'apac', 'sa'];
  e.upsertServers(Array.from({ length: 2000 }, (_, i) => ({
    id: `srv${i}`, capacity: 8,
    scopes: { region: regions[i % 4], tier: i % 10 },
    requirements: { tier: { between: [(i % 10) - 1, (i % 10) + 1] } },
  })));
  e.generateUsers(20000, { scopes: { region: regions, tier: { type: 'int', min: 0, max: 9 } }, requirements: { region: 'eu' } });
  const t0 = process.hrtime.bigint();
  const results = e.matchAll();
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  assert.equal(results.length, 4000);   // 500 eu servers × 8
  assert.equal(e.stats().full, 500);
  assert.ok(ms < 5000, `matchAll took ${ms}ms`);
  assert.ok(e.drainEvents().length > 4000);
});

test('autoEmit=false leaves events for manual drain', () => {
  const e = new LiveEngine({ autoEmit: false });
  e.upsertUser({ id: 'u' });
  assert.equal(e.pendingEvents(), 1);
  assert.deepEqual(e.drainEvents().map(ev => ev.type), ['user_added']);
  assert.equal(e.pendingEvents(), 0);
});

test('startAutoMatch pairs on a timer', async () => {
  const e = new LiveEngine();
  e.upsertServer({ id: 's', capacity: 1 });
  e.upsertUser({ id: 'u' });
  const paired = new Promise(resolve => e.once('paired', resolve));
  e.startAutoMatch(5);
  const ev = await paired;
  e.stopAutoMatch();
  assert.equal(ev.serverId, 's');
});

test('type errors surface as JS exceptions', () => {
  const e = new LiveEngine();
  assert.throws(() => e.upsertUser({}), /user\.id is required/);
  assert.throws(() => e.getUser(42), /must be a string/);
  assert.throws(() => e.setServerStatus('s', 'bogus'), /unknown server status/);
  assert.throws(() => e.findServersForUser('u', { strategy: 'bogus' }), /unknown strategy/);
});
