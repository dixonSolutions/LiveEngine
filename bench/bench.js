'use strict';
// Rough throughput numbers for README. Run: npm run bench
const { LiveEngine } = require('..');

function time(label, fn) {
  const t0 = process.hrtime.bigint();
  const out = fn();
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log(`${label.padEnd(48)} ${ms.toFixed(1).padStart(8)} ms`);
  return out;
}

const SERVERS = Number(process.argv[2] || 5000);
const USERS = Number(process.argv[3] || 50000);
const regions = ['eu', 'us', 'apac', 'sa', 'me'];
const e = new LiveEngine({ seed: 1, autoEmit: false });

time(`upsertServers x${SERVERS}`, () => e.upsertServers(Array.from({ length: SERVERS }, (_, i) => ({
  id: `srv${i}`, capacity: 10, scopes: { region: regions[i % 5], tier: i % 20, mode: i % 3 ? 'ranked' : 'casual' },
  requirements: { tier: { between: [(i % 20) - 2, (i % 20) + 2] } },
}))));
time(`generateUsers x${USERS}`, () => e.generateUsers(USERS, {
  scopes: { region: regions, tier: { type: 'int', min: 0, max: 19 }, rating: { type: 'int', min: 800, max: 2400 } },
  requirements: { region: 'eu', mode: 'ranked' },
}));
const r1 = time('findServersForUser x10000 (indexed eq)', () => { let n = 0; for (let i = 1; i <= 10000; i++) n += e.findServersForUser(`user-${i}`, { limit: 5 }).length; return n; });
const r2 = time('findServersForUser x10000 (affinity: tier)', () => { let n = 0; for (let i = 1; i <= 10000; i++) n += e.findServersForUser(`user-${i}`, { limit: 5, affinity: ['tier'] }).length; return n; });
const paired = time('matchAll (FIFO, first fit)', () => e.matchAll().length);
time('advertiseAll (remaining waiting users)', () => e.advertiseAll({ limit: 3 }));
time('drainEvents', () => e.drainEvents().length);
console.log({ matches1: r1, matches2: r2, paired, stats: e.stats() });
