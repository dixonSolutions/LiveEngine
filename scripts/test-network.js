'use strict';
const assert = require('node:assert/strict');
const { HubClient } = require('../lib/hubs');
const address = process.env.LIVEENGINE_NETWORK_URL;
const token = process.env.LIVEENGINE_NETWORK_TEST_TOKEN;
const id = `network-test-${process.env.GITHUB_RUN_ID || Date.now()}`;
const client = new HubClient(address, { token, timeoutMs: 15000 });
(async () => {
  await client.contract();
  const unauthenticated = await fetch(`${client.address}/v1/hubs`, { redirect: 'error', signal: AbortSignal.timeout(15000) });
  assert.equal(unauthenticated.status, 401);
  await client.register({ id, mode: 'internal', ttlSeconds: 300, scopes: { purpose: 'cross-network-test' } });
  try {
    await client.call(id, 'upsertServer', [{ id: 'network-room', capacity: 2 }]);
    await client.call(id, 'upsertUser', [{ id: 'remote-player' }]);
    const result = await client.call(id, 'autoJoin', ['remote-player']);
    assert.equal(result.ok, true);
    assert.equal(result.serverId, 'network-room');
    assert.equal((await client.call(id, 'stats')).paired, 1);
    console.log('Cross-network connectivity passed: authenticated contract, registry, room creation, player connection and pairing.');
  } finally { await client.remove(id); }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
