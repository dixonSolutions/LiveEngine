'use strict';
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { HubClient } = require('../lib/hubs');
const [centralPort, remotePort, secretDir, customImage] = process.argv.slice(2);
const centralToken = readFileSync(`${secretDir}/central-token`, 'utf8').trim();
const remoteToken = readFileSync(`${secretDir}/remote-token`, 'utf8').trim();
const central = new HubClient(`http://127.0.0.1:${centralPort}`, { token: centralToken });
const remote = new HubClient(`http://127.0.0.1:${remotePort}`, { token: remoteToken });
(async () => {
  await central.contract();
  await remote.contract();
  await remote.register({ id: 'gogh', mode: 'internal', scopes: { game: 'gogh-strike' } });
  await central.register({ id: 'gogh-remote', mode: 'external', address: 'http://remote:8787', remoteHubId: 'gogh', remoteToken, scopes: { game: 'gogh-strike' } });
  await central.call('gogh-remote', 'upsertServer', [{ id: 'room', capacity: 12, scopes: { game: 'gogh-strike' } }]);
  await central.call('gogh-remote', 'upsertUser', [{ id: 'network-player', requirements: { game: 'gogh-strike' } }]);
  const result = await central.call('gogh-remote', 'autoJoin', ['network-player']);
  assert.equal(result.ok, true);
  assert.equal(result.serverId, 'room');
  assert.equal((await remote.call('gogh', 'getUser', ['network-player'])).status, 'paired');
  assert.ok(!JSON.stringify(await central.list()).includes(remoteToken));
  await central.importRegistry({ address: 'http://remote:8787', token: remoteToken, prefix: 'imported-' });
  assert.equal((await central.call('imported-gogh', 'stats')).paired, 1);
  const denied = await fetch(`${central.address}/v1/hubs`);
  assert.equal(denied.status, 401);
  if (customImage) {
    await central.register({ id: 'custom-counter', mode: 'external', address: 'http://custom:8787', remoteHubId: 'counter', remoteToken });
    assert.deepEqual(await central.call('custom-counter', 'increment', [7]), { count: 7 });
    assert.deepEqual(await central.call('custom-counter', 'read'), { count: 7 });
    console.log('Standalone custom hub container passed arbitrary DTO exchange through the central registry.');
  }
  await central.remove('gogh-remote');
  assert.equal((await remote.call('gogh', 'stats')).paired, 1, 'detaching a remote must preserve its state');
  console.log('Container integration passed: independent credentials, remote handshake, forwarded matchmaking, live registry import, auth and detach.');
})().catch(error => { console.error(error.message); process.exitCode = 1; });
