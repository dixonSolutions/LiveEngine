'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { HubClient, HUB_PROTOCOL } = require('../lib/hubs');

test('HubClient validates protocol and sends calls without exposing credentials', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', {
    token: 'private-token',
    fetch: async (url, init) => {
      requests.push({ url, init });
      return Response.json(url.endsWith('contract') ? { protocol: HUB_PROTOCOL } : { result: { ok: true } });
    },
  });
  assert.equal((await client.contract()).protocol, HUB_PROTOCOL);
  assert.deepEqual(await client.call('game', 'join', ['user', 'room']), { ok: true });
  assert.equal(requests[1].init.headers.authorization, 'Bearer private-token');
  assert.equal(requests[1].init.redirect, 'error');
  assert.deepEqual(JSON.parse(requests[1].init.body), { method: 'join', args: ['user', 'room'] });
  assert.ok(!JSON.stringify(client).includes('private-token'));
});

test('HubClient rejects incompatible contracts, HTTP failures and oversized responses', async () => {
  const options = { token: 'secret' };
  await assert.rejects(new HubClient('https://example.com', { ...options, fetch: async () => Response.json({ protocol: 'other' }) }).contract(), /Incompatible/);
  await assert.rejects(new HubClient('https://example.com', { ...options, fetch: async () => Response.json({ error: 'denied' }, { status: 401 }) }).list(), error => error.status === 401 && /denied/.test(error.message));
  await assert.rejects(new HubClient('https://example.com', { ...options, maxResponseBytes: 4, fetch: async () => Response.json({ large: 'payload' }) }).list(), /exceeds/);
  assert.throws(() => new HubClient('https://user:password@example.com', options), /without credentials/);
});

test('HubClient.ping samples the native ping endpoint and reports RTT statistics', async () => {
  let calls = 0;
  const client = new HubClient('https://hub.example', {
    token: 'secret',
    fetch: async url => {
      calls += 1;
      assert.match(url, /\/v1\/ping$/);
      await new Promise(resolve => setTimeout(resolve, 4));
      return Response.json({ protocol: HUB_PROTOCOL, pong: true, serverTimeMs: 1234 });
    },
  });
  const result = await client.ping({ samples: 3 });
  assert.equal(calls, 3);
  assert.equal(result.protocol, HUB_PROTOCOL);
  assert.equal(result.samples.length, 3);
  assert.equal(result.serverTimeMs, 1234);
  assert.ok(result.avgMs >= 0);
  assert.ok(result.minMs <= result.avgMs);
  assert.ok(result.maxMs >= result.avgMs);
  await assert.rejects(client.ping({ samples: 0 }), /samples/);
  await assert.rejects(new HubClient('https://hub.example', {
    token: 'secret',
    fetch: async () => Response.json({ protocol: HUB_PROTOCOL, pong: false }),
  }).ping({ samples: 1 }), /Incompatible ping/);
});

test('HubClient sends template operations and on-demand registration', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'secret', fetch: async (url, init) => {
    requests.push({ url, init });
    return Response.json({});
  } });
  await client.createTemplate({ name: 'game', mode: 'internal' });
  await client.listTemplates();
  await client.getTemplate('game');
  await client.removeTemplate('game');
  await client.register({ id: 'one', template: 'game', ifAbsent: true });
  await client.route(['central'], 'register', { id: 'two', template: 'game', ifAbsent: true });
  assert.deepEqual(requests.map(r => r.init.method), ['POST', 'GET', 'GET', 'DELETE', 'POST', 'POST']);
  assert.deepEqual(JSON.parse(requests[4].init.body), { id: 'one', template: 'game', ifAbsent: true });
  assert.deepEqual(JSON.parse(requests[5].init.body), { path: ['central'], op: 'register', body: { id: 'two', template: 'game', ifAbsent: true } });
});
