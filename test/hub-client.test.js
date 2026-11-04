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
