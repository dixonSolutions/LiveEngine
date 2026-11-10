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
  await assert.rejects(new HubClient('https://example.com', { ...options, fetch: async () => Response.json({ error: 'too many sign-in attempts' }, { status: 429, headers: { 'retry-after': '7' } }) }).login('a', 'b'), error => error.status === 429 && error.retryAfter === 7);
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

test('HubClient requests nested routes and tree discovery', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'secret', fetch: async (url, init) => {
    requests.push({ url, init });
    return Response.json({ hubs: [] });
  } });
  await client.tree(3);
  await client.route(['eu', 'games'], 'tree', { depth: 2 });
  assert.equal(requests[0].url, 'https://hub.example/v1/tree?depth=3');
  assert.equal(requests[0].init.headers.authorization, 'Bearer secret');
  assert.deepEqual(JSON.parse(requests[1].init.body), { path: ['eu', 'games'], op: 'tree', body: { depth: 2 } });
});

test('HubClient sends schema and registry operations with encoded identifiers', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'test-administrator-token-long-enough', fetch: async (url, init) => {
    requests.push({ url, init });
    return new Response(init.method === 'DELETE' ? null : '{}', { status: init.method === 'DELETE' ? 204 : 200 });
  } });
  await client.createSchema({id: 'game', version: '1', schema: {type: 'object'}});
  await client.listSchemas();
  await client.getSchema('game@1');
  await client.removeSchema('game@1');
  await client.exportRegistry();
  await client.importRegistry({address: 'https://remote.example', token: 'remote-administrator-token-long-enough', refreshSeconds: 60});
  assert.deepEqual(requests.map(r => r.init.method), ['POST', 'GET', 'GET', 'DELETE', 'GET', 'POST']);
  assert.match(requests[2].url, /\/v1\/schemas\/game%401$/);
  assert.match(requests[4].url, /\/v1\/registry\/export$/);
  assert.equal(JSON.parse(requests[5].init.body).refreshSeconds, 60);
  assert.ok(requests.every(r => r.init.headers.authorization === 'Bearer test-administrator-token-long-enough'));
});

test('account and connection methods use the corresponding authenticated routes', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'session-token-for-testing-123456', fetch: async (url, init) => { requests.push({ url, init }); return Response.json({}); } });
  await client.login('alice', 'long-password'); await client.me(); await client.changePassword('old-password', 'new-password');
  await client.listAccounts(); await client.createAccount({ username: 'alice', role: 'user', password: 'long-password' });
  await client.getAccount('alice'); await client.updateAccount('alice', { enabled: false }); await client.deleteAccount('alice');
  await client.connections({ user: 'alice', room: 'a b' }); await client.kick('socket-1'); await client.logout();
  assert.deepEqual(requests.map(r => r.init.method), ['POST','GET','POST','GET','POST','GET','PATCH','DELETE','GET','DELETE','DELETE']);
  assert.match(requests[8].url, /user=alice&room=a\+b/);
  assert.match(requests[9].url, /connections\/socket-1$/);
});

test('connections leaves out filters that are undefined or null instead of sending "undefined"', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'secret', fetch: async url => { requests.push(url); return Response.json([]); } });
  await client.connections({ hub: undefined, room: null, user: 'alice' });
  await client.connections({ hub: undefined });
  await client.connections();
  assert.match(requests[0], /\/v1\/connections\?user=alice$/);
  assert.match(requests[1], /\/v1\/connections$/);
  assert.match(requests[2], /\/v1\/connections$/);
  assert.ok(requests.every(url => !url.includes('undefined') && !url.includes('null')));
});

test('link and link credential methods use the corresponding routes with encoded ids', async () => {
  const requests = [];
  const client = new HubClient('https://hub.example', { token: 'secret', fetch: async (url, init) => {
    requests.push({ url, init });
    return init.method === 'DELETE' ? new Response(null, { status: 204 }) : Response.json({});
  } });
  const link = { id: 'to far', address: 'https://far.example', remoteToken: 'remote-credential-at-least-24', hubId: 'far', userId: 'bridge', serverId: 'room' };
  await client.listLinks();
  await client.createLink(link);
  await client.getLink('to far');
  assert.equal(await client.deleteLink('to far'), null);
  await client.issueLinkCredential('far/hub');
  assert.equal(await client.revokeLinkCredentials('far/hub'), null);
  assert.deepEqual(requests.map(r => [r.init.method, new URL(r.url).pathname]), [
    ['GET', '/v1/links'],
    ['POST', '/v1/links'],
    ['GET', '/v1/links/to%20far'],
    ['DELETE', '/v1/links/to%20far'],
    ['POST', '/v1/hubs/far%2Fhub/link-credentials'],
    ['DELETE', '/v1/hubs/far%2Fhub/link-credentials'],
  ]);
  assert.deepEqual(JSON.parse(requests[1].init.body), link);
});
