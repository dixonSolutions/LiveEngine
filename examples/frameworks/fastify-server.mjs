// Fastify + HubClient to a running LiveEngine hub.
//
// Install the example dependencies next to this file:
//   npm install fastify @dixonsolutions/liveengine
// Run:
//   LIVEENGINE_URL=http://127.0.0.1:8787 LIVEENGINE_TOKEN=... \
//   LIVEENGINE_HUB_ID=ranked node fastify-server.mjs

import Fastify from 'fastify';
import { HubClient } from '@dixonsolutions/liveengine/hubs';

const hubUrl = process.env.LIVEENGINE_URL;
const token = process.env.LIVEENGINE_TOKEN;
const hubId = process.env.LIVEENGINE_HUB_ID || 'ranked';

if (!hubUrl || !token) {
  throw new Error('Set LIVEENGINE_URL and LIVEENGINE_TOKEN');
}

const app = Fastify({ logger: true });
const hub = new HubClient(hubUrl, { token, timeoutMs: 10_000 });
await hub.contract();

app.get('/api/health', async () => ({ app: 'ok', hub: await hub.contract() }));

app.post('/api/servers', async (request, reply) => {
  const { id, capacity = 2, minMembers = 0, scopes = {}, requirements } = request.body || {};
  if (!id) return reply.code(400).send({ error: 'id is required' });
  const result = await hub.call(hubId, 'upsertServer', [
    { id, capacity, minMembers, scopes, requirements },
  ]);
  return { id, created: result };
});

app.post('/api/match', async (request, reply) => {
  const { userId, scopes = {}, requirements } = request.body || {};
  if (!userId) return reply.code(400).send({ error: 'userId is required' });
  await hub.call(hubId, 'upsertUser', [{ id: userId, scopes, requirements }]);
  const result = await hub.call(hubId, 'autoJoin', [userId]);
  if (!result.ok) return reply.code(409).send(result);
  const server = await hub.call(hubId, 'getServer', [result.serverId]);
  return { ...result, server };
});

app.get('/api/stats', async () => hub.call(hubId, 'stats'));

const port = Number(process.env.PORT || 3001);
await app.listen({ port, host: '127.0.0.1' });
