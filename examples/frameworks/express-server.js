'use strict';

// Express + local LiveEngine.
//
// Install the example dependencies next to this file:
//   npm install express @dixonsolutions/liveengine
// Run:
//   node express-server.js

const express = require('express');
const { LiveEngine } = require('@dixonsolutions/liveengine');

const app = express();
app.use(express.json({ limit: '256kb' }));

const engine = new LiveEngine({
  seed: Number(process.env.LIVEENGINE_SEED || 0),
  autoEmit: false,
});

app.post('/api/servers', (req, res) => {
  const { id, capacity = 2, minMembers = 0, scopes = {}, requirements } = req.body || {};
  if (!id) return res.status(400).json({ error: 'id is required' });
  const created = engine.upsertServer({ id, capacity, minMembers, scopes, requirements });
  res.status(created ? 201 : 200).json({ id, created });
});

app.post('/api/match', (req, res) => {
  const { userId, scopes = {}, requirements } = req.body || {};
  if (!userId) return res.status(400).json({ error: 'userId is required' });
  engine.upsertUser({ id: userId, scopes, requirements });
  const result = engine.pair(userId);
  if (!result.ok) return res.status(409).json(result);
  res.json(result);
});

app.post('/api/leave', (req, res) => {
  const { userId } = req.body || {};
  if (!userId) return res.status(400).json({ error: 'userId is required' });
  res.json(engine.leave(userId));
});

app.get('/api/stats', (_req, res) => {
  res.json(engine.stats());
});

// Server-sent events. With autoEmit disabled, this endpoint drains the queue.
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
  const timer = setInterval(() => {
    for (const event of engine.drainEvents()) {
      res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
    }
  }, 250);
  req.on('close', () => clearInterval(timer));
});

const port = Number(process.env.PORT || 3000);
app.listen(port, '127.0.0.1', () => {
  console.log(`Express matchmaking API on http://127.0.0.1:${port}`);
});
