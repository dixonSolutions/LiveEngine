'use client';

import { useEffect, useState } from 'react';

type Server = {
  id: string;
  status: string;
  capacity: number;
  free: number;
  members: string[];
  scopes: unknown;
};

type Stats = {
  users: number;
  waiting: number;
  paired: number;
  servers: number;
  open: number;
};

type Ping = {
  avgMs: number;
  minMs: number;
  maxMs: number;
  jitterMs: number;
};

export default function LiveEnginePage() {
  const [userId, setUserId] = useState('player-1');
  const [servers, setServers] = useState<Server[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [ping, setPing] = useState<Ping | null>(null);
  const [message, setMessage] = useState('');

  async function load() {
    const response = await fetch('/api/liveengine/servers', { cache: 'no-store' });
    const data = await response.json();
    if (!response.ok) return setMessage(data.error || 'Could not load servers');
    setServers(data.servers ?? []);
    setStats(data.stats);
  }

  useEffect(() => {
    load();
    const timer = setInterval(load, 2500);
    return () => clearInterval(timer);
  }, []);

  async function connect() {
    setMessage('Connecting...');
    const response = await fetch('/api/liveengine/match', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ userId, scopes: { region: 'au' }, requirements: {} }),
    });
    const data = await response.json();
    setMessage(response.ok
      ? `Matched ${userId} into ${data.serverId}`
      : data.error || data.reason || 'No match');
    load();
  }

  async function samplePing() {
    const response = await fetch('/api/liveengine/ping', { cache: 'no-store' });
    const data = await response.json();
    if (response.ok) setPing(data);
    else setMessage(data.error || 'Ping failed');
  }

  return (
    <main style={{ maxWidth: 880, margin: '40px auto', fontFamily: 'system-ui', padding: 20 }}>
      <h1>LiveEngine matchmaking</h1>
      <p>Client connection and live server listing through Next.js route handlers.</p>

      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <input value={userId} onChange={event => setUserId(event.target.value)} />
        <button onClick={connect}>Find a match</button>
        <button onClick={samplePing}>Ping hub</button>
      </div>

      <p>{message}</p>
      {ping && (
        <p>
          Ping {ping.avgMs.toFixed(1)} ms average ({ping.minMs.toFixed(1)}–{ping.maxMs.toFixed(1)} ms,
          {' '}{ping.jitterMs.toFixed(1)} ms jitter)
        </p>
      )}
      {stats && <p>{stats.paired} paired / {stats.waiting} waiting / {stats.open} open rooms</p>}

      <h2>Servers</h2>
      <ul>
        {servers.map(server => (
          <li key={server.id}>
            <strong>{server.id}</strong> — {server.status}, {server.members.length}/{server.capacity} joined,
            {' '}{server.free} free — <code>{JSON.stringify(server.scopes)}</code>
          </li>
        ))}
      </ul>
    </main>
  );
}
