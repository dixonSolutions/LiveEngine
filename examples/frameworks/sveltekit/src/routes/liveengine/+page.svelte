<script>
  import { onMount } from 'svelte';

  let userId = 'player-1';
  let servers = [];
  let stats = null;
  let ping = null;
  let message = '';

  async function load() {
    const response = await fetch('/api/liveengine/servers', { cache: 'no-store' });
    const data = await response.json();
    if (!response.ok) return (message = data.error || 'Could not load servers');
    servers = data.servers ?? [];
    stats = data.stats;
  }

  async function connect() {
    message = 'Connecting...';
    const response = await fetch('/api/liveengine/match', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ userId, scopes: { region: 'au' }, requirements: {} }),
    });
    const data = await response.json();
    message = response.ok
      ? `Matched ${userId} into ${data.serverId}`
      : data.error || data.reason || 'No match';
    load();
  }

  async function samplePing() {
    const response = await fetch('/api/liveengine/ping', { cache: 'no-store' });
    const data = await response.json();
    if (response.ok) ping = data;
    else message = data.error || 'Ping failed';
  }

  onMount(() => {
    load();
    const timer = setInterval(load, 2500);
    return () => clearInterval(timer);
  });
</script>

<main>
  <h1>LiveEngine matchmaking</h1>
  <p>Client connection and live server listing through SvelteKit server routes.</p>

  <div class="actions">
    <input bind:value={userId} aria-label="User ID">
    <button on:click={connect}>Find a match</button>
    <button on:click={samplePing}>Ping hub</button>
  </div>

  <p>{message}</p>
  {#if ping}
    <p>Ping {ping.avgMs.toFixed(1)} ms average ({ping.minMs.toFixed(1)}–{ping.maxMs.toFixed(1)} ms, {ping.jitterMs.toFixed(1)} ms jitter)</p>
  {/if}
  {#if stats}
    <p>{stats.paired} paired / {stats.waiting} waiting / {stats.open} open rooms</p>
  {/if}

  <h2>Servers</h2>
  <ul>
    {#each servers as server}
      <li>
        <strong>{server.id}</strong> — {server.status}, {server.members.length}/{server.capacity} joined,
        {server.free} free — <code>{JSON.stringify(server.scopes)}</code>
      </li>
    {/each}
  </ul>
</main>

<style>
  main { max-width: 880px; margin: 40px auto; padding: 20px; font-family: system-ui, sans-serif; }
  .actions { display: flex; gap: 8px; margin-bottom: 16px; }
  input { padding: 8px; }
</style>
