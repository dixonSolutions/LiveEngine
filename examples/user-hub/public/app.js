import { LiveClient } from '/live-client.mjs';

'use strict';

const $ = id => document.getElementById(id);
const userIdPattern = /^[A-Za-z0-9_.-]{1,64}$/;
const storageKey = 'liveengine.userHub.userId';
const state = {
  userId: localStorage.getItem(storageKey) || `player-${Math.random().toString(36).slice(2, 7)}`,
  lastState: null,
  lastError: '',
  lastErrorAt: 0,
  stateBusy: false,
  pingBusy: false,
};

function setText(id, value) {
  $(id).textContent = value;
}

function setStatus(kind, label) {
  const dot = $('status-dot');
  dot.className = `dot ${kind}`;
  $('hub-label').textContent = label;
}

function log(message, kind = '') {
  const entry = document.createElement('li');
  if (kind) entry.className = kind;
  const time = document.createElement('time');
  time.textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const text = document.createElement('span');
  text.textContent = message;
  entry.append(time, text);
  const list = $('log');
  list.prepend(entry);
  while (list.children.length > 50) list.lastElementChild.remove();
}

function setMessage(message, kind = '') {
  const el = $('join-message');
  el.textContent = message;
  el.className = `message ${kind}`.trim();
}

function parseJsonField(id, label) {
  const raw = $(id).value.trim();
  if (!raw) return undefined;
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error(`${label} must be valid JSON`);
  }
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
  });
  let data = null;
  try { data = await response.json(); } catch { data = {}; }
  if (!response.ok) {
    const message = data?.error || data?.reason || `HTTP ${response.status}`;
    throw Object.assign(new Error(message), { status: response.status, data });
  }
  return data;
}

function currentUserId() {
  const value = $('user-id').value.trim();
  if (!userIdPattern.test(value)) {
    throw new Error('User ID must be 1–64 letters, digits, dot, underscore or hyphen');
  }
  return value;
}

function reportError(error) {
  const message = String(error?.message || error);
  setStatus('bad', 'Hub unavailable');
  const pingBadge = $('ping-badge');
  pingBadge.textContent = '-- ms';
  pingBadge.className = 'badge bad';
  const now = Date.now();
  if (message !== state.lastError || now - state.lastErrorAt > 15000) {
    state.lastError = message;
    state.lastErrorAt = now;
    log(message, 'bad');
  }
  setMessage(message, 'bad');
}

function renderSession(data) {
  const user = data.user;
  const server = data.server;
  const pill = $('session-state');
  pill.className = `pill ${user?.status || 'idle'}`;
  pill.textContent = user?.status || 'idle';
  setText('session-user', user?.id || '--');
  setText('session-room', user?.serverId || '--');
  setText('session-members', server ? `${server.members.length} / ${server.capacity}` : '--');
  setText('session-free', server ? String(server.free) : '--');
  $('matched-scopes').textContent = server
    ? JSON.stringify({ room: server.id, scopes: server.scopes, members: server.members }, null, 2)
    : 'No active room';
}

function renderRooms(data) {
  const list = $('rooms');
  list.textContent = '';
  const rooms = data.servers || [];
  const open = rooms.filter(room => room.status === 'open' && room.free > 0).length;
  setText('room-count', `${open} open / ${rooms.length} total`);
  if (!rooms.length) {
    const empty = document.createElement('p');
    empty.className = 'empty';
    empty.textContent = 'No rooms yet. Create a demo room below.';
    list.append(empty);
    return;
  }
  const selected = data.user?.serverId;
  for (const room of rooms) {
    const card = document.createElement('article');
    card.className = `room${selected === room.id ? ' selected' : ''}`;

    const head = document.createElement('div');
    head.className = 'room-head';
    const name = document.createElement('strong');
    name.textContent = room.id;
    const status = document.createElement('span');
    status.className = 'hint';
    status.textContent = room.status;
    head.append(name, status);

    const meta = document.createElement('div');
    meta.className = 'room-meta';
    for (const value of [
      `${room.members.length}/${room.capacity} joined`,
      `${room.free} free`,
      `min ${room.minMembers}`,
    ]) {
      const item = document.createElement('span');
      item.textContent = value;
      meta.append(item);
    }

    const scopes = document.createElement('div');
    scopes.className = 'room-scopes';
    scopes.textContent = JSON.stringify(room.scopes);

    const actions = document.createElement('div');
    actions.className = 'actions';
    const join = document.createElement('button');
    join.textContent = 'Join room';
    join.disabled = Boolean(selected) || room.status !== 'open' || room.free < 1;
    join.addEventListener('click', () => joinRoom(room.id));
    actions.append(join);

    card.append(head, meta, scopes, actions);
    list.append(card);
  }
}

async function refreshState() {
  if (state.stateBusy) return;
  state.stateBusy = true;
  try {
    const userId = state.userId;
    const data = await api(`/api/state?userId=${encodeURIComponent(userId)}`);
    state.lastState = data;
    state.lastError = '';
    setStatus('good', 'Connected');
    setText('hub-id', data.hub.id);
    $('hub-label').textContent = `${data.hub.id} · ${data.hub.url}`;
    renderSession(data);
    renderRooms(data);
    setMessage(data.user?.status === 'paired'
      ? `Matched into ${data.user.serverId}`
      : data.user?.status === 'waiting' ? 'Waiting for a fit...' : 'Ready.', 'good');
  } catch (error) {
    reportError(error);
  } finally {
    state.stateBusy = false;
  }
}

async function pingNow() {
  if (state.pingBusy) return;
  state.pingBusy = true;
  try {
    const result = await api('/api/ping?samples=5');
    setText('ping-avg', `${result.avgMs.toFixed(1)} ms`);
    setText('ping-min', `${result.minMs.toFixed(1)} ms`);
    setText('ping-max', `${result.maxMs.toFixed(1)} ms`);
    setText('ping-jitter', `${result.jitterMs.toFixed(1)} ms`);
    setText('ping-updated', `Updated ${new Date().toLocaleTimeString()}`);
    const badge = $('ping-badge');
    badge.textContent = `${result.avgMs.toFixed(0)} ms`;
    badge.className = 'badge';
    const bars = $('ping-bars');
    bars.textContent = '';
    const max = Math.max(...result.samples, 1);
    for (const sample of result.samples) {
      const bar = document.createElement('div');
      bar.className = 'ping-bar';
      bar.style.height = `${Math.max(8, (sample / max) * 100)}%`;
      bar.title = `${sample.toFixed(1)} ms`;
      bars.append(bar);
    }
  } catch (error) {
    reportError(error);
  } finally {
    state.pingBusy = false;
  }
}

async function join() {
  try {
    state.userId = currentUserId();
    localStorage.setItem(storageKey, state.userId);
    const scopes = parseJsonField('scopes', 'Scopes') || {};
    const requirements = parseJsonField('requirements', 'Requirements');
    setMessage('Matchmaking...');
    const result = await api('/api/join', {
      method: 'POST',
      body: JSON.stringify({
        userId: state.userId,
        scopes,
        requirements,
        autoMatch: $('auto-match').checked,
      }),
    });
    if (result.serverId) {
      log(`Matched ${state.userId} into ${result.serverId}`, 'good');
      setMessage(`Matched into ${result.serverId}`, 'good');
    } else {
      log(`Joined ${state.userId}`);
      setMessage('Joined. Waiting for a match.', 'good');
    }
    await refreshState();
  } catch (error) {
    if (error.status === 409) setMessage(error.message, 'bad');
    else reportError(error);
  }
}

async function joinRoom(serverId) {
  try {
    state.userId = currentUserId();
    await api('/api/join-room', {
      method: 'POST',
      body: JSON.stringify({ userId: state.userId, serverId }),
    });
    log(`Joined ${serverId}`, 'good');
    await refreshState();
  } catch (error) {
    if (error.status === 409) setMessage(error.message, 'bad');
    else reportError(error);
  }
}

async function leave() {
  try {
    state.userId = currentUserId();
    live?.close();
    await api('/api/leave', { method: 'POST', body: JSON.stringify({ userId: state.userId }) });
    log(`Left the room as ${state.userId}`);
    setMessage('Left the room.', 'good');
    await refreshState();
  } catch (error) {
    if (error.status === 409) setMessage(error.message, 'bad');
    else reportError(error);
  }
}

async function createRoom() {
  try {
    const capacity = Number($('demo-capacity').value);
    const id = `demo-${Math.random().toString(36).slice(2, 7)}`;
    await api('/api/rooms', {
      method: 'POST',
      body: JSON.stringify({ id, capacity, scopes: { demo: true }, requirements: {} }),
    });
    log(`Created room ${id}`, 'good');
    await refreshState();
  } catch (error) {
    reportError(error);
  }
}


let live;
async function ticket() {
  return api('/api/ticket', { method: 'POST', body: JSON.stringify({ userId: state.userId }) });
}
async function connectLive() {
  try {
    state.userId = currentUserId();
    live?.close();
    live = new LiveClient(await ticket(), { renew: ticket });
    live.on('message', frame => {
      const item = document.createElement('li');
      item.textContent = `${frame.from} via ${frame.via}: ${JSON.stringify(frame.payload)}`;
      $('messages').prepend(item);
    });
    const update = () => { $('live-status').textContent = `Peers: ${live.peers().map(p => `${p} (${live.transportOf(p)})`).join(', ') || 'none'}`; };
    live.on('presence', update); live.on('transport', update); live.on('welcome', update);
    live.on('close', () => { $('live-status').textContent = 'Disconnected'; });
    await live.connect(); update();
  } catch (error) { reportError(error); }
}
async function watchTree() {
  try {
    const watch = await api('/api/watch', { method: 'POST', body: '{}' });
    const socket = new WebSocket(watch.url);
    let seq = -1;
    socket.onopen = () => socket.send(JSON.stringify({ type: 'auth', ticket: watch.ticket, path: [], depth: 3 }));
    socket.onmessage = event => {
      const frame = JSON.parse(event.data);
      if (frame.type === 'snapshot') { seq = frame.seq; $('tree').textContent = JSON.stringify(frame, null, 2); }
      else if (typeof frame.seq === 'number') {
        if (frame.seq !== seq + 1) { socket.send(JSON.stringify({ type: 'resnapshot' })); return; }
        seq = frame.seq;
        // Refresh the displayed tree after an ordered diff.
        socket.send(JSON.stringify({ type: 'resnapshot' }));
      }
    };
    socket.onclose = () => { $('tree').textContent = 'Watch disconnected; retrying…'; setTimeout(watchTree, 3000); };
  } catch (error) { $('tree').textContent = String(error.message); setTimeout(watchTree, 3000); }
}

function init() {
  $('user-id').value = state.userId;
  $('live-connect').onclick = connectLive;
  $('live-disconnect').onclick = () => live?.close();
  $('send-relay').onclick = () => live?.send({ text: $('live-message').value });
  $('send-direct').onclick = () => { const peer = live?.peers()[0]; if (peer) live.send({ text: $('live-message').value }, { to: peer }); };
  watchTree();
  $('find-match').addEventListener('click', join);
  $('leave').addEventListener('click', leave);
  $('refresh').addEventListener('click', () => { refreshState(); pingNow(); });
  $('ping-now').addEventListener('click', pingNow);
  $('create-room').addEventListener('click', createRoom);
  $('clear-log').addEventListener('click', () => { $('log').textContent = ''; });
  $('user-id').addEventListener('change', event => {
    state.userId = event.target.value.trim();
    if (userIdPattern.test(state.userId)) localStorage.setItem(storageKey, state.userId);
  });

  log('User hub interface ready');
  refreshState();
  pingNow();
  setInterval(refreshState, 2500);
  setInterval(pingNow, 10000);
}

init();
