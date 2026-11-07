import LiveEngine, { NativeEngine, HubClient, HUB_PROTOCOL, nativeVersion, type PairRequest } from '@dixonsolutions/liveengine';
import { LiveClient, connectLive, watchHubs } from '@dixonsolutions/liveengine/live-client';
import { createHubApi } from '@dixonsolutions/liveengine/hub-api';

const engine = new LiveEngine({ seed: 42 });
engine.upsertUser({ id: 'alice', scopes: { region: 'au', rating: 1200 } });
engine.upsertServer({ id: 'room', capacity: 4 });
engine.findServersForUser('alice', { strategy: 'affinity', affinity: ['rating'] });
engine.join('alice', 'room');
engine.on('paired', event => console.log(event.serverId));
new NativeEngine(42).stats();
const hub = new HubClient('https://hub.example', { token: 'secret' });
const request: PairRequest = { subject: { id: 'alice', kind: 'peer' }, target: { kind: 'peer' } };
async function example() {
  await hub.register({ id: 'game', channels: { chat: { delivery: 'reliable' } } });
  await hub.request('/v1/links', { method: 'POST', body: { id: 'link' } });
  const descriptor = await hub.pair(request);
  const client = new LiveClient(descriptor, { renew: hub.renewal(request) });
  await client.connect();
  client.on('message', frame => console.log(frame.payload));
  client.send({ text: 'hello' }, { channel: 'chat' });
  client.connectPeer('bob').close();
  await connectLive({ pairUrl: '/api/pair', body: { id: 'alice' } });
  await watchHubs({ watchUrl: '/api/watch' });
  createHubApi({ client: hub, hubId: 'game', allowedOrigins: ['https://example.com'] });
  console.log(HUB_PROTOCOL, nativeVersion);
}
void example;
// @ts-expect-error user ids are strings
engine.upsertUser({ id: 1 });
// @ts-expect-error unknown delivery mode
hub.register({ id: 'game', channels: { chat: { delivery: 'instant' } } });
// @ts-expect-error a descriptor is required
new LiveClient({ ticket: 'x' });
// @ts-expect-error pairUrl is required
connectLive({});
// @ts-expect-error hubId is required
createHubApi({ client: hub });
