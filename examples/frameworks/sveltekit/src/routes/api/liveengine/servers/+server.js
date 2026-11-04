import { json } from '@sveltejs/kit';
import { liveEngine } from '$lib/server/liveengine';

export async function GET() {
  const { client, hubId } = liveEngine();
  const [servers, stats] = await Promise.all([
    client.call(hubId, 'listServers'),
    client.call(hubId, 'stats'),
  ]);
  return json({ hubId, servers, stats });
}
