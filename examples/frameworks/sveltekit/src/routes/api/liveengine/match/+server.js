import { json } from '@sveltejs/kit';
import { liveEngine } from '$lib/server/liveengine';

export async function POST({ request }) {
  const body = await request.json().catch(() => null);
  if (!body?.userId) return json({ error: 'userId is required' }, { status: 400 });

  const { client, hubId } = liveEngine();
  await client.call(hubId, 'upsertUser', [{
    id: body.userId,
    scopes: body.scopes ?? {},
    requirements: body.requirements,
  }]);
  const result = await client.call(hubId, 'autoJoin', [body.userId]);
  if (!result.ok) return json(result, { status: 409 });
  const server = await client.call(hubId, 'getServer', [result.serverId]);
  return json({ ...result, server });
}
