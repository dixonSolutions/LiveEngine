import { liveEngine } from '../../../utils/liveengine';

interface MatchBody {
  userId: string;
  scopes?: Record<string, unknown>;
  requirements?: unknown;
}

export default defineEventHandler(async event => {
  const body = await readBody<MatchBody>(event);
  if (!body?.userId) {
    throw createError({ statusCode: 400, statusMessage: 'userId is required' });
  }
  const { client, hubId } = liveEngine();
  await client.call(hubId, 'upsertUser', [{
    id: body.userId,
    scopes: body.scopes ?? {},
    requirements: body.requirements,
  }]);
  const result = await client.call<{ ok: boolean; serverId: string | null; reason: string | null }>(
    hubId,
    'autoJoin',
    [body.userId],
  );
  if (!result.ok) {
    throw createError({ statusCode: 409, statusMessage: result.reason || 'no_fit', data: result });
  }
  const server = await client.call(hubId, 'getServer', [result.serverId]);
  return { ...result, server };
});
