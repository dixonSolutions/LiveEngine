import { NextResponse } from 'next/server';
import { liveEngine } from '../../../../lib/liveengine';

export const runtime = 'nodejs';

export async function POST(request: Request) {
  const body = await request.json().catch(() => null);
  const userId = body?.userId;
  if (typeof userId !== 'string' || !userId) {
    return NextResponse.json({ error: 'userId is required' }, { status: 400 });
  }
  try {
    const { client, hubId } = liveEngine();
    await client.call(hubId, 'upsertUser', [{
      id: userId,
      scopes: body.scopes ?? {},
      requirements: body.requirements,
    }]);
    const result = await client.call<{ ok: boolean; serverId: string | null; reason: string | null }>(
      hubId,
      'autoJoin',
      [userId],
    );
    if (!result.ok) return NextResponse.json(result, { status: 409 });
    const server = await client.call(hubId, 'getServer', [result.serverId]);
    return NextResponse.json({ ...result, server });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 502 });
  }
}
