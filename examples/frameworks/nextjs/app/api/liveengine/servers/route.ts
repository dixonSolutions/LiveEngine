import { NextResponse } from 'next/server';
import { liveEngine } from '../../../../lib/liveengine';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const { client, hubId } = liveEngine();
    const [servers, stats] = await Promise.all([
      client.call(hubId, 'listServers'),
      client.call(hubId, 'stats'),
    ]);
    return NextResponse.json({ hubId, servers, stats });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 502 });
  }
}
