import { NextResponse } from 'next/server';
import { liveEngine } from '../../../../lib/liveengine';

export const runtime = 'nodejs';

export async function GET() {
  try {
    const { client } = liveEngine();
    return NextResponse.json(await client.ping({ samples: 5 }));
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 502 });
  }
}
