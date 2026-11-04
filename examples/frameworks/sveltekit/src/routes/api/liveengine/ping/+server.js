import { json } from '@sveltejs/kit';
import { liveEngine } from '$lib/server/liveengine';

export async function GET() {
  const { client } = liveEngine();
  return json(await client.ping({ samples: 5 }));
}
