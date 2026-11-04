import { env } from '$env/dynamic/private';
import { HubClient } from '@dixonsolutions/liveengine/hubs';

let client;

export function liveEngine() {
  if (!env.LIVEENGINE_URL || !env.LIVEENGINE_TOKEN) {
    throw new Error('Set LIVEENGINE_URL and LIVEENGINE_TOKEN');
  }
  client ??= new HubClient(env.LIVEENGINE_URL, {
    token: env.LIVEENGINE_TOKEN,
    timeoutMs: 10_000,
  });
  return {
    client,
    hubId: env.LIVEENGINE_HUB_ID || 'ranked',
  };
}
