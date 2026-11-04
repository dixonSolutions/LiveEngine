import { HubClient } from '@dixonsolutions/liveengine/hubs';

let client: HubClient | undefined;

export function liveEngine() {
  const url = process.env.LIVEENGINE_URL;
  const token = process.env.LIVEENGINE_TOKEN;
  if (!url || !token) {
    throw new Error('Set LIVEENGINE_URL and LIVEENGINE_TOKEN');
  }
  client ??= new HubClient(url, { token, timeoutMs: 10_000 });
  return {
    client,
    hubId: process.env.LIVEENGINE_HUB_ID || 'ranked',
  };
}
