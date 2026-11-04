import { liveEngine } from '../../../utils/liveengine';

export default defineEventHandler(async () => {
  const { client, hubId } = liveEngine();
  const [servers, stats] = await Promise.all([
    client.call(hubId, 'listServers'),
    client.call(hubId, 'stats'),
  ]);
  return { hubId, servers, stats };
});
