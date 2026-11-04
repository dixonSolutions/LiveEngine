import { liveEngine } from '../../../utils/liveengine';

export default defineEventHandler(async () => {
  const { client } = liveEngine();
  return client.ping({ samples: 5 });
});
