import { Injectable } from '@nestjs/common';
import { LiveEngine } from '@dixonsolutions/liveengine';

@Injectable()
export class LiveEngineService {
  readonly engine = new LiveEngine({
    seed: Number(process.env.LIVEENGINE_SEED ?? 0),
  });
}
