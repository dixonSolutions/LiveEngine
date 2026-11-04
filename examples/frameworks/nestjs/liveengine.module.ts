import { Module } from '@nestjs/common';
import { LiveEngineService } from './liveengine.service';
import { MatchController } from './match.controller';

@Module({
  providers: [LiveEngineService],
  controllers: [MatchController],
  exports: [LiveEngineService],
})
export class LiveEngineModule {}
