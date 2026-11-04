import { Body, Controller, Get, HttpException, HttpStatus, Post } from '@nestjs/common';
import { LiveEngineService } from './liveengine.service';

interface ServerBody {
  id: string;
  capacity?: number;
  minMembers?: number;
  scopes?: Record<string, unknown>;
  requirements?: unknown;
}

interface MatchBody {
  userId: string;
  scopes?: Record<string, unknown>;
  requirements?: unknown;
}

@Controller('api')
export class MatchController {
  constructor(private readonly liveEngine: LiveEngineService) {}

  @Post('servers')
  upsertServer(@Body() body: ServerBody) {
    if (!body?.id) throw new HttpException('id is required', HttpStatus.BAD_REQUEST);
    const created = this.liveEngine.engine.upsertServer(body);
    return { id: body.id, created };
  }

  @Post('match')
  match(@Body() body: MatchBody) {
    if (!body?.userId) throw new HttpException('userId is required', HttpStatus.BAD_REQUEST);
    this.liveEngine.engine.upsertUser(body);
    const result = this.liveEngine.engine.pair(body.userId);
    if (!result.ok) throw new HttpException(result, HttpStatus.CONFLICT);
    return result;
  }

  @Get('stats')
  stats() {
    return this.liveEngine.engine.stats();
  }
}
