import { EventEmitter } from 'node:events';

export type ScopeValue = number | boolean | string | string[] | null;
export type Scopes = Record<string, ScopeValue>;

export type Op =
  | 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte'
  | 'in' | 'nin' | 'between'
  | 'exists' | 'missing'
  | 'contains' | 'notContains' | 'overlaps';

export interface Rule { key: string; op?: Op; value?: ScopeValue | ScopeValue[]; }
export interface RuleSet { rules: Rule[]; match?: 'all' | 'any'; }
/** Shorthand: `{ region: 'eu', rating: { between: [1300, 1700] }, tags: { contains: 'ranked' } }` */
export type RequirementShorthand = Record<string, ScopeValue | ScopeValue[] | Partial<Record<Op, ScopeValue | ScopeValue[]>>>;
export type Requirements = Rule[] | RuleSet | RequirementShorthand;

export type UserStatus = 'waiting' | 'paired' | 'idle';
export type ServerStatus = 'open' | 'full' | 'closed';
export type Strategy = 'first' | 'random' | 'leastLoaded' | 'mostLoaded' | 'affinity';

export interface UserInput { id: string; scopes?: Scopes; requirements?: Requirements; status?: UserStatus; }
export interface User { id: string; scopes: Scopes; requirements: RuleSet; status: UserStatus; serverId: string | null; seq: number; }

export interface ServerInput {
  id: string; scopes?: Scopes; requirements?: Requirements;
  capacity?: number; minMembers?: number; status?: ServerStatus;
}
export interface Server {
  id: string; scopes: Scopes; requirements: RuleSet; status: ServerStatus;
  capacity: number; minMembers: number; members: string[]; free: number; seq: number;
}

export interface Affinity { key: string; weight?: number; }
export interface MatchOptions {
  /** 0 = unlimited. Default 10. */
  limit?: number;
  strategy?: Strategy;
  /** Numeric closeness keys; setting this without a strategy selects 'affinity'. */
  affinity?: Array<string | Affinity> | Record<string, number>;
}

export interface Match { id: string; score: number; load: number; }
export interface Result { ok: boolean; reason: string | null; userId: string | null; serverId: string | null; }
export interface PairResult extends Result { server?: Server; }

export type EventType =
  | 'user_added' | 'user_updated' | 'user_removed'
  | 'server_added' | 'server_updated' | 'server_removed'
  | 'server_available' | 'server_full' | 'server_unavailable' | 'server_ready'
  | 'advertised' | 'paired' | 'unpaired' | 'cleared';

export interface EngineEvent {
  type: EventType; seq: number;
  userId?: string; serverId?: string; ids?: string[]; reason?: string;
}

export type ScopeGenerator =
  | string[]
  | { type: 'int' | 'float'; min?: number; max?: number }
  | { type: 'choice'; values: string[]; weights?: number[] }
  | { type: 'bool'; p?: number; probability?: number }
  | { type: 'string'; prefix?: string }
  | { type: 'tags'; values: string[]; count?: number };

export interface GenerationSpec {
  idPrefix?: string;
  scopes?: Record<string, ScopeGenerator>;
  requirements?: Requirements;
}

export interface Stats {
  users: number; waiting: number; paired: number; idle: number;
  servers: number; open: number; full: number; closed: number;
  pendingEvents: number; totalJoins: number; totalLeaves: number; totalMatchesEvaluated: number;
}

export interface LiveEngineOptions { seed?: number; autoEmit?: boolean; }

export declare class NativeEngine {
  constructor(seed?: number);
  upsertUser(user: UserInput): boolean;
  upsertUsers(users: UserInput[]): number;
  removeUser(id: string): boolean;
  getUser(id: string): User | null;
  listUsers(status?: UserStatus): User[];
  setUserStatus(id: string, status: UserStatus): boolean;
  upsertServer(server: ServerInput): boolean;
  upsertServers(servers: ServerInput[]): number;
  removeServer(id: string): boolean;
  getServer(id: string): Server | null;
  listServers(status?: ServerStatus): Server[];
  setServerStatus(id: string, status: ServerStatus): boolean;
  findServersForUser(userId: string, options?: MatchOptions): Match[];
  findUsersForServer(serverId: string, options?: MatchOptions): Match[];
  fits(userId: string, serverId: string): boolean;
  advertise(userId: string, options?: MatchOptions): number;
  advertiseAll(options?: MatchOptions): number;
  join(userId: string, serverId: string): Result;
  leave(userId: string): Result;
  autoJoin(userId: string, options?: MatchOptions): Result;
  matchAll(options?: MatchOptions): Result[];
  generateUsers(count: number, spec?: GenerationSpec): User[];
  drainEvents(): EngineEvent[];
  pendingEvents(): number;
  stats(): Stats;
  clear(): void;
  reseed(seed: number): void;
}

export declare class LiveEngine extends EventEmitter {
  constructor(options?: LiveEngineOptions | number);
  autoEmit: boolean;

  upsertUser(user: UserInput): boolean;
  upsertUsers(users: UserInput[]): number;
  removeUser(id: string): boolean;
  getUser(id: string): User | null;
  listUsers(status?: UserStatus): User[];
  setUserStatus(id: string, status: UserStatus): boolean;
  upsertServer(server: ServerInput): boolean;
  upsertServers(servers: ServerInput[]): number;
  removeServer(id: string): boolean;
  getServer(id: string): Server | null;
  listServers(status?: ServerStatus): Server[];
  setServerStatus(id: string, status: ServerStatus): boolean;
  findServersForUser(userId: string, options?: MatchOptions): Match[];
  findUsersForServer(serverId: string, options?: MatchOptions): Match[];
  fits(userId: string, serverId: string): boolean;
  advertise(userId: string, options?: MatchOptions): number;
  advertiseAll(options?: MatchOptions): number;
  join(userId: string, serverId: string): Result;
  leave(userId: string): Result;
  autoJoin(userId: string, options?: MatchOptions): Result;
  matchAll(options?: MatchOptions): Result[];
  generateUsers(count: number, spec?: GenerationSpec): User[];
  drainEvents(): EngineEvent[];
  pendingEvents(): number;
  stats(): Stats;
  clear(): void;
  reseed(seed: number): void;

  joinHub(user: UserInput, options?: MatchOptions): { created: boolean; advertised: number };
  pair(userId: string, options?: MatchOptions): PairResult;
  startAutoMatch(intervalMs?: number, options?: MatchOptions): this;
  stopAutoMatch(): this;

  on(event: EventType | 'event', listener: (ev: EngineEvent) => void): this;
  once(event: EventType | 'event', listener: (ev: EngineEvent) => void): this;
  off(event: EventType | 'event', listener: (ev: EngineEvent) => void): this;
}

export declare const nativeVersion: string;
export default LiveEngine;
