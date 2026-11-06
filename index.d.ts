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
  capacity: number; minMembers: number; members: string[]; free: number; seq: number; system: boolean;
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

export type HubMode = 'internal' | 'external' | 'central';
export interface LiveChannel { schemaRef?: string; delivery: 'reliable' | 'lossy'; maxRate?: number; }
/** Per-hub credential verifier checked before a ticket is issued. Secrets are write-only. */
export type HubAuth =
  | { kind: 'token'; tokens: { token: string; identity?: string; claims?: Record<string, unknown> }[] }
  | { kind: 'hmac'; secret: string; leewaySeconds?: number }
  | { kind: 'jwt'; alg: 'HS256'; secret: string; issuer?: string; audience?: string; identityClaim?: string; leewaySeconds?: number }
  | { kind: 'jwt'; alg: 'EdDSA'; publicKey: string; issuer?: string; audience?: string; identityClaim?: string; leewaySeconds?: number }
  | { kind: 'webhook'; url: string; secret?: string; timeoutMs?: number }
  | { kind: 'custom'; name: string };
export interface HubDefinition {
  id: string;
  mode?: HubMode;
  template?: string;
  ifAbsent?: boolean;
  address?: string;
  remoteHubId?: string;
  remoteToken?: string;
  scopes?: Record<string, unknown>;
  scopeSchema?: Record<string, unknown> | boolean;
  schemaRef?: string;
  userSchema?: Record<string, unknown> | boolean;
  serverSchema?: Record<string, unknown> | boolean;
  channels?: Record<string, LiveChannel>;
  auth?: HubAuth;
  /** Central hubs only: run on this service and return a hub-scoped credential once. */
  hosting?: 'provider';
  ttlSeconds?: number;
  maxHubs?: number;
  liveDepth?: number;
}
export interface HubSchema { id: string; version: string; schema: Record<string, unknown> | boolean; }
export type HubTemplate = Omit<HubDefinition, 'id' | 'template' | 'ifAbsent'> & { name: string; mode: HubMode };
export interface HubClientOptions {
  token: string;
  timeoutMs?: number;
  maxResponseBytes?: number;
  fetch?: typeof globalThis.fetch;
}
export interface HubPingOptions {
  /** Number of probes, 1–32. Default 5. */
  samples?: number;
  /** Delay between probes in milliseconds. Default 0. */
  intervalMs?: number;
}
export interface HubPing {
  protocol: typeof HUB_PROTOCOL;
  /** Individual round-trip times in milliseconds. */
  samples: number[];
  minMs: number;
  maxMs: number;
  avgMs: number;
  jitterMs: number;
  serverTimeMs?: number;
}
export declare const HUB_PROTOCOL: 'liveengine.hub/v1';
export declare class HubClient {
  constructor(address: string, options: HubClientOptions);
  readonly address: string;
  contract(): Promise<{ protocol: typeof HUB_PROTOCOL; [key: string]: unknown }>;
  ping(options?: HubPingOptions): Promise<HubPing>;
  tree(depth?: number): Promise<unknown>;
  route(path: string[], op: 'register' | 'list' | 'get' | 'delete' | 'call' | 'tree', body?: Record<string, unknown>): Promise<unknown>;
  listTemplates(): Promise<unknown>;
  createTemplate(template: HubTemplate): Promise<unknown>;
  getTemplate(name: string): Promise<unknown>;
  removeTemplate(name: string): Promise<unknown>;
  listSchemas(): Promise<unknown>;
  createSchema(schema: HubSchema): Promise<unknown>;
  getSchema(id: string): Promise<unknown>;
  removeSchema(id: string): Promise<unknown>;
  exportRegistry(): Promise<unknown>;
  list(): Promise<unknown>;
  register(hub: HubDefinition): Promise<unknown>;
  get(id: string): Promise<unknown>;
  remove(id: string): Promise<unknown>;
  call<T = unknown>(id: string, method: string, args?: unknown[], options?: { signal?: AbortSignal }): Promise<T>;
  importRegistry(registry: { address: string; token: string; prefix?: string; refreshSeconds?: number }): Promise<unknown>;
  /** Issue a short-lived, single-use live ticket for a user already paired in `serverId`. */
  ticket(id: string, options: TicketOptions): Promise<{ ticket: string; url: string; expiresAt: number }>;
  /** Issue a read-only registry watch ticket scoped to a path and depth. */
  watchTicket(options?: { path?: string[]; depth?: number }): Promise<{ ticket: string; expiresAt: number }>;
  /** Find hubs and peers that match `requirements`. */
  discover(options?: { kind?: 'hub' | 'peer' | 'any'; path?: string[]; depth?: number; requirements?: Record<string, unknown> }): Promise<{ candidates: DiscoveryCandidate[] }>;
  /** Pair a subject with a hub or peer and return the descriptor for `LiveClient`. */
  pair(request: PairRequest): Promise<LiveDescriptor>;
  /** A `renew` function for `LiveClient` that pairs again with `request` for a fresh ticket. */
  renewal(request: PairRequest): LiveRenew;
  /** Subscribe to the live registry. Resolves after the first snapshot. */
  watchRegistry(options?: WatchRegistryOptions): Promise<RegistryWatch>;
}
export interface TicketOptions {
  userId: string;
  serverId: string;
  role?: 'peer' | 'service';
  /** The user's credential for a hub that sets `auth`. */
  credential?: string;
  /** Central namespace that holds the hub. Default: the root. */
  path?: string[];
}
export interface DiscoveryCandidate { kind: 'hub' | 'peer'; id: string; path: string[]; [key: string]: unknown; }
export interface PairRequest {
  subject: { id: string; kind?: 'peer' | 'hub'; scopes?: Record<string, unknown>; credential?: string };
  /** `id` narrows the match to one named hub or peer, for hubs that share scopes. */
  target?: { kind: 'hub' | 'peer' | 'any'; id?: string; requirements?: Record<string, unknown> };
  path?: string[];
  /** The subject's credential for a hub that sets `auth`. Overrides `subject.credential`. */
  credential?: string;
}
/** What `pair` returns and `LiveClient` consumes. */
export interface LiveDescriptor {
  link: 'peer-peer' | 'peer-hub' | 'hub-hub';
  room: string;
  /** The id this subject has in the room. */
  userId: string;
  ticket: string;
  /** WebSocket URL of the hub that holds the room. */
  url: string;
  transports: Array<'webrtc' | 'websocket'>;
}
export type LiveRenew = (context?: { credential?: string; previous?: LiveDescriptor }) => Promise<LiveDescriptor>;
export interface WatchRegistryOptions {
  path?: string[];
  depth?: number;
  /** A watch ticket from `watchTicket`; without it the client's token authenticates the watch. */
  ticket?: string;
  timeoutMs?: number;
  WebSocket?: typeof WebSocket;
}
export interface RegistryMessage { type: 'snapshot' | 'added' | 'removed' | 'updated' | 'status'; seq: number; path: string[]; [key: string]: unknown; }
export declare class RegistryWatch extends EventEmitter {
  /** The latest snapshot message. */
  snapshot: RegistryMessage | null;
  seq: number;
  closed: boolean;
  resnapshot(): void;
  close(): void;
  on(event: RegistryMessage['type'] | 'event', listener: (message: RegistryMessage) => void): this;
  on(event: 'close', listener: () => void): this;
}

/** Options for `LiveClient`, imported from `@dixonsolutions/liveengine/live-client`. */
export interface LiveClientOptions {
  /** The user's credential, or a function that returns a fresh one, handed to `renew`. */
  credential?: string | (() => string | Promise<string>);
  /** Returns a fresh descriptor (a new ticket) when the first is spent. Run it where the administrator token lives. */
  renew?: LiveRenew;
  /** `"auto"` (default) connects directly only for peer-to-peer pairings; `true` forces, `false` disables. */
  direct?: 'auto' | boolean;
  /** Reconnect when the hub connection drops. Default true. */
  reconnect?: boolean;
  retries?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
  connectTimeoutMs?: number;
  directTimeoutMs?: number;
  /**
   * Send broadcasts over each open data channel and relay a targeted copy only to the peers
   * without one. Default false: a broadcast goes through the hub, which fans it out.
   */
  directBroadcast?: boolean;
  maxFrameBytes?: number;
  queueLimit?: number;
  WebSocket?: typeof WebSocket;
  RTCPeerConnection?: typeof RTCPeerConnection;
}
export interface LiveFrame {
  v: number;
  type: string;
  channel: string;
  from: string;
  to?: string;
  seq: number;
  payload: unknown;
  /** How the frame arrived. */
  via: 'relay' | 'direct';
}
export interface LiveSendOptions { to?: string; channel?: string; type?: string; }
export interface LiveClientEvents {
  open: { reconnected: boolean };
  welcome: { peers: string[]; iceServers: unknown[]; identity: unknown };
  message: LiveFrame;
  presence: { event: 'join' | 'leave'; peer: string; identity?: unknown };
  transport: { peer: string; transport: 'direct' | 'relay'; reason?: string };
  'direct-failed': { peer: string; reason: string };
  reconnecting: { attempt: number; delayMs: number; error: Error };
  disconnected: { reconnecting: boolean };
  close: { reason: string; reconnecting: boolean; error?: Error };
  error: Error;
}
export interface PeerLink {
  readonly peer: string;
  readonly polite: boolean;
  readonly transport: 'direct' | 'relay';
  readonly closed: boolean;
  readonly failure: string | null;
  /** Resolves with the first transport that settles: "direct" once the channel opens, else "relay". */
  readonly ready: Promise<'direct' | 'relay'>;
  send(frame: object): boolean;
  close(): void;
}
/** Connection to a hub room with relay, direct WebRTC and reconnect handled for the caller. */
export interface LiveClient {
  readonly id: string;
  readonly room: string;
  readonly state: 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';
  readonly iceServers: unknown[];
  readonly identity: unknown;
  on<E extends keyof LiveClientEvents>(event: E, listener: (data: LiveClientEvents[E]) => void): () => void;
  off<E extends keyof LiveClientEvents>(event: E, listener: (data: LiveClientEvents[E]) => void): this;
  once<E extends keyof LiveClientEvents>(event: E, listener: (data: LiveClientEvents[E]) => void): () => void;
  peers(): string[];
  identityOf(peerId: string): unknown;
  transportOf(peerId: string): 'direct' | 'relay' | null;
  connect(): Promise<this>;
  reconnect(): Promise<this>;
  connectPeer(peerId: string, options?: ConnectPeerOptions): PeerLink;
  send(payload: unknown, options?: LiveSendOptions): boolean;
  close(): void;
}
export interface ConnectPeerOptions {
  RTCPeerConnection?: typeof RTCPeerConnection;
  iceServers?: unknown[];
  directTimeoutMs?: number;
}
