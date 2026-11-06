import type { IncomingMessage, ServerResponse } from 'node:http';
import type { HubClient, HubSchema } from '../index';

export interface HubApiLimits {
  /** Largest request body in bytes. Default 16384. */
  bodyBytes?: number;
  /** POST requests per client address per minute. Default 10. */
  requestsPerMinute?: number;
  /** GET requests (status, ping) per client address per minute. Default 300. */
  readsPerMinute?: number;
  /** A seat with no heartbeat for this long is released. Default 180000. */
  idleMs?: number;
  /** Most players the API tracks at once. Default 4096. */
  maxPlayers?: number;
}

export interface HubApiPeerLink {
  /** `"hub"` pairs every player with the hub's shared room. `"peer"` pairs players with each other. Default `"hub"`. */
  kind?: 'hub' | 'peer';
  /** How long a peer waits for a partner when the page does not say. Default 30000. */
  waitMs?: number;
  /** The most a page may ask to wait. Default 120000. */
  maxWaitMs?: number;
}

export interface HubApiOptions {
  /** Holds the administrator token. Omit it, with `notConfigured`, to answer 503 until configured. */
  client?: HubClient;
  /** The hub this API fronts. It is created on first use when the service has no such hub. */
  hubId: string;
  /** Slash-free ids of the central namespaces that hold the hub. Default: the root registry. */
  hubPath?: string[];
  mode?: 'internal' | 'external';
  /** The remote service for `mode: "external"`. */
  remote?: { address: string; hubId?: string; token: string };
  /** The hub's own scopes, and what a hub pairing requires. */
  scopes?: Record<string, unknown>;
  /** Schemas published once; an existing version is left alone. */
  schemas?: HubSchema[];
  /** The hub's channel table. */
  channels?: Record<string, unknown>;
  /** The hub's `auth` verifier. Unset leaves the hub open. */
  auth?: Record<string, unknown>;
  peerLink?: HubApiPeerLink;
  /** Pages allowed to call the API: a list of origins, or a predicate. A POST without an allowed Origin is refused. */
  allowedOrigins?: Iterable<string> | ((origin: string) => boolean);
  /** When set, a request with another Host header is refused (DNS rebinding). */
  allowedHosts?: Iterable<string>;
  /** Path the handler answers under when no router strips it. Default: none. */
  basePath?: string;
  limits?: HubApiLimits;
  /** Maps a request body to the scopes the player pairs with. Default: the hub's `scopes`. */
  subject?: (input: Record<string, unknown>) => Record<string, unknown>;
  /** Enables the seat-based `join` route: `server` is passed to `upsertServer`, `extra` is added to the answer. */
  join?: { server: Record<string, unknown>; extra?: Record<string, unknown> };
  /** Added to the `status` answer. */
  statusExtra?: Record<string, unknown>;
  /** Where the page opens the registry socket. Default: the hub's own address. */
  watchUrl?: string;
  /** The 503 message when there is no client. */
  notConfigured?: string;
}

export interface HubApi {
  (req: IncomingMessage, res: ServerResponse, next?: (error?: unknown) => void): Promise<void> | void;
  /** Publish schemas, channels and the hub now, and start the idle reaper. */
  setup(): Promise<void>;
  /** Stop the idle reaper. */
  close(): void;
  handle: HubApi;
  stats(): { seats: number; waiting: number; pairings: number };
}

/** The routes a page needs to connect to a hosted hub: status, ping, discover, pair, renew, leave, heartbeat, watch, live-client.mjs. */
export declare function createHubApi(options: HubApiOptions): HubApi;
/** Drop idle peers and old timestamps from a `Map<peer, number[]>`. */
export declare function pruneRates(rates: Map<string, number[]>, now: number, windowMs: number): void;
export default createHubApi;
