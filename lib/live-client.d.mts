import type { ConnectPeerOptions, LiveClient as LiveClientType, LiveClientOptions, LiveDescriptor, PeerLink } from '../index';
export type { ConnectPeerOptions, LiveClientEvents, LiveClientOptions, LiveDescriptor, LiveFrame, LiveRenew, LiveSendOptions, PeerLink } from '../index';
export declare const LiveClient: {
  new (descriptor: LiveDescriptor, options?: LiveClientOptions): LiveClientType;
};
export type LiveClient = LiveClientType;
export declare function connectPeer(client: LiveClientType, peerId: string, options?: ConnectPeerOptions): PeerLink;
export interface ConnectLiveOptions extends LiveClientOptions {
  pairUrl: string;
  /** Posted for a fresh descriptor on reconnect. Defaults to `pairUrl`. */
  renewUrl?: string;
  /** The request body `pairUrl` and `renewUrl` receive. */
  body?: Record<string, unknown>;
  fetch?: typeof fetch;
}
/** Pair through the application's own routes, connect, and resolve with the open client. */
export declare function connectLive(options: ConnectLiveOptions): Promise<LiveClientType>;
export interface WatchedHub { id: string; mode: string; status: string; roomCount: number; memberCount: number; path: string[]; childrenLive?: boolean }
export interface HubWatch {
  hubs(): WatchedHub[];
  readonly closed: boolean;
  on(event: 'change', listener: (hubs: WatchedHub[]) => void): () => void;
  on(event: 'close', listener: (info: { reason: string }) => void): () => void;
  close(): void;
}
export interface WatchHubsOptions {
  /** The application's route that issues a watch ticket and answers `{ url, ticket, path, depth }`. */
  watchUrl: string;
  body?: Record<string, unknown>;
  fetch?: typeof fetch;
  WebSocket?: typeof WebSocket;
  timeoutMs?: number;
}
/** Watch the live hub list with a ticket from the application's own route. Resolves after the first snapshot. */
export declare function watchHubs(options: WatchHubsOptions): Promise<HubWatch>;
export default LiveClient;
