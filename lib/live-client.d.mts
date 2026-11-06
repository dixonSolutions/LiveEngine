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
export default LiveClient;
