import type { ConnectPeerOptions, LiveClient as LiveClientType, LiveClientOptions, LiveDescriptor, PeerLink } from '../index';
export type { ConnectPeerOptions, LiveClientEvents, LiveClientOptions, LiveDescriptor, LiveFrame, LiveRenew, LiveSendOptions, PeerLink } from '../index';
export declare const LiveClient: {
  new (descriptor: LiveDescriptor, options?: LiveClientOptions): LiveClientType;
};
export type LiveClient = LiveClientType;
export declare function connectPeer(client: LiveClientType, peerId: string, options?: ConnectPeerOptions): PeerLink;
export default LiveClient;
