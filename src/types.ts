import { WebSocket } from "ws";

export interface Message {
    type: "MESSAGE";
    messageId: string;
    senderDeviceId: string;
    receiverDeviceId: string; // Target device ID, or "*" for broadcast
    payload: string;
    timestamp: number;
    ttl: number;              // Time-To-Live (hop count limit)
    hopCount: number;         // Number of times relayed
    routePath?: string[];     // Trace of device IDs that forwarded the message
}

export interface IdentifyMessage {
    type: "IDENTIFY";
    deviceId: string;
}

export interface DiscoveredPeer {
    deviceId: string;
    ip: string;
    port: number;
}

export interface Peer {
    id: string;
    socket: WebSocket;
}