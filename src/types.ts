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

export interface AckMessage {
    type: "ACK";
    ackMessageId: string;     // Target messageId being acknowledged
    senderDeviceId: string;   // Device that received the original message
    receiverDeviceId: string; // Original sender waiting for acknowledgment
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath?: string[];
}

export interface IdentifyMessage {
    type: "IDENTIFY";
    deviceId: string;
}

export interface HeartbeatMessage {
    type: "PING" | "PONG";
    deviceId: string;
    timestamp: number;
}

export type MeshPacket = Message | AckMessage | IdentifyMessage | HeartbeatMessage;

export interface DiscoveredPeer {
    deviceId: string;
    ip: string;
    port: number;
}

export interface Peer {
    id: string;
    socket: WebSocket;
}