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

// Phase 4: Route Discovery Protocol (AODV-inspired)
export interface RouteRequest {
    type: "RREQ";
    requestId: string;
    originDeviceId: string;     // Node looking for the route
    targetDeviceId: string;     // Destination node needed
    hopCount: number;
    ttl: number;
    routePath: string[];
}

export interface RouteReply {
    type: "RREP";
    requestId: string;
    originDeviceId: string;     // Node waiting for the route
    targetDeviceId: string;     // Destination node found
    hopCount: number;
    ttl: number;
    routePath: string[];        // Full path discovered
}

// Phase 4 (Step 2): Route Error Protocol (RERR) for Link Failures & Self-Healing
export interface RouteError {
    type: "RERR";
    errorId: string;
    reporterDeviceId: string;       // Node that detected the broken link
    unreachableDestinations: string[]; // List of destinations no longer reachable
    ttl: number;
    hopCount: number;
    routePath: string[];
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

export type MeshPacket = Message | AckMessage | RouteRequest | RouteReply | RouteError | IdentifyMessage | HeartbeatMessage;

export interface RouteEntry {
    destination: string;        // Destination device ID
    nextHop: string;            // Direct neighbor to forward to
    hopDistance: number;        // Number of hops to destination
    expiresAt: number;          // Timestamp when entry expires
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