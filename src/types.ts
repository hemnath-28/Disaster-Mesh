import { WebSocket } from "ws";

// ==================== General Mesh & Routing Types ====================
export interface Message {
    type: "MESSAGE";
    messageId: string;
    senderDeviceId: string;
    receiverDeviceId: string;
    payload: string;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath?: string[];
    attempt?: number;
}

export interface AckMessage {
    type: "ACK";
    ackMessageId: string;
    senderDeviceId: string;
    receiverDeviceId: string;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath?: string[];
    attempt?: number;
}

export interface RouteRequest {
    type: "RREQ";
    requestId: string;
    originDeviceId: string;
    targetDeviceId: string;
    hopCount: number;
    ttl: number;
    routePath: string[];
    originSeqNum?: number;
    destSeqNum?: number;
}

export interface RouteReply {
    type: "RREP";
    requestId: string;
    originDeviceId: string;
    targetDeviceId: string;
    hopCount: number;
    ttl: number;
    routePath: string[];
    destSeqNum?: number;
}

export interface RouteError {
    type: "RERR";
    errorId: string;
    reporterDeviceId: string;
    unreachableDestinations: string[];
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

// ==================== Phase 6: Emergency & Mesh Hop Proximity Types ====================

/**
 * Mesh Hop Proximity Location:
 * Determines victim localization via topology tracing through anchor relays.
 */
export interface MeshProximityLocation {
    landmark?: string;
    // Anchor node closest to the victim that has known coordinates or post designation
    anchorDeviceId?: string;
    // Number of wireless hops from the anchor node
    hopsFromAnchor: number;
    // Estimated search radius in meters based on wireless radio hop distance (~75m per hop)
    estimatedRadiusMeters: number;
    // Full hop chain trace from victim to rescue base
    proximityChain: string[];
}

export type EmergencyType = "TRAPPED" | "MEDICAL" | "SUPPLIES" | "EVACUATION";
export type EmergencySeverity = "CRITICAL" | "HIGH" | "MODERATE";
export type EmergencyStatus = "OPEN" | "STANDBY" | "DISPATCHED" | "RESOLVED";

export interface SosAlert {
    type: "SOS";
    sosId: string;
    senderDeviceId: string;
    emergencyType: EmergencyType;
    severity: EmergencySeverity;
    peopleCount: number;
    description: string;
    location: MeshProximityLocation;
    status: EmergencyStatus;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath: string[];
}

export interface RescueAck {
    type: "RESCUE_ACK";
    sosId: string;
    rescuerDeviceId: string;
    victimDeviceId: string;
    status: "DISPATCHED" | "STANDBY" | "RESOLVED";
    estimatedArrivalTime?: string;
    instructions: string;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath: string[];
}

export type HazardType = "FLOOD_SURGE" | "COLLAPSE_RISK" | "SAFE_ZONE" | "AIRDROP";

export interface DisasterBulletin {
    type: "BULLETIN";
    bulletinId: string;
    senderDeviceId: string;
    hazardType: HazardType;
    announcement: string;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePath: string[];
}

export type MeshPacket =
    | Message
    | AckMessage
    | RouteRequest
    | RouteReply
    | RouteError
    | IdentifyMessage
    | HeartbeatMessage
    | SosAlert
    | RescueAck
    | DisasterBulletin;

export interface RouteEntry {
    destination: string;
    nextHop: string;
    hopDistance: number;
    expiresAt: number;
    destSeqNum?: number;
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