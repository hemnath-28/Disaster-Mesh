import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";
import readline from "readline";
import { EventEmitter } from "events";
import {
    Message,
    AckMessage,
    RouteRequest,
    RouteReply,
    RouteError,
    HeartbeatMessage,
    IdentifyMessage,
    MeshPacket,
    RouteEntry,
    DiscoveredPeer,
    SosAlert,
    RescueAck,
    DisasterBulletin,
    EmergencyType,
    EmergencySeverity,
    HazardType
} from "./types.js";
import { PeerDiscovery } from "./discovery.js";
import { MessageStore } from "./storage.js";
import { startDashboardServer, stopDashboardServer } from "./dashboard.js";

// Configuration via environment or arguments
const HOST = process.env.HOST || "0.0.0.0";
const PORT = parseInt(process.env.PORT || process.argv[2] || "5000", 10);
const DEVICE_ID = process.env.DEVICE_ID || process.argv[3] || `Node-${randomUUID().substring(0, 5)}`;
const MANUAL_PEER = process.env.PEER_URL || process.argv[4] || "";
const ENABLE_MDNS = process.env.ENABLE_MDNS !== "false";
const IS_RESCUE_BASE = process.env.IS_RESCUE_BASE === "true" || process.argv.includes("--rescue");
const DASHBOARD_PORT = parseInt(process.env.DASHBOARD_PORT || (PORT + 1000 === 6000 ? "6003" : (PORT + 1000).toString()), 10);
const STORAGE_DIR = process.env.STORAGE_DIR || "./data";

// Decoupled internal event emitter for UI & operational telemetry
export const meshEvents = new EventEmitter();

console.log("=========================================");
console.log(` 🚨 DisasterMesh Crisis Node: ${DEVICE_ID}`);
console.log(` Role:               ${IS_RESCUE_BASE ? "RESCUE BASE / OPS COMMAND" : "FIELD / VICTIM NODE"}`);
console.log(` WebSocket Port:     ${PORT}`);
if (IS_RESCUE_BASE) {
    console.log(` Web Dashboard:      http://${HOST === "0.0.0.0" ? "localhost" : HOST}:${DASHBOARD_PORT}`);
}
console.log(` mDNS Auto-Discovery:${ENABLE_MDNS ? "ENABLED" : "DISABLED"}`);
console.log(` Storage (SQLite):    ENABLED (data/store-${DEVICE_ID}.db)`);
console.log("=========================================");

// Phase 5 & 6: Persistent Storage
export const storage = new MessageStore(DEVICE_ID, STORAGE_DIR);

// Active connections: deviceId -> WebSocket
const peers = new Map<string, WebSocket>();
const knownPeerUrls = new Set<string>();
let discoveryService: PeerDiscovery | null = null;

// Routing Table
const routingTable = new Map<string, RouteEntry>();
const ROUTE_TTL_MS = 5 * 60 * 1000;

export function getActivePeers(): string[] {
    return Array.from(peers.keys());
}

export function getRoutingTable(): RouteEntry[] {
    const now = Date.now();
    for (const [dest, entry] of routingTable.entries()) {
        if (now > entry.expiresAt || (!peers.has(dest) && !peers.has(entry.nextHop))) {
            routingTable.delete(dest);
        }
    }
    return Array.from(routingTable.values());
}

// Active route discovery tracking
const pendingRouteRequests = new Set<string>();

// Pending ACKs
interface PendingAck {
    message: Message;
    sentAt: number;
    timer: NodeJS.Timeout;
}
const pendingAcks = new Map<string, PendingAck>();
const ACK_TIMEOUT_MS = 6000;

// Deduplication cache
const seenPackets = new Map<string, number>();
const SEEN_CACHE_TTL_MS = 2 * 60 * 1000;

// Metrics
const stats = {
    originatedMessages: 0,
    deliveredMessages: 0,
    relayedMessages: 0,
    directedForwarded: 0,
    floodedForwarded: 0,
    sosOriginated: 0,
    sosRelayed: 0,
    sosReceived: 0,
    rescueAcksSent: 0,
    rescueAcksReceived: 0,
    bulletinsSent: 0,
    bulletinsReceived: 0,
    droppedDuplicates: 0,
    droppedExpiredTtl: 0,
    droppedLoops: 0
};

export function getStats(): typeof stats {
    return { ...stats };
}

function isDuplicate(packetKey: string): boolean {
    const now = Date.now();
    if (seenPackets.size > 1000) {
        for (const [id, expiry] of seenPackets.entries()) {
            if (now > expiry) {
                seenPackets.delete(id);
            }
        }
    }

    if (seenPackets.has(packetKey)) {
        stats.droppedDuplicates++;
        console.log(`[DEDUPE] Suppressed duplicate packet: ${packetKey}`);
        return true;
    }

    seenPackets.set(packetKey, now + SEEN_CACHE_TTL_MS);
    return false;
}

function updateRoute(destination: string, nextHop: string, hopDistance: number): void {
    const existing = routingTable.get(destination);
    if (!existing || hopDistance < existing.hopDistance || Date.now() > existing.expiresAt) {
        routingTable.set(destination, {
            destination,
            nextHop,
            hopDistance,
            expiresAt: Date.now() + ROUTE_TTL_MS
        });
        checkAndFlushPending(destination);
        meshEvents.emit("routes:change", getRoutingTable());
    }
}

function getValidRoute(destination: string): RouteEntry | undefined {
    if (peers.has(destination)) {
        return {
            destination,
            nextHop: destination,
            hopDistance: 1,
            expiresAt: Date.now() + ROUTE_TTL_MS
        };
    }

    const route = routingTable.get(destination);
    if (!route) return undefined;

    if (Date.now() > route.expiresAt || !peers.has(route.nextHop)) {
        routingTable.delete(destination);
        return undefined;
    }

    return route;
}

const DEFAULT_SEARCH_TTL = parseInt(process.env.DEFAULT_SEARCH_TTL || "15", 10);
const TTL_SAFETY_MARGIN = 3;

export function getAdaptiveTtl(destination: string, fallbackTtl: number = DEFAULT_SEARCH_TTL): number {
    const route = getValidRoute(destination);
    if (route) {
        return Math.max(route.hopDistance + TTL_SAFETY_MARGIN, 4);
    }
    return fallbackTtl;
}

export function checkAndFlushPending(destinationDeviceId: string): void {
    const pendingMessages = storage.getPendingMessagesFor(destinationDeviceId);
    if (pendingMessages.length === 0) return;

    const route = getValidRoute(destinationDeviceId);
    if (!route) {
        if (!pendingRouteRequests.has(destinationDeviceId)) {
            pendingRouteRequests.add(destinationDeviceId);
            setTimeout(() => pendingRouteRequests.delete(destinationDeviceId), 3000);
            broadcastPacket({
                type: "RREQ",
                requestId: randomUUID(),
                originDeviceId: DEVICE_ID,
                targetDeviceId: destinationDeviceId,
                hopCount: 0,
                ttl: DEFAULT_SEARCH_TTL,
                routePath: [DEVICE_ID]
            });
        }
        return;
    }

    for (const msg of pendingMessages) {
        if (msg.ttl === DEFAULT_SEARCH_TTL) {
            msg.ttl = getAdaptiveTtl(destinationDeviceId);
            console.log(`[ROUTE-AWARE] Route resolved for ${destinationDeviceId} (distance: ${route.hopDistance}) -> Adaptive TTL dynamically set to ${msg.ttl}`);
        }
        storage.markInTransit(msg.messageId);
        stats.directedForwarded++;
        const sent = sendDirected(route.nextHop, msg);
        if (sent) {
            registerPendingAck(msg);
        }
    }
}

export function syncCrisisStateToPeer(socket: WebSocket, targetDeviceId?: string): void {
    if (socket.readyState !== WebSocket.OPEN) return;

    // 1. Sync active open SOS distress beacons from SQLite
    const openAlerts = storage.getAllSosAlerts().filter((a) => a.status === "OPEN");
    for (const alert of openAlerts) {
        const chain: string[] = JSON.parse(alert.proximityChainJson || "[]");
        if (targetDeviceId && chain.includes(targetDeviceId)) continue;

        const sosPacket: SosAlert = {
            type: "SOS",
            sosId: alert.sosId,
            senderDeviceId: alert.senderDeviceId,
            emergencyType: alert.emergencyType as EmergencyType,
            severity: alert.severity as EmergencySeverity,
            peopleCount: alert.peopleCount,
            description: alert.description,
            location: {
                landmark: alert.landmark,
                anchorDeviceId: alert.anchorDeviceId,
                hopsFromAnchor: alert.hopsFromAnchor,
                estimatedRadiusMeters: alert.estimatedRadiusMeters,
                proximityChain: chain
            },
            status: alert.status,
            timestamp: alert.createdAt,
            ttl: 10,
            hopCount: alert.hopsFromAnchor,
            routePath: chain.length > 0 ? chain : [alert.senderDeviceId]
        };
        socket.send(JSON.stringify(sosPacket));
    }

    // 2. Sync recent community hazard bulletins from SQLite
    // shortcut: sync 10 latest bulletins, when to upgrade: if bulletin volume exceeds memory buffer
    const bulletins = storage.getAllBulletins().slice(0, 10);
    for (const b of bulletins) {
        if (targetDeviceId && b.senderDeviceId === targetDeviceId) continue;

        const bulletinPacket: DisasterBulletin = {
            type: "BULLETIN",
            bulletinId: b.bulletinId,
            senderDeviceId: b.senderDeviceId,
            hazardType: b.hazardType as HazardType,
            announcement: b.announcement,
            timestamp: b.timestamp,
            ttl: 10,
            hopCount: b.hopCount,
            routePath: [b.senderDeviceId]
        };
        socket.send(JSON.stringify(bulletinPacket));
    }
}

// ==================== HTTP Gateway & Captive Portal ====================
const mobileClients = new Set<WebSocket>();

const captivePaths = new Set([
    "/hotspot-detect.html",        // Apple iOS
    "/library/test/success.html",  // Apple iOS
    "/generate_204",              // Android
    "/gen_204",                   // Android
    "/connecttest.txt",           // Windows
    "/ncsi.txt"                   // Windows
]);

let cachedMobileHtml: string | null = null;
function getMobileHtml(): string {
    if (cachedMobileHtml) return cachedMobileHtml;
    const candidates = [
        path.join(process.cwd(), "src", "web", "mobile.html"),
        path.resolve("./src/web/mobile.html")
    ];
    for (const candidate of candidates) {
        if (fs.existsSync(candidate)) {
            cachedMobileHtml = fs.readFileSync(candidate, "utf-8");
            return cachedMobileHtml;
        }
    }
    return `<!DOCTYPE html><html><body><h1>DisasterMesh Mobile Portal</h1></body></html>`;
}

const httpServer = http.createServer((req, res) => {
    const parsedUrl = new URL(req.url || "/", `http://${req.headers.host || "127.0.0.1"}`);
    const pathname = parsedUrl.pathname;

    // Captive probe detection -> 302 Redirect to /mobile
    if (captivePaths.has(pathname)) {
        res.writeHead(302, {
            Location: "/mobile",
            "Cache-Control": "no-cache"
        });
        res.end();
        return;
    }

    if (pathname === "/mobile" || pathname === "/") {
        res.writeHead(200, {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-cache"
        });
        res.end(getMobileHtml());
        return;
    }

    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
});

// ==================== WebSocket Server (Mesh & Mobile) ====================
const server = new WebSocketServer({ server: httpServer, maxPayload: 1024 * 1024 });

server.on("connection", (socket: WebSocket, req: http.IncomingMessage) => {
    const reqUrl = req.url || "/";
    if (reqUrl.startsWith("/mobile-ws")) {
        // Mobile phone client connecting to emergency portal
        mobileClients.add(socket);
        console.log(`[MOBILE GATEWAY] Mobile client connected. Active mobile clients: ${mobileClients.size}`);

        // Sync recent hazard bulletins to phone
        const bulletins = storage.getAllBulletins().slice(0, 5);
        for (const b of bulletins) {
            socket.send(JSON.stringify(b));
        }

        socket.on("message", (data) => {
            try {
                const packet = JSON.parse(data.toString());
                if (packet.type === "PHONE_SOS") {
                    console.log(`[MOBILE GATEWAY] Received Phone SOS from mobile client`);
                    const validTypes = new Set(["TRAPPED", "MEDICAL", "SUPPLIES", "EVACUATION"]);
                    const validSeverities = new Set(["CRITICAL", "HIGH", "MODERATE"]);
                    const emergencyType = (validTypes.has(packet.emergencyType) ? packet.emergencyType : "TRAPPED") as EmergencyType;
                    const severity = (validSeverities.has(packet.severity) ? packet.severity : "CRITICAL") as EmergencySeverity;
                    const peopleCount = Math.max(1, Math.min(999, parseInt(packet.peopleCount, 10) || 1));
                    const description = String(packet.description || "Phone Emergency SOS").slice(0, 500);
                    const landmark = String(packet.landmark || "Nearby Mobile Client").slice(0, 200);

                    const alert = broadcastSos(
                        emergencyType,
                        severity,
                        peopleCount,
                        description,
                        landmark
                    );
                    socket.send(JSON.stringify({
                        type: "PHONE_SOS_ACK",
                        sosId: alert.sosId,
                        status: "DELIVERED"
                    }));
                } else if (packet.type === "SYNC_REQUEST" && packet.sosId) {
                    const alert = storage.getSosAlert(packet.sosId);
                    if (alert) {
                        if (alert.status === "RESOLVED") {
                            socket.send(JSON.stringify({
                                type: "RESCUER_RESPONSE",
                                sosId: alert.sosId,
                                status: alert.status,
                                rescuerDeviceId: alert.rescuerDeviceId || "Rescue Unit",
                                instructions: alert.rescuerInstructions || "Help dispatched/resolved.",
                                estimatedArrivalTime: "Arrived or En Route"
                            }));
                        } else {
                            socket.send(JSON.stringify({
                                type: "PHONE_SOS_ACK",
                                sosId: alert.sosId,
                                status: alert.status
                            }));
                        }
                    }
                }
            } catch (err: any) {
                console.error("[MOBILE WS ERROR]", err.message);
            }
        });

        socket.on("close", () => {
            mobileClients.delete(socket);
            console.log(`[MOBILE GATEWAY] Mobile client disconnected. Remaining: ${mobileClients.size}`);
        });

        socket.on("error", (err) => {
            console.error("[MOBILE WS CLIENT ERROR]", err.message);
            mobileClients.delete(socket);
        });
    } else {
        // Inter-node mesh peering socket
        setupSocketHandlers(socket, "inbound");
    }
});

server.on("error", (error) => {
    console.error("[SERVER ERROR]", error.message);
});

httpServer.listen(PORT, HOST, () => {
    console.log(`[SERVER] Listening on ${HOST}:${PORT} (HTTP Captive Portal & WebSockets)`);
});

export function connectToPeer(address: string): void {
    let url = address;
    if (!url.startsWith("ws://") && !url.startsWith("wss://")) {
        url = `ws://${url}`;
    }

    if (url.includes(`:${PORT}`) && (url.includes("localhost") || url.includes("127.0.0.1") || url.includes("0.0.0.0"))) {
        return;
    }

    knownPeerUrls.add(url);

    try {
        const socket = new WebSocket(url);
        socket.on("open", () => {
            console.log(`[CLIENT] Connected to peer at ${url}`);
            setupSocketHandlers(socket, "outbound", url);
        });
        socket.on("error", (err) => {
            console.log(`[CLIENT ERROR] Connection to ${url} failed: ${err.message}`);
            setTimeout(() => {
                const alreadyConnected = Array.from(peers.values()).some((s: any) => s._originUrl === url);
                if (!alreadyConnected && knownPeerUrls.has(url)) {
                    connectToPeer(url);
                }
            }, 2000);
        });
    } catch (e: any) {
        console.error(`[CLIENT EXCEPTION] Cannot connect to ${url}: ${e.message}`);
    }
}

function setupSocketHandlers(socket: WebSocket, direction: "inbound" | "outbound", originUrl?: string): void {
    if (originUrl) (socket as any)._originUrl = originUrl;
    const identifyMsg: IdentifyMessage = {
        type: "IDENTIFY",
        deviceId: DEVICE_ID
    };
    socket.send(JSON.stringify(identifyMsg));

    let remoteDeviceId: string | null = null;

    socket.on("message", (data) => {
        try {
            const raw = JSON.parse(data.toString()) as MeshPacket;

            if (raw.type === "IDENTIFY") {
                remoteDeviceId = raw.deviceId;
                if (remoteDeviceId === DEVICE_ID) {
                    socket.close();
                    return;
                }

                const existing = peers.get(remoteDeviceId);
                if (existing && existing !== socket && existing.readyState === WebSocket.OPEN) {
                    socket.close();
                    return;
                }

                peers.set(remoteDeviceId, socket);
                updateRoute(remoteDeviceId, remoteDeviceId, 1);
                console.log(`[PEER] Identified: ${remoteDeviceId} (${direction}) | Active peers: ${peers.size}`);
                checkAndFlushPending(remoteDeviceId);
                for (const pendingDest of storage.getAllPendingReceivers()) {
                    checkAndFlushPending(pendingDest);
                }
                syncCrisisStateToPeer(socket, remoteDeviceId);
                meshEvents.emit("peer:change", getActivePeers());
                meshEvents.emit("routes:change", getRoutingTable());
                return;
            }

            if (raw.type === "PING") {
                const pong: HeartbeatMessage = {
                    type: "PONG",
                    deviceId: DEVICE_ID,
                    timestamp: Date.now()
                };
                if (socket.readyState === WebSocket.OPEN) {
                    socket.send(JSON.stringify(pong));
                }
                return;
            }
            if (raw.type === "PONG") return;

            // Phase 6: Emergency SOS Handlers
            if (raw.type === "SOS") {
                handleSosAlert(raw as SosAlert, socket);
                return;
            }
            if (raw.type === "RESCUE_ACK") {
                handleRescueAck(raw as RescueAck, socket);
                return;
            }
            if (raw.type === "BULLETIN") {
                handleDisasterBulletin(raw as DisasterBulletin, socket);
                return;
            }

            // Routing Packets
            if (raw.type === "RREQ") {
                handleRouteRequest(raw as RouteRequest, socket);
                return;
            }
            if (raw.type === "RREP") {
                handleRouteReply(raw as RouteReply, socket);
                return;
            }
            if (raw.type === "RERR") {
                handleRouteError(raw as RouteError, socket);
                return;
            }
            if (raw.type === "ACK") {
                handleAckMessage(raw as AckMessage, socket);
                return;
            }
            if (raw.type === "MESSAGE") {
                handleMeshMessage(raw as Message, socket);
                return;
            }
        } catch (err: any) {
            console.error("[WARN] Failed to parse packet:", err.message);
        }
    });

    const cleanup = () => {
        if (remoteDeviceId && peers.get(remoteDeviceId) === socket) {
            peers.delete(remoteDeviceId);
            if (discoveryService) {
                discoveryService.removePeer(remoteDeviceId);
            }
            console.log(`[PEER] Disconnected: ${remoteDeviceId} | Active peers: ${peers.size}`);
            meshEvents.emit("peer:change", getActivePeers());
            meshEvents.emit("routes:change", getRoutingTable());

            const brokenDestinations: string[] = [];
            for (const [dest, entry] of routingTable.entries()) {
                if (entry.nextHop === remoteDeviceId) {
                    routingTable.delete(dest);
                    brokenDestinations.push(dest);
                }
            }

            if (brokenDestinations.length > 0) {
                broadcastRouteError(brokenDestinations);
            }

            if (originUrl) {
                setTimeout(() => {
                    if (!peers.has(remoteDeviceId!)) {
                        connectToPeer(originUrl);
                    }
                }, 4000);
            }
        }
    };

    socket.on("close", cleanup);
    socket.on("error", cleanup);
}

// ==================== Phase 6: Emergency SOS & Mesh Hop Proximity Engine ====================

export function broadcastSos(
    emergencyType: EmergencyType,
    severity: EmergencySeverity,
    peopleCount: number,
    description: string,
    landmark?: string
): SosAlert {
    stats.sosOriginated++;

    const sosAlert: SosAlert = {
        type: "SOS",
        sosId: randomUUID(),
        senderDeviceId: DEVICE_ID,
        emergencyType,
        severity,
        peopleCount,
        description,
        location: {
            landmark: landmark || "Rooftop / Unspecified",
            anchorDeviceId: DEVICE_ID,
            hopsFromAnchor: 0,
            estimatedRadiusMeters: 50,
            proximityChain: [DEVICE_ID]
        },
        status: "OPEN",
        timestamp: Date.now(),
        ttl: 10, // Higher TTL for emergency
        hopCount: 0,
        routePath: [DEVICE_ID]
    };

    isDuplicate(`SOS-${sosAlert.sosId}`);
    storage.saveSosAlert(sosAlert);
    meshEvents.emit("sos:upsert", sosAlert);
    meshEvents.emit("routes:change", getRoutingTable());

    console.log("\n=======================================================");
    console.log("🚨🚨🚨 EMERGENCY SOS BEACON BROADCASTED 🚨🚨🚨");
    console.log(`SOS ID:      ${sosAlert.sosId}`);
    console.log(`Type:        ${sosAlert.emergencyType} | Severity: ${sosAlert.severity}`);
    console.log(`Victims:     ${sosAlert.peopleCount} person(s)`);
    console.log(`Landmark:    ${sosAlert.location.landmark}`);
    console.log(`Description: "${sosAlert.description}"`);
    console.log("=======================================================\n");

    // Flood network-wide with highest priority
    broadcastPacket(sosAlert);
    return sosAlert;
}

function handleSosAlert(sos: SosAlert, sourceSocket: WebSocket): void {
    const sosKey = `SOS-${sos.sosId}`;
    if (isDuplicate(sosKey)) {
        return;
    }

    if (sos.routePath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    stats.sosReceived++;

    // Calculate Mesh Hop Proximity Localization
    const currentHops = sos.hopCount + 1;
    // Each Wi-Fi hop approximates ~75 meters
    const estimatedRadius = 50 + currentHops * 75;
    const updatedProximityChain = [...sos.location.proximityChain, DEVICE_ID];

    const updatedSos: SosAlert = {
        ...sos,
        ttl: sos.ttl - 1,
        hopCount: currentHops,
        routePath: [...sos.routePath, DEVICE_ID],
        location: {
            ...sos.location,
            hopsFromAnchor: currentHops,
            estimatedRadiusMeters: estimatedRadius,
            proximityChain: updatedProximityChain
        }
    };

    // Store in local triage registry
    storage.saveSosAlert(updatedSos);
    meshEvents.emit("sos:upsert", updatedSos);
    meshEvents.emit("routes:change", getRoutingTable());

    // Learn reverse route to victim!
    const previousHop = sos.routePath[sos.routePath.length - 1];
    updateRoute(sos.senderDeviceId, previousHop, currentHops);

    console.log("\n🚨🚨🚨 [CRITICAL ALERT RECEIVED] 🚨🚨🚨");
    console.log(`SOS ID:          ${sos.sosId}`);
    console.log(`Victim Node:     ${sos.senderDeviceId}`);
    console.log(`Type:            ${sos.emergencyType} (${sos.severity}) | Victims: ${sos.peopleCount}`);
    console.log(`Landmark:        ${sos.location.landmark}`);
    console.log(`Description:     "${sos.description}"`);
    console.log(`📍 Mesh Proximity: ${currentHops} hop(s) away (~${estimatedRadius}m search radius)`);
    console.log(`📍 Topology Trace: ${updatedProximityChain.join(" -> ")}`);
    console.log("-------------------------------------------------------\n");

    // Relay onward if TTL allows
    if (updatedSos.ttl > 1) {
        stats.sosRelayed++;
        broadcastPacket(updatedSos, sourceSocket);
    }
}

export function respondToSos(
    sosId: string,
    status: "DISPATCHED" | "STANDBY" | "RESOLVED",
    instructions: string,
    eta?: string
): boolean {
    const alert = storage.getSosAlert(sosId);
    if (!alert) {
        console.log(`❌ SOS ID ${sosId} not found in triage database.`);
        return false;
    }

    stats.rescueAcksSent++;
    storage.updateSosStatus(sosId, status, DEVICE_ID, instructions);
    const updatedAlert = storage.getSosAlert(sosId);
    if (updatedAlert) meshEvents.emit("sos:upsert", updatedAlert);

    const localMobileNotification = JSON.stringify({
        type: "RESCUER_RESPONSE",
        sosId: sosId,
        status: status,
        rescuerDeviceId: DEVICE_ID,
        estimatedArrivalTime: eta || "15-20 mins",
        instructions: instructions
    });
    for (const client of mobileClients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(localMobileNotification);
        }
    }

    const rescueAck: RescueAck = {
        type: "RESCUE_ACK",
        sosId: sosId,
        rescuerDeviceId: DEVICE_ID,
        victimDeviceId: alert.senderDeviceId,
        status: status,
        estimatedArrivalTime: eta || "15-20 mins",
        instructions: instructions,
        timestamp: Date.now(),
        ttl: getAdaptiveTtl(alert.senderDeviceId, 15),
        hopCount: 0,
        routePath: [DEVICE_ID]
    };

    isDuplicate(`RESCUE_ACK-${sosId}-${status}`);

    console.log("\n=======================================================");
    console.log(`✅ [RESCUE RESPONSE DISPATCHED] To Victim: ${alert.senderDeviceId}`);
    console.log(`Status:       ${status}`);
    console.log(`ETA:          ${rescueAck.estimatedArrivalTime}`);
    console.log(`Instructions: "${instructions}"`);
    console.log("=======================================================\n");

    // Try directed forwarding via routing table, fallback to broadcast
    const route = getValidRoute(alert.senderDeviceId);
    if (route) {
        sendDirected(route.nextHop, rescueAck);
    } else {
        broadcastPacket(rescueAck);
    }

    return true;
}

function handleRescueAck(ack: RescueAck, sourceSocket: WebSocket): void {
    const ackKey = `RESCUE_ACK-${ack.sosId}-${ack.status}`;
    if (isDuplicate(ackKey)) return;

    if (ack.routePath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    stats.rescueAcksReceived++;
    storage.updateSosStatus(ack.sosId, ack.status, ack.rescuerDeviceId, ack.instructions);
    const updatedAckAlert = storage.getSosAlert(ack.sosId);
    if (updatedAckAlert) meshEvents.emit("sos:upsert", updatedAckAlert);

    // Push rescue update down to any connected mobile phone clients
    const mobileNotification = JSON.stringify({
        type: "RESCUER_RESPONSE",
        sosId: ack.sosId,
        status: ack.status,
        rescuerDeviceId: ack.rescuerDeviceId,
        estimatedArrivalTime: ack.estimatedArrivalTime,
        instructions: ack.instructions
    });
    for (const client of mobileClients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(mobileNotification);
        }
    }

    // If WE are the victim node
    if (ack.victimDeviceId === DEVICE_ID) {
        console.log("\n=======================================================");
        console.log("🔔🔔🔔 RESCUE TEAM CONFIRMATION RECEIVED! 🔔🔔🔔");
        console.log(`Rescuer Unit: ${ack.rescuerDeviceId}`);
        console.log(`Status:       ${ack.status} (HELP IS ON THE WAY!)`);
        console.log(`ETA:          ${ack.estimatedArrivalTime}`);
        console.log(`Instructions: "${ack.instructions}"`);
        console.log(`Route Taken:  ${[...ack.routePath, DEVICE_ID].join(" -> ")}`);
        console.log("=======================================================\n");
        return;
    }

    // Relay back to victim
    if (ack.ttl <= 1) return;

    const relayedAck: RescueAck = {
        ...ack,
        ttl: ack.ttl - 1,
        hopCount: ack.hopCount + 1,
        routePath: [...ack.routePath, DEVICE_ID]
    };

    const route = getValidRoute(relayedAck.victimDeviceId);
    if (route) {
        sendDirected(route.nextHop, relayedAck);
    } else {
        broadcastPacket(relayedAck, sourceSocket);
    }
}

export function broadcastBulletin(hazardType: HazardType, announcement: string): DisasterBulletin {
    stats.bulletinsSent++;
    const bulletin: DisasterBulletin = {
        type: "BULLETIN",
        bulletinId: randomUUID(),
        senderDeviceId: DEVICE_ID,
        hazardType,
        announcement,
        timestamp: Date.now(),
        ttl: DEFAULT_SEARCH_TTL,
        hopCount: 0,
        routePath: [DEVICE_ID]
    };

    isDuplicate(`BULLETIN-${bulletin.bulletinId}`);
    storage.saveBulletin(bulletin);
    meshEvents.emit("bulletin:new", bulletin);

    for (const client of mobileClients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(bulletin));
        }
    }

    console.log("\n⚠️⚠️⚠️ [PUBLIC HAZARD BULLETIN BROADCASTED] ⚠️⚠️⚠️");
    console.log(`Hazard:       ${hazardType}`);
    console.log(`Announcement: "${announcement}"`);
    console.log("===================================================\n");

    broadcastPacket(bulletin);
    return bulletin;
}

function handleDisasterBulletin(bulletin: DisasterBulletin, sourceSocket: WebSocket): void {
    const key = `BULLETIN-${bulletin.bulletinId}`;
    if (isDuplicate(key)) return;

    if (bulletin.routePath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    stats.bulletinsReceived++;
    storage.saveBulletin(bulletin);
    meshEvents.emit("bulletin:new", bulletin);

    for (const client of mobileClients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(bulletin));
        }
    }

    console.log("\n⚠️⚠️⚠️ [COMMUNITY DISASTER WARNING] ⚠️⚠️⚠️");
    console.log(`Issued By:    ${bulletin.senderDeviceId}`);
    console.log(`Hazard:       ${bulletin.hazardType}`);
    console.log(`Announcement: "${bulletin.announcement}"`);
    console.log(`Hops Away:    ${bulletin.hopCount + 1}`);
    console.log("==========================================\n");

    if (bulletin.ttl > 1) {
        const relayed: DisasterBulletin = {
            ...bulletin,
            ttl: bulletin.ttl - 1,
            hopCount: bulletin.hopCount + 1,
            routePath: [...bulletin.routePath, DEVICE_ID]
        };
        broadcastPacket(relayed, sourceSocket);
    }
}

// ==================== General Mesh & Routing Handlers ====================
function broadcastRouteError(brokenDestinations: string[]): void {
    const rerr: RouteError = {
        type: "RERR",
        errorId: randomUUID(),
        reporterDeviceId: DEVICE_ID,
        unreachableDestinations: brokenDestinations,
        ttl: DEFAULT_SEARCH_TTL,
        hopCount: 0,
        routePath: [DEVICE_ID]
    };
    isDuplicate(`RERR-${rerr.errorId}`);
    broadcastPacket(rerr);
}

function handleRouteError(rerr: RouteError, sourceSocket: WebSocket): void {
    if (isDuplicate(`RERR-${rerr.errorId}`) || rerr.routePath.includes(DEVICE_ID)) return;

    const previousHop = rerr.routePath[rerr.routePath.length - 1];
    let invalidated = false;

    for (const dest of rerr.unreachableDestinations) {
        const entry = routingTable.get(dest);
        if (entry && entry.nextHop === previousHop) {
            routingTable.delete(dest);
            invalidated = true;
        }
    }

    if (invalidated && rerr.ttl > 1) {
        broadcastPacket({
            ...rerr,
            ttl: rerr.ttl - 1,
            hopCount: rerr.hopCount + 1,
            routePath: [...rerr.routePath, DEVICE_ID]
        }, sourceSocket);
    }
}

function handleRouteRequest(rreq: RouteRequest, sourceSocket: WebSocket): void {
    if (isDuplicate(`RREQ-${rreq.requestId}`) || rreq.routePath.includes(DEVICE_ID)) return;

    const previousHop = rreq.routePath[rreq.routePath.length - 1];
    updateRoute(rreq.originDeviceId, previousHop, rreq.hopCount + 1);

    if (rreq.targetDeviceId === DEVICE_ID) {
        const rrep: RouteReply = {
            type: "RREP",
            requestId: rreq.requestId,
            originDeviceId: rreq.originDeviceId,
            targetDeviceId: DEVICE_ID,
            hopCount: 0,
            ttl: getAdaptiveTtl(rreq.originDeviceId, DEFAULT_SEARCH_TTL),
            routePath: [DEVICE_ID]
        };
        isDuplicate(`RREP-${rrep.requestId}-${rrep.targetDeviceId}`);
        const reverseRoute = getValidRoute(rreq.originDeviceId);
        if (reverseRoute) {
            sendDirected(reverseRoute.nextHop, rrep);
        } else {
            sendDirected(previousHop, rrep);
        }
        return;
    }

    if (rreq.ttl <= 1) return;
    broadcastPacket({
        ...rreq,
        ttl: rreq.ttl - 1,
        hopCount: rreq.hopCount + 1,
        routePath: [...rreq.routePath, DEVICE_ID]
    }, sourceSocket);
}

function handleRouteReply(rrep: RouteReply, sourceSocket: WebSocket): void {
    if (isDuplicate(`RREP-${rrep.requestId}-${rrep.targetDeviceId}`) || rrep.routePath.includes(DEVICE_ID)) return;

    const previousHop = rrep.routePath[rrep.routePath.length - 1];
    updateRoute(rrep.targetDeviceId, previousHop, rrep.hopCount + 1);

    if (rrep.originDeviceId === DEVICE_ID) {
        pendingRouteRequests.delete(rrep.targetDeviceId);
        checkAndFlushPending(rrep.targetDeviceId);
        return;
    }

    if (rrep.ttl <= 1) return;
    const reverseRoute = getValidRoute(rrep.originDeviceId);
    const forwarded: RouteReply = {
        ...rrep,
        ttl: rrep.ttl - 1,
        hopCount: rrep.hopCount + 1,
        routePath: [...rrep.routePath, DEVICE_ID]
    };
    if (reverseRoute) {
        sendDirected(reverseRoute.nextHop, forwarded);
    } else {
        broadcastPacket(forwarded, sourceSocket);
    }
}

function registerPendingAck(message: Message): void {
    if (message.senderDeviceId !== DEVICE_ID) return;
    if (pendingAcks.has(message.messageId)) {
        clearTimeout(pendingAcks.get(message.messageId)!.timer);
    }
    const timer = setTimeout(() => {
        pendingAcks.delete(message.messageId);
        const record = storage.getAllStoredRecords().find(r => r.messageId === message.messageId);
        if (record && record.status !== "DELIVERED" && record.retryCount <= 3) {
            console.log(`[ACK TIMEOUT] Message ${message.messageId} to ${message.receiverDeviceId} unacknowledged. Retrying...`);
            dispatchDirectedMessage(message);
        }
    }, ACK_TIMEOUT_MS);
    pendingAcks.set(message.messageId, { message, sentAt: Date.now(), timer });
}

export function sendDirectedMessage(targetDeviceId: string, text: string, customTtl?: number): void {
    stats.originatedMessages++;
    const calculatedTtl = customTtl !== undefined ? customTtl : getAdaptiveTtl(targetDeviceId);
    const msg: Message = {
        type: "MESSAGE",
        messageId: randomUUID(),
        senderDeviceId: DEVICE_ID,
        receiverDeviceId: targetDeviceId,
        payload: text,
        timestamp: Date.now(),
        ttl: calculatedTtl,
        hopCount: 0,
        routePath: [DEVICE_ID]
    };
    isDuplicate(msg.messageId);
    console.log(`[DISPATCH] Sending message to ${targetDeviceId} (Adaptive TTL: ${calculatedTtl}): "${text}"`);
    dispatchDirectedMessage(msg);
}

function dispatchDirectedMessage(message: Message): void {
    const route = getValidRoute(message.receiverDeviceId);
    if (route) {
        if (message.ttl === DEFAULT_SEARCH_TTL) {
            message.ttl = getAdaptiveTtl(message.receiverDeviceId);
            console.log(`[ROUTE-AWARE] Route resolved for ${message.receiverDeviceId} (distance: ${route.hopDistance}) -> Adaptive TTL dynamically set to ${message.ttl}`);
        }
        stats.directedForwarded++;
        const sent = sendDirected(route.nextHop, message);
        if (sent) {
            registerPendingAck(message);
            return;
        }
    }

    storage.saveMessage(message, "PENDING");
    console.log(`[STORAGE] No route to ${message.receiverDeviceId}. Message ${message.messageId} buffered to SQLite (PENDING)`);

    if (!pendingRouteRequests.has(message.receiverDeviceId)) {
        pendingRouteRequests.add(message.receiverDeviceId);
        setTimeout(() => pendingRouteRequests.delete(message.receiverDeviceId), 3000);
        broadcastPacket({
            type: "RREQ",
            requestId: randomUUID(),
            originDeviceId: DEVICE_ID,
            targetDeviceId: message.receiverDeviceId,
            hopCount: 0,
            ttl: DEFAULT_SEARCH_TTL,
            routePath: [DEVICE_ID]
        });
    }
}

function handleMeshMessage(message: Message, sourceSocket: WebSocket): void {
    if (isDuplicate(message.messageId)) return;
    if ((message.routePath ?? []).includes(DEVICE_ID)) {
        stats.droppedLoops++;
        console.log(`[LOOP] Suppressed packet loop: ${message.messageId} already passed through ${DEVICE_ID}`);
        return;
    }

    const currentPath = message.routePath ? [...message.routePath] : [];

    if (message.receiverDeviceId === DEVICE_ID || message.receiverDeviceId === "*") {
        stats.deliveredMessages++;
        console.log(`\n📬 [DELIVERED] From: ${message.senderDeviceId} | "${message.payload}"\n`);
        if (message.receiverDeviceId === DEVICE_ID) {
            sendAck(message.senderDeviceId, message.messageId);
            return;
        }
    }

    if (message.ttl <= 1) {
        stats.droppedExpiredTtl++;
        console.log(`[TTL] Dropped expired packet ${message.messageId} at ${DEVICE_ID} (TTL reached 0)`);
        return;
    }

    stats.relayedMessages++;
    console.log(`[RELAY] Relayed message from ${message.senderDeviceId} towards ${message.receiverDeviceId}: "${message.payload}"`);
    const relayed: Message = {
        ...message,
        ttl: message.ttl - 1,
        hopCount: message.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    if (message.receiverDeviceId === "*") {
        broadcastPacket(relayed, sourceSocket);
    } else {
        const route = getValidRoute(relayed.receiverDeviceId);
        if (route) {
            stats.directedForwarded++;
            sendDirected(route.nextHop, relayed);
        } else {
            broadcastPacket(relayed, sourceSocket);
        }
    }
}

function sendAck(targetDeviceId: string, ackMessageId: string): void {
    const ack: AckMessage = {
        type: "ACK",
        ackMessageId,
        senderDeviceId: DEVICE_ID,
        receiverDeviceId: targetDeviceId,
        timestamp: Date.now(),
        ttl: getAdaptiveTtl(targetDeviceId, DEFAULT_SEARCH_TTL),
        hopCount: 0,
        routePath: [DEVICE_ID]
    };
    isDuplicate(`ACK-${ack.ackMessageId}-${ack.senderDeviceId}`);
    const route = getValidRoute(targetDeviceId);
    if (route) sendDirected(route.nextHop, ack);
    else broadcastPacket(ack);
}

function handleAckMessage(ack: AckMessage, sourceSocket: WebSocket): void {
    if (isDuplicate(`ACK-${ack.ackMessageId}-${ack.senderDeviceId}`) || (ack.routePath ?? []).includes(DEVICE_ID)) return;

    if (ack.receiverDeviceId === DEVICE_ID) {
        storage.markDelivered(ack.ackMessageId);
        const pending = pendingAcks.get(ack.ackMessageId);
        if (pending) {
            clearTimeout(pending.timer);
            pendingAcks.delete(ack.ackMessageId);
            console.log(`✅ [ACK CONFIRMED] Message ${ack.ackMessageId} delivered to ${ack.senderDeviceId}`);
        }
        return;
    }

    if (ack.ttl <= 1) return;
    const relayed: AckMessage = {
        ...ack,
        ttl: ack.ttl - 1,
        hopCount: ack.hopCount + 1,
        routePath: [...(ack.routePath ?? []), DEVICE_ID]
    };
    const route = getValidRoute(relayed.receiverDeviceId);
    if (route) sendDirected(route.nextHop, relayed);
    else broadcastPacket(relayed, sourceSocket);
}

function sendDirected(nextHopDeviceId: string, packet: MeshPacket): boolean {
    const socket = peers.get(nextHopDeviceId);
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(packet));
        return true;
    }
    return false;
}

function broadcastPacket(packet: MeshPacket, exceptSocket?: WebSocket): void {
    stats.floodedForwarded++;
    const payload = JSON.stringify(packet);
    for (const [peerId, socket] of peers.entries()) {
        if (socket !== exceptSocket && socket.readyState === WebSocket.OPEN) {
            socket.send(payload);
        }
    }
}

// Keepalive
setInterval(() => {
    const ping: HeartbeatMessage = { type: "PING", deviceId: DEVICE_ID, timestamp: Date.now() };
    const payload = JSON.stringify(ping);
    for (const socket of peers.values()) {
        if (socket.readyState === WebSocket.OPEN) socket.send(payload);
    }
}, 15000);

// Phase 5 Store-and-Forward: Periodic retry worker for partitioned messages
setInterval(() => {
    if (peers.size === 0) return;
    const pendingReceivers = storage.getAllPendingReceivers();
    for (const target of pendingReceivers) {
        checkAndFlushPending(target);
    }
}, 3000);

// mDNS Discovery
if (ENABLE_MDNS) {
    discoveryService = new PeerDiscovery({
        deviceId: DEVICE_ID,
        port: PORT,
        onPeerDiscovered: (peer: DiscoveredPeer) => {
            if (!peers.has(peer.deviceId)) {
                connectToPeer(`ws://${peer.ip}:${peer.port}`);
            }
        }
    });
    discoveryService.start(5000);
}

if (MANUAL_PEER) {
    connectToPeer(MANUAL_PEER);
}

// Start visual operations dashboard if running as Rescue Base
if (IS_RESCUE_BASE) {
    startDashboardServer({
        host: HOST,
        port: DASHBOARD_PORT,
        nodeDeviceId: DEVICE_ID,
        storage,
        respondToSos,
        broadcastBulletin,
        getActivePeers,
        getRoutingTable,
        getStats,
        meshEvents
    });
}

// ==================== Crisis Command Terminal CLI ====================
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log("\nEmergency Commands:");
console.log("  sos <TRAPPED|MEDICAL|SUPPLIES|EVACUATION> <CRITICAL|HIGH|MODERATE> <count> <description> - Send SOS Beacon");
console.log("  respond <sosId> <DISPATCHED|STANDBY|RESOLVED> <instructions>                              - Rescuer Response");
console.log("  warn <FLOOD_SURGE|COLLAPSE_RISK|SAFE_ZONE|AIRDROP> <announcement>                         - Broadcast Warning");
console.log("  triage                                                                                    - View SOS Registry");
console.log("  bulletins                                                                                 - View Hazard Bulletins");
console.log("  routes                                                                                    - View Routing Table");
console.log("  peers                                                                                     - View Connected Peers");
console.log("  connect <host:port>                                                                       - Connect to peer");
console.log("  quit                                                                                      - Exit\n");

rl.on("line", (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;

    const parts = trimmed.split(" ");
    const cmd = parts[0].toLowerCase();

    if (cmd === "sos") {
        if (parts.length < 5) {
            console.log("Usage: sos <TRAPPED|MEDICAL|SUPPLIES|EVACUATION> <CRITICAL|HIGH|MODERATE> <peopleCount> <description>");
            return;
        }
        const type = parts[1].toUpperCase() as EmergencyType;
        const severity = parts[2].toUpperCase() as EmergencySeverity;
        const count = parseInt(parts[3], 10) || 1;
        const desc = parts.slice(4).join(" ");
        broadcastSos(type, severity, count, desc);
    } else if (cmd === "respond") {
        if (parts.length < 4) {
            console.log("Usage: respond <sosId> <DISPATCHED|STANDBY|RESOLVED> <instructions>");
            return;
        }
        const sosId = parts[1];
        const status = parts[2].toUpperCase() as "DISPATCHED" | "STANDBY" | "RESOLVED";
        const instructions = parts.slice(3).join(" ");
        respondToSos(sosId, status, instructions);
    } else if (cmd === "warn") {
        if (parts.length < 3) {
            console.log("Usage: warn <FLOOD_SURGE|COLLAPSE_RISK|SAFE_ZONE|AIRDROP> <announcement>");
            return;
        }
        const hazard = parts[1].toUpperCase() as HazardType;
        const announcement = parts.slice(2).join(" ");
        broadcastBulletin(hazard, announcement);
    } else if (cmd === "triage") {
        const alerts = storage.getAllSosAlerts();
        console.log(`\n=== 🚨 Active Emergency Triage Registry (${alerts.length} Incidents) ===`);
        for (const a of alerts) {
            console.log(`-------------------------------------------------------------------`);
            console.log(`ID:       ${a.sosId}`);
            console.log(`Victim:   ${a.senderDeviceId} | Victims: ${a.peopleCount} person(s)`);
            console.log(`Status:   [${a.status}] | Severity: [${a.severity}] | Type: ${a.emergencyType}`);
            console.log(`Desc:     "${a.description}"`);
            console.log(`📍 Proximity: ${a.hopsFromAnchor} hop(s) away | Search Radius: ~${a.estimatedRadiusMeters}m`);
            console.log(`📍 Trace:     ${a.proximityChainJson}`);
            if (a.rescuerDeviceId) {
                console.log(`Rescuer:  ${a.rescuerDeviceId} | Note: "${a.rescuerInstructions}"`);
            }
        }
        console.log(`-------------------------------------------------------------------\n`);
    } else if (cmd === "bulletins") {
        const list = storage.getAllBulletins();
        console.log(`\n=== ⚠️ Community Hazard Warnings (${list.length} Bulletins) ===`);
        for (const b of list) {
            const timeStr = new Date(b.timestamp).toLocaleTimeString();
            console.log(`  [${b.hazardType}] (${timeStr}) "${b.announcement}" - From: ${b.senderDeviceId}`);
        }
        console.log("=================================================================\n");
    } else if (cmd === "routes") {
        console.log(`\n=== Routing Table (${routingTable.size} entries) ===`);
        for (const [dest, entry] of routingTable.entries()) {
            console.log(`  Target: ${dest.padEnd(15)} NextHop: ${entry.nextHop.padEnd(15)} Hops: ${entry.hopDistance}`);
        }
        console.log("=====================================\n");
    } else if (cmd === "peers") {
        console.log(`Connected peers (${peers.size}):`);
        for (const [id] of peers.entries()) console.log(`  - ${id}`);
    } else if (cmd === "stats") {
        console.log(`\n=== Node Metrics & Telemetry ===`);
        console.log(JSON.stringify(stats, null, 2));
        console.log("=================================\n");
    } else if (cmd === "msg") {
        if (parts.length < 3) {
            console.log("Usage: msg <targetDeviceId> <text> [ttl]");
            return;
        }
        const target = parts[1];
        let customTtl: number | undefined = undefined;
        const textParts = parts.slice(2);
        if (textParts.length > 1 && !isNaN(parseInt(textParts[textParts.length - 1], 10))) {
            customTtl = parseInt(textParts.pop()!, 10);
        }
        const text = textParts.join(" ");
        sendDirectedMessage(target, text, customTtl);
    } else if (cmd === "connect" && parts[1]) {
        connectToPeer(parts[1]);
    } else if (cmd === "quit" || cmd === "exit") {
        if (discoveryService) discoveryService.stop();
        stopDashboardServer();
        storage.close();
        httpServer.close();
        server.close();
        process.exit(0);
    } else {
        console.log("Commands: sos | respond | warn | triage | bulletins | routes | peers | connect | msg | quit");
    }
});
