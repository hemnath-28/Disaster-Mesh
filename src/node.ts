import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import readline from "readline";
import {
    Message,
    AckMessage,
    RouteRequest,
    RouteReply,
    HeartbeatMessage,
    IdentifyMessage,
    MeshPacket,
    RouteEntry,
    DiscoveredPeer
} from "./types.js";
import { PeerDiscovery } from "./discovery.js";

// Configuration via environment or arguments
const HOST = process.env.HOST || "0.0.0.0";
const PORT = parseInt(process.env.PORT || process.argv[2] || "5000", 10);
const DEVICE_ID = process.env.DEVICE_ID || process.argv[3] || `Node-${randomUUID().substring(0, 5)}`;
const MANUAL_PEER = process.env.PEER_URL || process.argv[4] || "";
const ENABLE_MDNS = process.env.ENABLE_MDNS !== "false";

console.log("=========================================");
console.log(` DisasterMesh Node: ${DEVICE_ID}`);
console.log(` WebSocket Port:    ${PORT}`);
console.log(` mDNS Auto-Discovery: ${ENABLE_MDNS ? "ENABLED" : "DISABLED"}`);
console.log("=========================================");

// Active connections: deviceId -> WebSocket
const peers = new Map<string, WebSocket>();
// Known peer URLs for reconnection
const knownPeerUrls = new Set<string>();

// ==================== Phase 4: Routing Table & Route Discovery ====================
// destinationDeviceId -> RouteEntry
const routingTable = new Map<string, RouteEntry>();
const ROUTE_TTL_MS = 5 * 60 * 1000; // Routes valid for 5 minutes

// Pending outgoing messages waiting for route discovery: destinationDeviceId -> Array of queued messages
const messageWaitQueue = new Map<string, Array<{ message: Message; ttl: number }>>();
// Track active route requests to prevent duplicate broadcasts
const pendingRouteRequests = new Set<string>();

// Track pending acknowledgments: messageId -> PendingAck
interface PendingAck {
    message: Message;
    sentAt: number;
    timer: NodeJS.Timeout;
}
const pendingAcks = new Map<string, PendingAck>();
const ACK_TIMEOUT_MS = 6000;

// Deduplication cache: stores packet key and expiry timestamp
const seenPackets = new Map<string, number>();
const SEEN_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

// Node Metrics
const stats = {
    originatedMessages: 0,
    deliveredMessages: 0,
    relayedMessages: 0,
    directedForwarded: 0,
    floodedForwarded: 0,
    routeRequestsOriginated: 0,
    routeRepliesOriginated: 0,
    acksOriginated: 0,
    acksReceived: 0,
    acksRelayed: 0,
    droppedDuplicates: 0,
    droppedExpiredTtl: 0,
    droppedLoops: 0
};

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
        return true;
    }

    seenPackets.set(packetKey, now + SEEN_CACHE_TTL_MS);
    return false;
}

// Add or update an entry in the routing table
function updateRoute(destination: string, nextHop: string, hopDistance: number): void {
    const existing = routingTable.get(destination);
    // Update if no route exists or if new route is strictly shorter
    if (!existing || hopDistance < existing.hopDistance || Date.now() > existing.expiresAt) {
        routingTable.set(destination, {
            destination,
            nextHop,
            hopDistance,
            expiresAt: Date.now() + ROUTE_TTL_MS
        });
        console.log(`🗺️ [ROUTING TABLE] Updated route to ${destination} via ${nextHop} (${hopDistance} hops)`);
    }
}

function getValidRoute(destination: string): RouteEntry | undefined {
    // Direct 1-hop neighbor is always a valid 1-hop route
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

    if (Date.now() > route.expiresAt) {
        routingTable.delete(destination);
        return undefined;
    }

    // Verify nextHop peer is currently connected
    if (!peers.has(route.nextHop)) {
        routingTable.delete(destination);
        return undefined;
    }

    return route;
}

// ==================== WebSocket Server ====================
const server = new WebSocketServer({ host: HOST, port: PORT });

server.on("listening", () => {
    console.log(`[SERVER] Listening on ${HOST}:${PORT}`);
});

server.on("connection", (socket: WebSocket) => {
    setupSocketHandlers(socket, "inbound");
});

server.on("error", (error) => {
    console.error("[SERVER ERROR]", error.message);
});

// ==================== WebSocket Peer Connection ====================
export function connectToPeer(address: string): void {
    let url = address;
    if (!url.startsWith("ws://") && !url.startsWith("wss://")) {
        url = `ws://${url}`;
    }

    // Prevent connecting to self
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
        });
    } catch (e: any) {
        console.error(`[CLIENT EXCEPTION] Cannot connect to ${url}: ${e.message}`);
    }
}

function setupSocketHandlers(socket: WebSocket, direction: "inbound" | "outbound", originUrl?: string): void {
    const identifyMsg: IdentifyMessage = {
        type: "IDENTIFY",
        deviceId: DEVICE_ID
    };
    socket.send(JSON.stringify(identifyMsg));

    let remoteDeviceId: string | null = null;

    socket.on("message", (data) => {
        try {
            const raw = JSON.parse(data.toString()) as MeshPacket;

            // Handle Identification Handshake
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
                console.log(`[PEER] Identified: ${remoteDeviceId} (${direction}) | Total peers: ${peers.size}`);
                return;
            }

            // Handle Heartbeats
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
            if (raw.type === "PONG") {
                return;
            }

            // Phase 4: Route Discovery Packets
            if (raw.type === "RREQ") {
                handleRouteRequest(raw as RouteRequest, socket);
                return;
            }
            if (raw.type === "RREP") {
                handleRouteReply(raw as RouteReply, socket);
                return;
            }

            // Handle ACKs
            if (raw.type === "ACK") {
                handleAckMessage(raw as AckMessage, socket);
                return;
            }

            // Handle Messages
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
            console.log(`[PEER] Disconnected: ${remoteDeviceId} | Remaining peers: ${peers.size}`);

            // Remove dead routes through this disconnected peer
            for (const [dest, entry] of routingTable.entries()) {
                if (entry.nextHop === remoteDeviceId) {
                    routingTable.delete(dest);
                    console.log(`🗺️ [ROUTING TABLE] Invalidated route to ${dest} (nextHop ${remoteDeviceId} lost)`);
                }
            }

            // Reconnection attempt
            if (originUrl) {
                setTimeout(() => {
                    if (!peers.has(remoteDeviceId!)) {
                        console.log(`[RECONNECT] Attempting reconnect to ${originUrl}...`);
                        connectToPeer(originUrl);
                    }
                }, 4000);
            }
        }
    };

    socket.on("close", cleanup);
    socket.on("error", cleanup);
}

// ==================== Phase 4: Route Discovery Protocol (RREQ / RREP) ====================
function initiateRouteDiscovery(targetDeviceId: string): void {
    if (pendingRouteRequests.has(targetDeviceId)) {
        return; // Already actively discovering
    }

    pendingRouteRequests.add(targetDeviceId);
    stats.routeRequestsOriginated++;

    const rreq: RouteRequest = {
        type: "RREQ",
        requestId: randomUUID(),
        originDeviceId: DEVICE_ID,
        targetDeviceId: targetDeviceId,
        hopCount: 0,
        ttl: 5,
        routePath: [DEVICE_ID]
    };

    isDuplicate(`RREQ-${rreq.requestId}`);
    console.log(`\n🔍 [ROUTE DISCOVERY] Seeking shortest route to ${targetDeviceId}...`);
    broadcastPacket(rreq);

    // Timeout if no reply
    setTimeout(() => {
        pendingRouteRequests.delete(targetDeviceId);
        if (!getValidRoute(targetDeviceId)) {
            console.log(`⚠️ [ROUTE TIMEOUT] Could not find route to ${targetDeviceId}`);
            // If any messages were queued, drop them
            const queued = messageWaitQueue.get(targetDeviceId);
            if (queued) {
                console.log(`[QUEUE] Dropped ${queued.length} messages for unreachable ${targetDeviceId}`);
                messageWaitQueue.delete(targetDeviceId);
            }
        }
    }, 4000);
}

function handleRouteRequest(rreq: RouteRequest, sourceSocket: WebSocket): void {
    const rreqKey = `RREQ-${rreq.requestId}`;
    if (isDuplicate(rreqKey)) {
        return;
    }

    // Loop detection
    if (rreq.routePath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    // Determine which neighbor just handed us this RREQ (previous hop)
    const previousHop = rreq.routePath[rreq.routePath.length - 1];

    // Reverse Route Learning: We now know how to reach rreq.originDeviceId!
    updateRoute(rreq.originDeviceId, previousHop, rreq.hopCount + 1);

    // Check if WE are the target of the discovery
    if (rreq.targetDeviceId === DEVICE_ID) {
        stats.routeRepliesOriginated++;
        console.log(`🎯 [RREQ TARGET] Route request reached destination (${DEVICE_ID})! Generating RREP.`);

        const rrep: RouteReply = {
            type: "RREP",
            requestId: rreq.requestId,
            originDeviceId: rreq.originDeviceId,
            targetDeviceId: DEVICE_ID,
            hopCount: 0,
            ttl: 5,
            routePath: [DEVICE_ID]
        };

        isDuplicate(`RREP-${rrep.requestId}-${rrep.targetDeviceId}`);

        // Send RREP back along the learned reverse route
        const reverseRoute = getValidRoute(rreq.originDeviceId);
        if (reverseRoute) {
            console.log(`📨 [RREP SENT] Sending reply back to ${rreq.originDeviceId} via nextHop: ${reverseRoute.nextHop}`);
            sendDirected(reverseRoute.nextHop, rrep);
        } else {
            // Fallback: send directly back to previousHop
            sendDirected(previousHop, rrep);
        }
        return;
    }

    // Not the target: check TTL and forward RREQ
    if (rreq.ttl <= 1) {
        stats.droppedExpiredTtl++;
        return;
    }

    const forwardedRreq: RouteRequest = {
        ...rreq,
        ttl: rreq.ttl - 1,
        hopCount: rreq.hopCount + 1,
        routePath: [...rreq.routePath, DEVICE_ID]
    };

    // Forward RREQ to all neighbors except sender
    broadcastPacket(forwardedRreq, sourceSocket);
}

function handleRouteReply(rrep: RouteReply, sourceSocket: WebSocket): void {
    const rrepKey = `RREP-${rrep.requestId}-${rrep.targetDeviceId}`;
    if (isDuplicate(rrepKey)) {
        return;
    }

    if (rrep.routePath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    // Previous hop that just handed us this RREP
    const previousHop = rrep.routePath[rrep.routePath.length - 1];

    // Forward Route Learning: We now know how to reach rrep.targetDeviceId via previousHop!
    updateRoute(rrep.targetDeviceId, previousHop, rrep.hopCount + 1);

    // Check if WE are the origin node that asked for this route
    if (rrep.originDeviceId === DEVICE_ID) {
        pendingRouteRequests.delete(rrep.targetDeviceId);
        console.log(`\n🎉 [ROUTE DISCOVERED] Established route to ${rrep.targetDeviceId} via ${previousHop} (Total Hops: ${rrep.hopCount + 1})!`);

        // Flush any queued messages for this destination
        const queued = messageWaitQueue.get(rrep.targetDeviceId);
        if (queued && queued.length > 0) {
            console.log(`[QUEUE] Flushing ${queued.length} queued messages to ${rrep.targetDeviceId}...`);
            messageWaitQueue.delete(rrep.targetDeviceId);
            for (const item of queued) {
                dispatchDirectedMessage(item.message);
            }
        }
        return;
    }

    // Relay RREP back to the origin
    if (rrep.ttl <= 1) {
        stats.droppedExpiredTtl++;
        return;
    }

    const forwardedRrep: RouteReply = {
        ...rrep,
        ttl: rrep.ttl - 1,
        hopCount: rrep.hopCount + 1,
        routePath: [...rrep.routePath, DEVICE_ID]
    };

    // Use our routing table to direct RREP back towards originDeviceId
    const reverseRoute = getValidRoute(rrep.originDeviceId);
    if (reverseRoute) {
        sendDirected(reverseRoute.nextHop, forwardedRrep);
    } else {
        broadcastPacket(forwardedRrep, sourceSocket);
    }
}

// ==================== Phase 4: Directed Next-Hop Forwarding ====================
function dispatchDirectedMessage(message: Message): void {
    const route = getValidRoute(message.receiverDeviceId);

    if (route) {
        stats.directedForwarded++;
        console.log(`🎯 [DIRECTED FORWARD] Message ${message.messageId} -> Target: ${message.receiverDeviceId} via NextHop: ${route.nextHop} (${route.hopDistance} hops)`);
        sendDirected(route.nextHop, message);
    } else {
        // Queue message and trigger on-demand route discovery
        console.log(`⏳ [ROUTE MISS] No route for ${message.receiverDeviceId}. Queueing message and initiating discovery...`);
        if (!messageWaitQueue.has(message.receiverDeviceId)) {
            messageWaitQueue.set(message.receiverDeviceId, []);
        }
        messageWaitQueue.get(message.receiverDeviceId)!.push({ message, ttl: message.ttl });
        initiateRouteDiscovery(message.receiverDeviceId);
    }
}

function handleMeshMessage(message: Message, sourceSocket: WebSocket): void {
    if (isDuplicate(message.messageId)) {
        return;
    }

    const currentPath = message.routePath ? [...message.routePath] : [];

    // Loop detection
    if (currentPath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    const isTarget = message.receiverDeviceId === DEVICE_ID;
    const isBroadcast = message.receiverDeviceId === "*";

    // Delivered
    if (isTarget || isBroadcast) {
        stats.deliveredMessages++;
        const finalRoute = [...currentPath, DEVICE_ID];
        console.log("\n-------------------------------------------");
        console.log(`📬 [DELIVERED] Message ${message.messageId}`);
        console.log(`From:      ${message.senderDeviceId}`);
        console.log(`To:        ${message.receiverDeviceId}`);
        console.log(`Payload:   ${message.payload}`);
        console.log(`Hop Count: ${message.hopCount}`);
        console.log(`Route:     ${finalRoute.join(" -> ")}`);
        console.log("-------------------------------------------\n");

        if (isTarget) {
            sendAck(message.senderDeviceId, message.messageId);
            return;
        }
    }

    // TTL Expiration
    if (message.ttl <= 1) {
        stats.droppedExpiredTtl++;
        console.log(`[DROP] TTL expired for message ${message.messageId}`);
        return;
    }

    // Relay
    stats.relayedMessages++;
    const relayedMessage: Message = {
        ...message,
        ttl: message.ttl - 1,
        hopCount: message.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    if (isBroadcast) {
        // Broadcasts are flooded
        stats.floodedForwarded++;
        broadcastPacket(relayedMessage, sourceSocket);
    } else {
        // Unicast messages are directed using nextHop from the Routing Table!
        const route = getValidRoute(relayedMessage.receiverDeviceId);
        if (route) {
            stats.directedForwarded++;
            console.log(`🔁 [INTELLIGENT RELAY] Routing ${relayedMessage.messageId} directly to nextHop: ${route.nextHop} (Target: ${relayedMessage.receiverDeviceId})`);
            sendDirected(route.nextHop, relayedMessage);
        } else {
            // Fallback to controlled flood if route was lost
            stats.floodedForwarded++;
            console.log(`⚠️ [FALLBACK RELAY] Route lost for ${relayedMessage.receiverDeviceId}. Falling back to neighbor forward.`);
            broadcastPacket(relayedMessage, sourceSocket);
        }
    }
}

// ==================== Phase 3 & 4: ACK Forwarding ====================
function sendAck(targetDeviceId: string, ackMessageId: string): void {
    stats.acksOriginated++;
    const ack: AckMessage = {
        type: "ACK",
        ackMessageId: ackMessageId,
        senderDeviceId: DEVICE_ID,
        receiverDeviceId: targetDeviceId,
        timestamp: Date.now(),
        ttl: 5,
        hopCount: 0,
        routePath: [DEVICE_ID]
    };

    isDuplicate(`ACK-${ack.ackMessageId}-${ack.senderDeviceId}`);
    console.log(`🔔 [ACK ORIGINATED] Confirming receipt of message ${ackMessageId} to ${targetDeviceId}`);

    const route = getValidRoute(targetDeviceId);
    if (route) {
        sendDirected(route.nextHop, ack);
    } else {
        broadcastPacket(ack);
    }
}

function handleAckMessage(ack: AckMessage, sourceSocket: WebSocket): void {
    const ackKey = `ACK-${ack.ackMessageId}-${ack.senderDeviceId}`;
    if (isDuplicate(ackKey)) {
        return;
    }

    const currentPath = ack.routePath ? [...ack.routePath] : [];

    if (currentPath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    if (ack.receiverDeviceId === DEVICE_ID) {
        stats.acksReceived++;
        const pending = pendingAcks.get(ack.ackMessageId);
        const finalRoute = [...currentPath, DEVICE_ID];

        if (pending) {
            clearTimeout(pending.timer);
            pendingAcks.delete(ack.ackMessageId);
            const rttMs = Date.now() - pending.sentAt;
            console.log("\n===========================================");
            console.log(`✅ [ACK CONFIRMED] Delivery of message ${ack.ackMessageId} confirmed!`);
            console.log(`From Recipient: ${ack.senderDeviceId}`);
            console.log(`Round-Trip RTT: ${rttMs}ms`);
            console.log(`ACK Route:      ${finalRoute.join(" -> ")}`);
            console.log("===========================================\n");
        }
        return;
    }

    if (ack.ttl <= 1) {
        stats.droppedExpiredTtl++;
        return;
    }

    stats.acksRelayed++;
    const relayedAck: AckMessage = {
        ...ack,
        ttl: ack.ttl - 1,
        hopCount: ack.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    const route = getValidRoute(relayedAck.receiverDeviceId);
    if (route) {
        sendDirected(route.nextHop, relayedAck);
    } else {
        broadcastPacket(relayedAck, sourceSocket);
    }
}

// ==================== Helper Sending Primitives ====================
function sendDirected(nextHopDeviceId: string, packet: MeshPacket): boolean {
    const socket = peers.get(nextHopDeviceId);
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(packet));
        return true;
    }
    return false;
}

function broadcastPacket(packet: MeshPacket, exceptSocket?: WebSocket): void {
    const payload = JSON.stringify(packet);
    for (const [peerId, socket] of peers.entries()) {
        if (socket !== exceptSocket && socket.readyState === WebSocket.OPEN) {
            socket.send(payload);
        }
    }
}

// ==================== Public Messaging API ====================
export function sendMessage(receiverDeviceId: string, payload: string, ttl: number = 5): Message {
    stats.originatedMessages++;
    const message: Message = {
        type: "MESSAGE",
        messageId: randomUUID(),
        senderDeviceId: DEVICE_ID,
        receiverDeviceId: receiverDeviceId,
        payload: payload,
        timestamp: Date.now(),
        ttl: ttl,
        hopCount: 0,
        routePath: [DEVICE_ID]
    };

    isDuplicate(message.messageId);

    if (receiverDeviceId !== "*") {
        const timer = setTimeout(() => {
            if (pendingAcks.has(message.messageId)) {
                pendingAcks.delete(message.messageId);
                console.log(`\n⚠️ [ACK TIMEOUT] Message ${message.messageId} to ${receiverDeviceId} unconfirmed after ${ACK_TIMEOUT_MS}ms\n`);
            }
        }, ACK_TIMEOUT_MS);

        pendingAcks.set(message.messageId, {
            message,
            sentAt: Date.now(),
            timer
        });

        // Use Directed Forwarding / Route Discovery
        dispatchDirectedMessage(message);
    } else {
        console.log(`\n📢 [BROADCAST ORIGINATING] "${payload}"`);
        broadcastPacket(message);
    }

    return message;
}

// ==================== Keepalive Heartbeat Interval ====================
setInterval(() => {
    const ping: HeartbeatMessage = {
        type: "PING",
        deviceId: DEVICE_ID,
        timestamp: Date.now()
    };
    const pingPayload = JSON.stringify(ping);
    for (const [peerId, socket] of peers.entries()) {
        if (socket.readyState === WebSocket.OPEN) {
            socket.send(pingPayload);
        }
    }
}, 15000);

// ==================== mDNS Auto-Discovery ====================
let discoveryService: PeerDiscovery | null = null;

if (ENABLE_MDNS) {
    discoveryService = new PeerDiscovery({
        deviceId: DEVICE_ID,
        port: PORT,
        onPeerDiscovered: (peer: DiscoveredPeer) => {
            if (!peers.has(peer.deviceId)) {
                console.log(`[DISCOVERY] Attempting auto-connection to ${peer.deviceId} (${peer.ip}:${peer.port})`);
                connectToPeer(`ws://${peer.ip}:${peer.port}`);
            }
        }
    });
    discoveryService.start(5000);
}

// ==================== Manual Initial Peer Connection ====================
if (MANUAL_PEER) {
    console.log(`[CONFIG] Connecting to manual peer: ${MANUAL_PEER}`);
    connectToPeer(MANUAL_PEER);
}

// ==================== Interactive Console ====================
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log("\nCommands:");
console.log("  send <targetDeviceId|*> <message>  - Send message (intelligent routing)");
console.log("  routes                             - View routing table");
console.log("  peers                              - List connected neighbors");
console.log("  stats                              - View mesh statistics");
console.log("  connect <host:port>                - Manually connect to peer");
console.log("  quit                               - Exit\n");

rl.on("line", (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;

    const parts = trimmed.split(" ");
    const cmd = parts[0].toLowerCase();

    if (cmd === "routes") {
        console.log(`\n=== Routing Table (${routingTable.size} entries) ===`);
        for (const [dest, entry] of routingTable.entries()) {
            const timeLeft = Math.max(0, Math.round((entry.expiresAt - Date.now()) / 1000));
            console.log(`  Destination: ${dest.padEnd(15)} NextHop: ${entry.nextHop.padEnd(15)} Hops: ${entry.hopDistance} (TTL: ${timeLeft}s)`);
        }
        console.log("=====================================\n");
    } else if (cmd === "peers") {
        console.log(`Connected neighbors (${peers.size}):`);
        for (const [id] of peers.entries()) {
            console.log(`  - ${id}`);
        }
    } else if (cmd === "stats") {
        console.log("\n=== Mesh Node Statistics ===");
        console.log(`Node ID:              ${DEVICE_ID}`);
        console.log(`Active Peers:         ${peers.size}`);
        console.log(`Known Routes:         ${routingTable.size}`);
        console.log(`Directed Forwards:    ${stats.directedForwarded}`);
        console.log(`Flooded Forwards:     ${stats.floodedForwarded}`);
        console.log(`Route Requests (RREQ):${stats.routeRequestsOriginated}`);
        console.log(`Route Replies (RREP): ${stats.routeRepliesOriginated}`);
        console.log(`Messages Originated:  ${stats.originatedMessages}`);
        console.log(`Messages Delivered:   ${stats.deliveredMessages}`);
        console.log(`ACKs Received:        ${stats.acksReceived}`);
        console.log("============================\n");
    } else if (cmd === "connect" && parts[1]) {
        connectToPeer(parts[1]);
    } else if (cmd === "send" && parts.length >= 3) {
        const target = parts[1];
        const payload = parts.slice(2).join(" ");
        sendMessage(target, payload);
    } else if (cmd === "quit" || cmd === "exit") {
        if (discoveryService) discoveryService.stop();
        server.close();
        process.exit(0);
    } else {
        console.log("Unknown command. Commands: send <target|*> <message> | routes | peers | stats | connect <host:port>");
    }
});
