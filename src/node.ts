import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import readline from "readline";
import { Message, AckMessage, HeartbeatMessage, IdentifyMessage, MeshPacket, DiscoveredPeer } from "./types.js";
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
// Known peer URLs for reconnection: deviceId/URL -> string
const knownPeerUrls = new Set<string>();

// Track pending acknowledgments: messageId -> PendingAck
interface PendingAck {
    message: Message;
    sentAt: number;
    timer: NodeJS.Timeout;
}
const pendingAcks = new Map<string, PendingAck>();
const ACK_TIMEOUT_MS = 6000;

// Deduplication cache: stores messageId/packetKey and expiry timestamp
const seenPackets = new Map<string, number>();
const SEEN_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

// Node Metrics
const stats = {
    originatedMessages: 0,
    deliveredMessages: 0,
    relayedMessages: 0,
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

    // Prevent connecting to ourselves
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
    // 1. Send IDENTIFY immediately
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

            // Handle ACK Messages
            if (raw.type === "ACK") {
                handleAckMessage(raw as AckMessage, socket);
                return;
            }

            // Handle Standard Messages
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

            // Reconnection attempt if known URL
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

// ==================== Phase 3: Mesh Forwarding & Routing Guard ====================
function handleMeshMessage(message: Message, sourceSocket: WebSocket): void {
    // 1. Deduplication check
    if (isDuplicate(message.messageId)) {
        return;
    }

    const currentPath = message.routePath ? [...message.routePath] : [];

    // 2. Loop Detection: Check if our device is already in the route path
    if (currentPath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        console.log(`[LOOP GUARD] Dropped message ${message.messageId}: Self (${DEVICE_ID}) already in route path: ${currentPath.join(" -> ")}`);
        return;
    }

    const isTarget = message.receiverDeviceId === DEVICE_ID;
    const isBroadcast = message.receiverDeviceId === "*";

    // 3. Delivery
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

        // Send ACK back to sender for unicast delivery
        if (isTarget) {
            sendAck(message.senderDeviceId, message.messageId);
            return;
        }
    }

    // 4. TTL Expiration Check
    if (message.ttl <= 1) {
        stats.droppedExpiredTtl++;
        console.log(`[DROP] TTL expired for message ${message.messageId} destined for ${message.receiverDeviceId}`);
        return;
    }

    // 5. Relay
    stats.relayedMessages++;
    const relayedMessage: Message = {
        ...message,
        ttl: message.ttl - 1,
        hopCount: message.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    console.log(`🔁 [RELAYING] Message ${message.messageId} -> Target: ${message.receiverDeviceId} (TTL: ${relayedMessage.ttl}, Hops: ${relayedMessage.hopCount})`);
    relayToNeighbors(relayedMessage, sourceSocket);
}

// ==================== Phase 3: Acknowledgment (ACK) Engine ====================
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

    const ackKey = `ACK-${ack.ackMessageId}-${ack.senderDeviceId}`;
    isDuplicate(ackKey);

    console.log(`🔔 [ACK ORIGINATED] Confirming receipt of message ${ackMessageId} back to ${targetDeviceId}`);
    relayToNeighbors(ack);
}

function handleAckMessage(ack: AckMessage, sourceSocket: WebSocket): void {
    const ackKey = `ACK-${ack.ackMessageId}-${ack.senderDeviceId}`;
    if (isDuplicate(ackKey)) {
        return;
    }

    const currentPath = ack.routePath ? [...ack.routePath] : [];

    // Loop detection
    if (currentPath.includes(DEVICE_ID)) {
        stats.droppedLoops++;
        return;
    }

    // Are we the original sender waiting for this ACK?
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
            console.log(`ACK Hop Count:  ${ack.hopCount}`);
            console.log(`ACK Route:      ${finalRoute.join(" -> ")}`);
            console.log("===========================================\n");
        } else {
            console.log(`✅ [ACK RECEIVED] Message ${ack.ackMessageId} confirmed by ${ack.senderDeviceId}`);
        }
        return;
    }

    // TTL Check
    if (ack.ttl <= 1) {
        stats.droppedExpiredTtl++;
        console.log(`[DROP ACK] TTL expired for ACK of message ${ack.ackMessageId}`);
        return;
    }

    // Relay ACK back towards sender
    stats.acksRelayed++;
    const relayedAck: AckMessage = {
        ...ack,
        ttl: ack.ttl - 1,
        hopCount: ack.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    console.log(`🔁 [RELAYING ACK] For message ${ack.ackMessageId} -> Target: ${ack.receiverDeviceId} (TTL: ${relayedAck.ttl})`);
    relayToNeighbors(relayedAck, sourceSocket);
}

function relayToNeighbors(packet: MeshPacket, exceptSocket?: WebSocket): void {
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

    // Mark our own sent message in cache to prevent bounce-back
    isDuplicate(message.messageId);

    // Track ACK for unicast messages
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
    }

    console.log(`\n🚀 [ORIGINATING] Message ${message.messageId} to ${receiverDeviceId}: "${payload}"`);
    relayToNeighbors(message);

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
console.log("  send <targetDeviceId|*> <message>  - Send message (tracks delivery ACK)");
console.log("  peers                              - List connected peers");
console.log("  stats                              - View node mesh statistics");
console.log("  connect <host:port>                - Manually connect to peer");
console.log("  quit                               - Exit\n");

rl.on("line", (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;

    const parts = trimmed.split(" ");
    const cmd = parts[0].toLowerCase();

    if (cmd === "peers") {
        console.log(`Connected peers (${peers.size}):`);
        for (const [id] of peers.entries()) {
            console.log(`  - ${id}`);
        }
    } else if (cmd === "stats") {
        console.log("\n=== Mesh Node Statistics ===");
        console.log(`Node ID:             ${DEVICE_ID}`);
        console.log(`Active Peers:        ${peers.size}`);
        console.log(`Pending ACKs:        ${pendingAcks.size}`);
        console.log(`Messages Originated: ${stats.originatedMessages}`);
        console.log(`Messages Delivered:  ${stats.deliveredMessages}`);
        console.log(`Messages Relayed:    ${stats.relayedMessages}`);
        console.log(`ACKs Originated:     ${stats.acksOriginated}`);
        console.log(`ACKs Received:       ${stats.acksReceived}`);
        console.log(`ACKs Relayed:        ${stats.acksRelayed}`);
        console.log(`Dropped (Duplicates):${stats.droppedDuplicates}`);
        console.log(`Dropped (Loops):     ${stats.droppedLoops}`);
        console.log(`Dropped (TTL Expired):${stats.droppedExpiredTtl}`);
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
        console.log("Unknown command. Commands: send <target|*> <message> | peers | stats | connect <host:port>");
    }
});
