import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import readline from "readline";
import { Message, IdentifyMessage, DiscoveredPeer } from "./types.js";
import { PeerDiscovery } from "./discovery.js";

// Configuration via environment or arguments
const HOST = process.env.HOST || "0.0.0.0";
const PORT = parseInt(process.env.PORT || process.argv[2] || "5000", 10);
const DEVICE_ID = process.env.DEVICE_ID || process.argv[3] || `Node-${randomUUID().substring(0, 5)}`;
const MANUAL_PEER = process.env.PEER_URL || process.argv[4] || ""; // e.g. ws://127.0.0.1:5001 or 127.0.0.1:5001
const ENABLE_MDNS = process.env.ENABLE_MDNS !== "false";

console.log("=========================================");
console.log(` DisasterMesh Node: ${DEVICE_ID}`);
console.log(` WebSocket Port:    ${PORT}`);
console.log(` mDNS Auto-Discovery: ${ENABLE_MDNS ? "ENABLED" : "DISABLED"}`);
console.log("=========================================");

// Active connections: deviceId -> WebSocket
const peers = new Map<string, WebSocket>();

// Deduplication cache: stores messageId and expiry timestamp
const seenMessages = new Map<string, number>();
const SEEN_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

function isDuplicate(messageId: string): boolean {
    const now = Date.now();
    // Periodic lazy cleanup
    if (seenMessages.size > 1000) {
        for (const [id, expiry] of seenMessages.entries()) {
            if (now > expiry) {
                seenMessages.delete(id);
            }
        }
    }

    if (seenMessages.has(messageId)) {
        return true;
    }

    seenMessages.set(messageId, now + SEEN_CACHE_TTL_MS);
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
    if (url.includes(`:${PORT}`)) {
        return;
    }

    // Check if we are already connected to a peer with this URL (or checking after handshake)
    try {
        const socket = new WebSocket(url);

        socket.on("open", () => {
            console.log(`[CLIENT] Connected to peer at ${url}`);
            setupSocketHandlers(socket, "outbound");
        });

        socket.on("error", (err) => {
            console.log(`[CLIENT ERROR] Connection to ${url} failed: ${err.message}`);
        });
    } catch (e: any) {
        console.error(`[CLIENT EXCEPTION] Cannot connect to ${url}: ${e.message}`);
    }
}

function setupSocketHandlers(socket: WebSocket, direction: "inbound" | "outbound"): void {
    // 1. Send IDENTIFY immediately upon connection
    const identifyMsg: IdentifyMessage = {
        type: "IDENTIFY",
        deviceId: DEVICE_ID
    };
    socket.send(JSON.stringify(identifyMsg));

    let remoteDeviceId: string | null = null;

    socket.on("message", (data) => {
        try {
            const raw = JSON.parse(data.toString());

            // Handle Identification Handshake
            if (raw.type === "IDENTIFY") {
                const idMsg = raw as IdentifyMessage;
                remoteDeviceId = idMsg.deviceId;

                // Don't connect to ourselves if loopback occurs
                if (remoteDeviceId === DEVICE_ID) {
                    socket.close();
                    return;
                }

                // If already have an open connection with this peer, close the duplicate
                const existing = peers.get(remoteDeviceId);
                if (existing && existing !== socket && existing.readyState === WebSocket.OPEN) {
                    socket.close();
                    return;
                }

                peers.set(remoteDeviceId, socket);
                console.log(`[PEER] Identified: ${remoteDeviceId} (${direction}) | Total peers: ${peers.size}`);
                return;
            }

            // Handle Mesh Message
            if (raw.type === "MESSAGE") {
                const message = raw as Message;
                handleMeshMessage(message, socket);
            }
        } catch (err: any) {
            console.error("[WARN] Failed to parse message:", err.message);
        }
    });

    const cleanup = () => {
        if (remoteDeviceId && peers.get(remoteDeviceId) === socket) {
            peers.delete(remoteDeviceId);
            console.log(`[PEER] Disconnected: ${remoteDeviceId} | Remaining peers: ${peers.size}`);
        }
    };

    socket.on("close", cleanup);
    socket.on("error", cleanup);
}

// ==================== Phase 3: Mesh Forwarding Engine ====================
function handleMeshMessage(message: Message, sourceSocket: WebSocket): void {
    // 1. Deduplication check: drop if seen before
    if (isDuplicate(message.messageId)) {
        return;
    }

    const currentPath = message.routePath ? [...message.routePath] : [];

    // 2. Check if this node is the intended recipient
    const isTarget = message.receiverDeviceId === DEVICE_ID;
    const isBroadcast = message.receiverDeviceId === "*";

    if (isTarget || isBroadcast) {
        console.log("\n-------------------------------------------");
        console.log(`📬 [DELIVERED] Message ${message.messageId}`);
        console.log(`From:      ${message.senderDeviceId}`);
        console.log(`To:        ${message.receiverDeviceId}`);
        console.log(`Payload:   ${message.payload}`);
        console.log(`Hop Count: ${message.hopCount}`);
        console.log(`Route:     ${[...currentPath, DEVICE_ID].join(" -> ")}`);
        console.log("-------------------------------------------\n");

        if (isTarget) {
            // Unicast delivered to final destination! No relay needed.
            return;
        }
    }

    // 3. TTL Expiration Check
    if (message.ttl <= 1) {
        console.log(`[DROP] TTL expired for message ${message.messageId} destined for ${message.receiverDeviceId}`);
        return;
    }

    // 4. Relay: Decrement TTL, increment hopCount, append self to route
    const relayedMessage: Message = {
        ...message,
        ttl: message.ttl - 1,
        hopCount: message.hopCount + 1,
        routePath: [...currentPath, DEVICE_ID]
    };

    console.log(`🔁 [RELAYING] Message ${message.messageId} -> Target: ${message.receiverDeviceId} (TTL: ${relayedMessage.ttl}, Hops: ${relayedMessage.hopCount})`);

    // 5. Controlled Flooding: Forward to all connected peers EXCEPT source
    relayToNeighbors(relayedMessage, sourceSocket);
}

function relayToNeighbors(message: Message, exceptSocket?: WebSocket): void {
    const payload = JSON.stringify(message);
    for (const [peerId, socket] of peers.entries()) {
        if (socket !== exceptSocket && socket.readyState === WebSocket.OPEN) {
            socket.send(payload);
        }
    }
}

// ==================== Public Messaging API ====================
export function sendMessage(receiverDeviceId: string, payload: string, ttl: number = 5): Message {
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

    // Mark our own sent message in the cache to avoid echoing back
    isDuplicate(message.messageId);

    console.log(`\n🚀 [ORIGINATING] Message ${message.messageId} to ${receiverDeviceId}: "${payload}"`);

    // Broadcast outward to direct neighbors
    relayToNeighbors(message);

    return message;
}

// ==================== mDNS Auto-Discovery ====================
let discoveryService: PeerDiscovery | null = null;

if (ENABLE_MDNS) {
    discoveryService = new PeerDiscovery({
        deviceId: DEVICE_ID,
        port: PORT,
        onPeerDiscovered: (peer: DiscoveredPeer) => {
            // Auto connect if not connected
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
console.log("  send <targetDeviceId|*> <message>  - Send unicast or broadcast message");
console.log("  peers                              - List connected peers");
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
        console.log("Unknown command. Usage: send <target|*> <message> | peers | connect <host:port>");
    }
});
