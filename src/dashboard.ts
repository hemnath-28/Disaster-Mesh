import http from "http";
import fs from "fs";
import path from "path";
import { WebSocketServer, WebSocket } from "ws";
import { EventEmitter } from "events";
import { MessageStore } from "./storage.js";
import { RouteEntry, HazardType } from "./types.js";

export interface DashboardOptions {
    host: string;
    port: number;
    nodeDeviceId: string;
    storage: MessageStore;
    respondToSos: (sosId: string, status: "DISPATCHED" | "STANDBY" | "RESOLVED", instructions: string, eta?: string) => boolean;
    broadcastBulletin: (hazardType: HazardType, announcement: string) => any;
    getActivePeers: () => string[];
    getRoutingTable: () => RouteEntry[];
    getStats: () => any;
    meshEvents: EventEmitter;
}

let httpServer: http.Server | null = null;
let wss: WebSocketServer | null = null;
let dashboardClients = new Set<WebSocket>();

export function startDashboardServer(options: DashboardOptions): http.Server {
    if (httpServer) {
        return httpServer;
    }

    // Resolve index.html path relative to project root or current directory
    const candidates = [
        path.join(process.cwd(), "src", "web", "index.html"),
        path.join(process.cwd(), "dist", "web", "index.html"),
        path.resolve("./src/web/index.html")
    ];

    let htmlContent = "";
    for (const p of candidates) {
        if (fs.existsSync(p)) {
            htmlContent = fs.readFileSync(p, "utf-8");
            break;
        }
    }

    if (!htmlContent) {
        htmlContent = `<!DOCTYPE html><html><body><h1>DisasterMesh Dashboard: index.html not found</h1></body></html>`;
    }

    // 1. Create HTTP Server for HTML delivery
    httpServer = http.createServer((req, res) => {
        const url = req.url || "/";

        if (url === "/" || url === "/index.html") {
            let content = htmlContent;
            for (const p of candidates) {
                if (fs.existsSync(p)) {
                    try { content = fs.readFileSync(p, "utf-8"); break; } catch (_) {}
                }
            }
            res.writeHead(200, {
                "Content-Type": "text/html; charset=utf-8",
                "Cache-Control": "no-cache"
            });
            res.end(content);
            return;
        }

        if (url === "/api/health") {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ status: "OK", node: options.nodeDeviceId, peers: options.getActivePeers().length }));
            return;
        }

        res.writeHead(404, { "Content-Type": "text/plain" });
        res.end("Not Found");
    });

    // 2. Attach WebSocket server for dashboard operators
    wss = new WebSocketServer({
        server: httpServer,
        path: "/dashboard-ws"
    });

    function broadcastToDashboard(payload: any) {
        const data = JSON.stringify(payload);
        for (const client of dashboardClients) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(data);
            }
        }
    }

    function sendSnapshot(client: WebSocket) {
        if (client.readyState !== WebSocket.OPEN) return;
        const snapshot = {
            type: "INIT_STATE",
            nodeDeviceId: options.nodeDeviceId,
            peers: options.getActivePeers(),
            routes: options.getRoutingTable(),
            sosAlerts: options.storage.getAllSosAlerts(),
            bulletins: options.storage.getAllBulletins().slice(0, 30),
            stats: options.getStats(),
            timestamp: Date.now()
        };
        client.send(JSON.stringify(snapshot));
    }

    wss.on("connection", (socket: WebSocket) => {
        dashboardClients.add(socket);

        // Send full initial state snapshot immediately
        sendSnapshot(socket);

        socket.on("message", (raw) => {
            try {
                const action = JSON.parse(raw.toString());

                if (action.type === "ACTION_DISPATCH") {
                    const ok = options.respondToSos(
                        action.sosId,
                        action.status || "DISPATCHED",
                        action.instructions || "Rescue team deployed",
                        action.eta || "15 mins"
                    );
                    if (ok) {
                        const updated = options.storage.getSosAlert(action.sosId);
                        if (updated) {
                            broadcastToDashboard({ type: "SOS_UPSERT", alert: updated });
                        }
                    }
                } else if (action.type === "ACTION_BULLETIN") {
                    const bulletin = options.broadcastBulletin(
                        action.hazardType as HazardType,
                        action.announcement
                    );
                    if (bulletin) {
                        broadcastToDashboard({ type: "BULLETIN_NEW", bulletin });
                    }
                } else if (action.type === "REQ_SYNC") {
                    sendSnapshot(socket);
                }
            } catch (err: any) {
                console.error("[DASHBOARD WS ERROR] Failed to handle message:", err.message);
            }
        });

        socket.on("close", () => {
            dashboardClients.delete(socket);
        });

        socket.on("error", () => {
            dashboardClients.delete(socket);
        });
    });

    // 3. Hook into internal mesh event bus
    options.meshEvents.on("sos:upsert", (alert) => {
        broadcastToDashboard({ type: "SOS_UPSERT", alert });
    });

    options.meshEvents.on("peer:change", (peers) => {
        broadcastToDashboard({ type: "PEERS_CHANGE", peers });
    });

    options.meshEvents.on("routes:change", (routes) => {
        broadcastToDashboard({ type: "ROUTES_CHANGE", routes });
    });

    options.meshEvents.on("bulletin:new", (bulletin) => {
        broadcastToDashboard({ type: "BULLETIN_NEW", bulletin });
    });

    options.meshEvents.on("stats:tick", (stats) => {
        broadcastToDashboard({ type: "STATS_TICK", stats });
    });

    httpServer.listen(options.port, options.host, () => {
        console.log(`[DASHBOARD] Operations Console listening at http://${options.host === "0.0.0.0" ? "localhost" : options.host}:${options.port}`);
    });

    return httpServer;
}

export function stopDashboardServer(): void {
    for (const client of dashboardClients) {
        try {
            client.close();
        } catch (_) {}
    }
    dashboardClients.clear();

    if (wss) {
        try {
            wss.close();
        } catch (_) {}
        wss = null;
    }

    if (httpServer) {
        try {
            httpServer.close();
        } catch (_) {}
        httpServer = null;
    }
}
