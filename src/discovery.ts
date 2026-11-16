import mdns from "multicast-dns";
import os from "os";
import { DiscoveredPeer } from "./types.js";

const SERVICE = "_disastermesh._tcp.local";

export function getLocalIP(): string {
    const interfaces = os.networkInterfaces();

    for (const name of Object.keys(interfaces)) {
        const networkInterface = interfaces[name];
        if (!networkInterface) {
            continue;
        }

        for (const address of networkInterface) {
            if (address.family === "IPv4" && !address.internal) {
                return address.address;
            }
        }
    }

    return "127.0.0.1";
}

export interface DiscoveryOptions {
    deviceId: string;
    port: number;
    onPeerDiscovered?: (peer: DiscoveredPeer) => void;
}

export class PeerDiscovery {
    private mdnsInstance: any;
    private deviceId: string;
    private port: number;
    private localIp: string;
    private hostname: string;
    private intervalId?: NodeJS.Timeout;
    private discoveredPeers = new Map<string, DiscoveredPeer>();
    private onPeerDiscovered?: (peer: DiscoveredPeer) => void;

    constructor(options: DiscoveryOptions) {
        this.deviceId = options.deviceId;
        this.port = options.port;
        this.onPeerDiscovered = options.onPeerDiscovered;
        this.localIp = getLocalIP();
        this.hostname = `node-${this.deviceId}.local`;
        this.mdnsInstance = mdns();
        this.setupHandlers();
    }

    private setupHandlers(): void {
        this.mdnsInstance.on("error", (error: any) => {
            console.error("mDNS error:", error);
        });

        // Respond when another device queries for DisasterMesh service
        this.mdnsInstance.on("query", (query: any) => {
            for (const question of query.questions) {
                if (question.name === SERVICE && question.type === "PTR") {
                    this.mdnsInstance.respond({
                        answers: [
                            {
                                name: SERVICE,
                                type: "PTR",
                                ttl: 120,
                                data: this.hostname
                            }
                        ],
                        additionals: [
                            {
                                name: this.hostname,
                                type: "SRV",
                                ttl: 120,
                                data: {
                                    port: this.port,
                                    priority: 0,
                                    weight: 0,
                                    target: this.hostname
                                }
                            },
                            {
                                name: this.hostname,
                                type: "A",
                                ttl: 120,
                                data: this.localIp
                            },
                            {
                                name: this.hostname,
                                type: "TXT",
                                ttl: 120,
                                data: [`deviceId=${this.deviceId}`]
                            }
                        ]
                    });
                }
            }
        });

        // Parse responses from other nodes
        this.mdnsInstance.on("response", (response: any) => {
            let discoveredHostname: string | undefined;
            let discoveredIP: string | undefined;
            let discoveredPort: number | undefined;
            let discoveredDeviceId: string | undefined;

            for (const answer of response.answers) {
                if (answer.name === SERVICE && answer.type === "PTR") {
                    discoveredHostname = answer.data;
                }
            }

            if (!discoveredHostname) {
                return;
            }

            for (const answer of response.additionals ?? []) {
                if (answer.type === "SRV" && answer.name === discoveredHostname) {
                    discoveredPort = answer.data.port;
                }
                if (answer.type === "A" && answer.name === discoveredHostname) {
                    discoveredIP = answer.data;
                }
                if (answer.type === "TXT" && answer.name === discoveredHostname) {
                    for (const text of answer.data) {
                        const value = text.toString();
                        if (value.startsWith("deviceId=")) {
                            discoveredDeviceId = value.substring("deviceId=".length);
                        }
                    }
                }
            }

            // Ignore self
            if (!discoveredDeviceId || discoveredDeviceId === this.deviceId) {
                return;
            }

            if (discoveredIP && discoveredPort && discoveredDeviceId) {
                const peerInfo: DiscoveredPeer = {
                    deviceId: discoveredDeviceId,
                    ip: discoveredIP,
                    port: discoveredPort
                };

                const isNew = !this.discoveredPeers.has(discoveredDeviceId);
                this.discoveredPeers.set(discoveredDeviceId, peerInfo);

                if (isNew) {
                    console.log(`[mDNS] Discovered new peer: ${discoveredDeviceId} at ${discoveredIP}:${discoveredPort}`);
                    if (this.onPeerDiscovered) {
                        this.onPeerDiscovered(peerInfo);
                    }
                }
            }
        });
    }

    public start(intervalMs: number = 5000): void {
        this.query();
        this.intervalId = setInterval(() => {
            this.query();
        }, intervalMs);
    }

    public query(): void {
        this.mdnsInstance.query([
            {
                name: SERVICE,
                type: "PTR"
            }
        ]);
    }

    public stop(): void {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
        try {
            this.mdnsInstance.destroy();
        } catch (_) {}
    }

    public getDiscoveredPeers(): Map<string, DiscoveredPeer> {
        return this.discoveredPeers;
    }
}