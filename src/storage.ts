import { DatabaseSync } from "node:sqlite";
import fs from "fs";
import path from "path";
import { Message } from "./types.js";

export type MessageStatus = "PENDING" | "IN_TRANSIT" | "DELIVERED" | "EXPIRED";

export interface StoredMessageRecord {
    messageId: string;
    senderDeviceId: string;
    receiverDeviceId: string;
    payload: string;
    timestamp: number;
    ttl: number;
    hopCount: number;
    routePathJson: string;
    status: MessageStatus;
    createdAt: number;
    expiresAt: number;
    retryCount: number;
    lastAttemptAt: number | null;
    deliveredAt: number | null;
}

export class MessageStore {
    private db: DatabaseSync;

    constructor(nodeId: string, storageDir: string = "./data") {
        if (!fs.existsSync(storageDir)) {
            fs.mkdirSync(storageDir, { recursive: true });
        }

        const safeNodeId = nodeId.replace(/[^a-zA-Z0-9_-]/g, "_");
        const dbPath = path.join(storageDir, `store-${safeNodeId}.db`);
        this.db = new DatabaseSync(dbPath);
        this.initSchema();
    }

    private initSchema(): void {
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS messages (
                message_id TEXT PRIMARY KEY,
                sender_device_id TEXT NOT NULL,
                receiver_device_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                ttl INTEGER NOT NULL,
                hop_count INTEGER NOT NULL,
                route_path_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_attempt_at INTEGER,
                delivered_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_receiver_status 
            ON messages (receiver_device_id, status);

            CREATE INDEX IF NOT EXISTS idx_status_expires 
            ON messages (status, expires_at);
        `);
    }

    public saveMessage(message: Message, status: MessageStatus = "PENDING", retentionHours: number = 24): void {
        const now = Date.now();
        const expiresAt = now + retentionHours * 60 * 60 * 1000;
        const routePathJson = JSON.stringify(message.routePath ?? []);

        const stmt = this.db.prepare(`
            INSERT INTO messages (
                message_id, sender_device_id, receiver_device_id, payload,
                timestamp, ttl, hop_count, route_path_json, status,
                created_at, expires_at, retry_count, last_attempt_at, delivered_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, 0, NULL, NULL
            )
            ON CONFLICT(message_id) DO UPDATE SET
                status = excluded.status,
                retry_count = messages.retry_count + 1
        `);

        stmt.run(
            message.messageId,
            message.senderDeviceId,
            message.receiverDeviceId,
            message.payload,
            message.timestamp,
            message.ttl,
            message.hopCount,
            routePathJson,
            status,
            now,
            expiresAt
        );
    }

    public markInTransit(messageId: string): void {
        const stmt = this.db.prepare(`
            UPDATE messages
            SET status = 'IN_TRANSIT',
                retry_count = retry_count + 1,
                last_attempt_at = ?
            WHERE message_id = ?
        `);
        stmt.run(Date.now(), messageId);
    }

    public markDelivered(messageId: string): void {
        const stmt = this.db.prepare(`
            UPDATE messages
            SET status = 'DELIVERED',
                delivered_at = ?
            WHERE message_id = ?
        `);
        stmt.run(Date.now(), messageId);
    }

    public getPendingMessagesFor(receiverDeviceId: string): Message[] {
        const now = Date.now();
        const stmt = this.db.prepare(`
            SELECT * FROM messages
            WHERE receiver_device_id = ?
              AND status IN ('PENDING', 'IN_TRANSIT')
              AND expires_at > ?
            ORDER BY created_at ASC
        `);

        const rows = stmt.all(receiverDeviceId, now) as any[];
        return rows.map((row) => ({
            type: "MESSAGE" as const,
            messageId: row.message_id,
            senderDeviceId: row.sender_device_id,
            receiverDeviceId: row.receiver_device_id,
            payload: row.payload,
            timestamp: row.timestamp,
            ttl: row.ttl,
            hopCount: row.hop_count,
            routePath: JSON.parse(row.route_path_json)
        }));
    }

    public getAllPendingReceivers(): string[] {
        const now = Date.now();
        const stmt = this.db.prepare(`
            SELECT DISTINCT receiver_device_id
            FROM messages
            WHERE status IN ('PENDING', 'IN_TRANSIT')
              AND expires_at > ?
        `);

        const rows = stmt.all(now) as any[];
        return rows.map((r) => r.receiver_device_id);
    }

    public getAllStoredRecords(): StoredMessageRecord[] {
        const stmt = this.db.prepare(`
            SELECT * FROM messages
            ORDER BY created_at DESC
        `);

        const rows = stmt.all() as any[];
        return rows.map((row) => ({
            messageId: row.message_id,
            senderDeviceId: row.sender_device_id,
            receiverDeviceId: row.receiver_device_id,
            payload: row.payload,
            timestamp: row.timestamp,
            ttl: row.ttl,
            hopCount: row.hop_count,
            routePathJson: row.route_path_json,
            status: row.status as MessageStatus,
            createdAt: row.created_at,
            expiresAt: row.expires_at,
            retryCount: row.retry_count,
            lastAttemptAt: row.last_attempt_at,
            deliveredAt: row.delivered_at
        }));
    }

    public purgeExpired(): number {
        const now = Date.now();
        const stmt = this.db.prepare(`
            UPDATE messages
            SET status = 'EXPIRED'
            WHERE status IN ('PENDING', 'IN_TRANSIT')
              AND expires_at <= ?
        `);
        const result = stmt.run(now) as any;
        return result.changes ?? 0;
    }

    public close(): void {
        this.db.close();
    }
}
