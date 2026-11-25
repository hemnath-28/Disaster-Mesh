import { DatabaseSync } from "node:sqlite";
import fs from "fs";
import path from "path";
import { Message, SosAlert, DisasterBulletin, EmergencyStatus } from "./types.js";

export type MessageStatus = "PENDING" | "IN_TRANSIT" | "DELIVERED" | "EXPIRED";

export function safeParseStringArray(raw: string | null | undefined): string[] {
    if (!raw) return [];
    try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return [];
    }
}

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

export interface StoredSosRecord {
    sosId: string;
    senderDeviceId: string;
    emergencyType: string;
    severity: string;
    peopleCount: number;
    description: string;
    landmark: string;
    anchorDeviceId: string;
    hopsFromAnchor: number;
    estimatedRadiusMeters: number;
    proximityChainJson: string;
    status: EmergencyStatus;
    rescuerDeviceId: string | null;
    rescuerInstructions: string | null;
    createdAt: number;
    updatedAt: number;
}

export interface StoredBulletinRecord {
    bulletinId: string;
    senderDeviceId: string;
    hazardType: string;
    announcement: string;
    timestamp: number;
    hopCount: number;
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
            -- Standard Message Store
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

            -- Phase 6: Emergency SOS Triage Registry
            CREATE TABLE IF NOT EXISTS sos_alerts (
                sos_id TEXT PRIMARY KEY,
                sender_device_id TEXT NOT NULL,
                emergency_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                people_count INTEGER NOT NULL,
                description TEXT NOT NULL,
                landmark TEXT,
                anchor_device_id TEXT,
                hops_from_anchor INTEGER DEFAULT 0,
                estimated_radius_meters INTEGER DEFAULT 50,
                proximity_chain_json TEXT NOT NULL,
                status TEXT NOT NULL,
                rescuer_device_id TEXT,
                rescuer_instructions TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sos_status 
            ON sos_alerts (status);

            -- Phase 6: Public Hazard Bulletins
            CREATE TABLE IF NOT EXISTS disaster_bulletins (
                bulletin_id TEXT PRIMARY KEY,
                sender_device_id TEXT NOT NULL,
                hazard_type TEXT NOT NULL,
                announcement TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                hop_count INTEGER NOT NULL
            );
        `);
    }

    // ==================== Phase 5: Standard Message Store ====================
    public saveMessage(message: Message, status: MessageStatus = "PENDING", retentionHours: number = 24): void {
        const now = Date.now();
        const expiresAt = now + retentionHours * 60 * 60 * 1000;
        const routePathJson = JSON.stringify(message.routePath ?? []);
        const lastAttemptAt = status === "IN_TRANSIT" ? now : null;

        const stmt = this.db.prepare(`
            INSERT INTO messages (
                message_id, sender_device_id, receiver_device_id, payload,
                timestamp, ttl, hop_count, route_path_json, status,
                created_at, expires_at, retry_count, last_attempt_at, delivered_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, 0, ?, NULL
            )
            ON CONFLICT(message_id) DO UPDATE SET
                status = CASE WHEN messages.status = 'DELIVERED' THEN 'DELIVERED' ELSE excluded.status END,
                retry_count = messages.retry_count + 1,
                last_attempt_at = COALESCE(excluded.last_attempt_at, messages.last_attempt_at)
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
            expiresAt,
            lastAttemptAt
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

    public getMessageStatus(messageId: string): MessageStatus | undefined {
        const stmt = this.db.prepare(`SELECT status FROM messages WHERE message_id = ?`);
        const row = stmt.get(messageId) as any;
        return row ? (row.status as MessageStatus) : undefined;
    }

    public getPendingMessagesFor(receiverDeviceId: string, retryAfterMs: number = 6000): Message[] {
        const now = Date.now();
        const retryCutoff = now - retryAfterMs;
        const stmt = this.db.prepare(`
            SELECT * FROM messages
            WHERE receiver_device_id = ?
              AND (
                status = 'PENDING'
                OR (status = 'IN_TRANSIT' AND (last_attempt_at IS NULL OR last_attempt_at <= ?))
              )
              AND retry_count <= 3
              AND expires_at > ?
            ORDER BY created_at ASC
        `);

        const rows = stmt.all(receiverDeviceId, retryCutoff, now) as any[];
        return rows.map((row) => ({
            type: "MESSAGE" as const,
            messageId: row.message_id,
            senderDeviceId: row.sender_device_id,
            receiverDeviceId: row.receiver_device_id,
            payload: row.payload,
            timestamp: row.timestamp,
            ttl: row.ttl,
            hopCount: row.hop_count,
            routePath: safeParseStringArray(row.route_path_json),
            attempt: (row.retry_count ?? 0) + 1
        }));
    }

    public getAllPendingReceivers(retryAfterMs: number = 6000): string[] {
        const now = Date.now();
        const retryCutoff = now - retryAfterMs;
        const stmt = this.db.prepare(`
            SELECT DISTINCT receiver_device_id
            FROM messages
            WHERE (
                status = 'PENDING'
                OR (status = 'IN_TRANSIT' AND (last_attempt_at IS NULL OR last_attempt_at <= ?))
              )
              AND retry_count <= 3
              AND expires_at > ?
        `);

        const rows = stmt.all(retryCutoff, now) as any[];
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

    // ==================== Phase 6: Emergency SOS & Triage Registry ====================
    public saveSosAlert(alert: SosAlert): void {
        const now = Date.now();
        const chainJson = JSON.stringify(alert.location.proximityChain ?? []);

        const stmt = this.db.prepare(`
            INSERT INTO sos_alerts (
                sos_id, sender_device_id, emergency_type, severity,
                people_count, description, landmark, anchor_device_id,
                hops_from_anchor, estimated_radius_meters, proximity_chain_json,
                status, rescuer_device_id, rescuer_instructions, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, NULL, NULL, ?, ?
            )
            ON CONFLICT(sos_id) DO UPDATE SET
                emergency_type = excluded.emergency_type,
                severity = excluded.severity,
                people_count = excluded.people_count,
                description = excluded.description,
                landmark = excluded.landmark,
                status = CASE WHEN sos_alerts.status = 'OPEN' THEN excluded.status ELSE sos_alerts.status END,
                hops_from_anchor = MIN(sos_alerts.hops_from_anchor, excluded.hops_from_anchor),
                estimated_radius_meters = CASE
                    WHEN excluded.hops_from_anchor < sos_alerts.hops_from_anchor THEN excluded.estimated_radius_meters
                    ELSE sos_alerts.estimated_radius_meters
                END,
                proximity_chain_json = CASE
                    WHEN excluded.hops_from_anchor < sos_alerts.hops_from_anchor THEN excluded.proximity_chain_json
                    ELSE sos_alerts.proximity_chain_json
                END,
                updated_at = excluded.updated_at
        `);

        stmt.run(
            alert.sosId,
            alert.senderDeviceId,
            alert.emergencyType,
            alert.severity,
            alert.peopleCount,
            alert.description,
            alert.location.landmark ?? "Unknown",
            alert.location.anchorDeviceId ?? "None",
            alert.location.hopsFromAnchor,
            alert.location.estimatedRadiusMeters,
            chainJson,
            alert.status,
            now,
            now
        );
    }

    public updateSosStatus(
        sosId: string,
        status: EmergencyStatus,
        rescuerDeviceId?: string,
        instructions?: string
    ): void {
        const now = Date.now();
        const stmt = this.db.prepare(`
            UPDATE sos_alerts
            SET status = ?,
                rescuer_device_id = COALESCE(?, rescuer_device_id),
                rescuer_instructions = COALESCE(?, rescuer_instructions),
                updated_at = ?
            WHERE sos_id = ?
        `);
        stmt.run(status, rescuerDeviceId ?? null, instructions ?? null, now, sosId);
    }

    public getSosAlert(sosId: string): StoredSosRecord | undefined {
        const stmt = this.db.prepare(`
            SELECT * FROM sos_alerts WHERE sos_id = ?
        `);
        const row = stmt.get(sosId) as any;
        if (!row) return undefined;
        return this.mapSosRow(row);
    }

    public getAllSosAlerts(): StoredSosRecord[] {
        const stmt = this.db.prepare(`
            SELECT * FROM sos_alerts
            ORDER BY 
                CASE severity 
                    WHEN 'CRITICAL' THEN 1 
                    WHEN 'HIGH' THEN 2 
                    ELSE 3 
                END,
                created_at DESC
        `);
        const rows = stmt.all() as any[];
        return rows.map((r) => this.mapSosRow(r));
    }

    private mapSosRow(row: any): StoredSosRecord {
        return {
            sosId: row.sos_id,
            senderDeviceId: row.sender_device_id,
            emergencyType: row.emergency_type,
            severity: row.severity,
            peopleCount: row.people_count,
            description: row.description,
            landmark: row.landmark,
            anchorDeviceId: row.anchor_device_id,
            hopsFromAnchor: row.hops_from_anchor,
            estimatedRadiusMeters: row.estimated_radius_meters,
            proximityChainJson: row.proximity_chain_json,
            status: row.status as EmergencyStatus,
            rescuerDeviceId: row.rescuer_device_id,
            rescuerInstructions: row.rescuer_instructions,
            createdAt: row.created_at,
            updatedAt: row.updated_at
        };
    }

    // ==================== Phase 6: Disaster Bulletins ====================
    public saveBulletin(bulletin: DisasterBulletin): void {
        const stmt = this.db.prepare(`
            INSERT INTO disaster_bulletins (
                bulletin_id, sender_device_id, hazard_type, announcement, timestamp, hop_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bulletin_id) DO NOTHING
        `);
        stmt.run(
            bulletin.bulletinId,
            bulletin.senderDeviceId,
            bulletin.hazardType,
            bulletin.announcement,
            bulletin.timestamp,
            bulletin.hopCount
        );
    }

    public getAllBulletins(): StoredBulletinRecord[] {
        const stmt = this.db.prepare(`
            SELECT * FROM disaster_bulletins
            ORDER BY timestamp DESC
        `);
        const rows = stmt.all() as any[];
        return rows.map((r) => ({
            bulletinId: r.bulletin_id,
            senderDeviceId: r.sender_device_id,
            hazardType: r.hazard_type,
            announcement: r.announcement,
            timestamp: r.timestamp,
            hopCount: r.hop_count
        }));
    }

    public close(): void {
        this.db.close();
    }
}
