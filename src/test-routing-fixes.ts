import assert from "assert";
import fs from "fs";
import { spawn, ChildProcess, execSync } from "child_process";
import { MessageStore, safeParseStringArray } from "./storage.js";
import { Message, SosAlert } from "./types.js";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));
const TEST_DIR = "./data/test-routing-fixes";

if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
}
fs.mkdirSync(TEST_DIR, { recursive: true });

function testStorageFixes(): void {
    const store = new MessageStore("Test-Store", TEST_DIR);
    const msg: Message = {
        type: "MESSAGE",
        messageId: "msg-1",
        senderDeviceId: "Node-A",
        receiverDeviceId: "Node-B",
        payload: "hello",
        timestamp: Date.now(),
        ttl: 5,
        hopCount: 0,
        routePath: ["Node-A"]
    };

    // 1. IN_TRANSIT message should not be returned before retryAfterMs (6000ms)
    store.saveMessage(msg, "IN_TRANSIT");
    assert.strictEqual(store.getPendingMessagesFor("Node-B", 6000).length, 0, "Should not resend in-flight message immediately");

    // 2. If retryAfterMs = 0 (simulating elapsed timeout), it should be returned with attempt = 1
    const due = store.getPendingMessagesFor("Node-B", 0);
    assert.strictEqual(due.length, 1, "Should return message once retry window elapses");
    assert.strictEqual(due[0].attempt, 1);

    // 3. DELIVERED status must not be overwritten back to PENDING
    store.markDelivered("msg-1");
    store.saveMessage(msg, "PENDING");
    assert.strictEqual(store.getMessageStatus("msg-1"), "DELIVERED", "DELIVERED must not revert to PENDING");

    // 4. saveSosAlert updates severity/peopleCount/description on conflict while preserving DISPATCHED status
    const sos: SosAlert = {
        type: "SOS",
        sosId: "sos-1",
        senderDeviceId: "Node-A",
        emergencyType: "TRAPPED",
        severity: "MODERATE",
        peopleCount: 1,
        description: "Initial report",
        location: { landmark: "Roof", anchorDeviceId: "Node-A", hopsFromAnchor: 0, estimatedRadiusMeters: 50, proximityChain: ["Node-A"] },
        status: "OPEN",
        timestamp: Date.now(),
        ttl: 10,
        hopCount: 0,
        routePath: ["Node-A"]
    };
    store.saveSosAlert(sos);
    store.saveSosAlert({ ...sos, severity: "CRITICAL", peopleCount: 4, description: "Water rising" });
    const updatedSos = store.getSosAlert("sos-1");
    assert.strictEqual(updatedSos?.severity, "CRITICAL");
    assert.strictEqual(updatedSos?.peopleCount, 4);
    assert.strictEqual(updatedSos?.description, "Water rising");

    store.updateSosStatus("sos-1", "DISPATCHED", "Rescue-1", "En route");
    store.saveSosAlert({ ...sos, status: "OPEN", severity: "CRITICAL", peopleCount: 4, description: "Water rising" });
    assert.strictEqual(store.getSosAlert("sos-1")?.status, "DISPATCHED", "DISPATCHED must not revert to OPEN");

    // 5. safeParseStringArray handles malformed JSON without throwing
    assert.deepStrictEqual(safeParseStringArray("{bad json"), []);
    assert.deepStrictEqual(safeParseStringArray(null), []);

    // 6. purgeExpired marks expired messages EXPIRED
    store.saveMessage({ ...msg, messageId: "msg-expired" }, "PENDING", -1);
    assert.strictEqual(store.purgeExpired(), 1);
    assert.strictEqual(store.getMessageStatus("msg-expired"), "EXPIRED");

    store.close();
    console.log("✅ Storage retry-window, SOS upsert, safe JSON parse & purgeExpired checks passed.");
}

async function testRreqBackoffAndCap(): Promise<void> {
    execSync("fuser -k 7061/tcp 7062/tcp 2>/dev/null || true");
    const launch = (port: string, name: string, peer?: string): { proc: ChildProcess; stdout: string[] } => {
        const args = ["tsx", "src/node.ts", port, name];
        if (peer) args.push(peer);
        const proc = spawn("npx", args, {
            env: { ...process.env, ENABLE_MDNS: "false", HOST: "127.0.0.1", STORAGE_DIR: TEST_DIR },
            stdio: ["pipe", "pipe", "pipe"],
            detached: true
        });
        const stdout: string[] = [];
        proc.stdout?.on("data", (d) => stdout.push(d.toString()));
        return { proc, stdout };
    };

    const nodeB = launch("7062", "Node-B");
    await delay(1000);
    const nodeA = launch("7061", "Node-A", "ws://127.0.0.1:7062");
    await delay(1500);

    try {
        // Send message to a non-existent destination; RREQ should be capped at 3 attempts, not flood every 3s
        nodeA.proc.stdin?.write("msg Node-Ghost Hello_Ghost\n");
        await delay(10500);

        nodeA.proc.stdin?.write("stats\n");
        await delay(500);

        const combined = nodeA.stdout.join("");
        const match = combined.match(/"floodedForwarded":\s*(\d+)/);
        const flooded = match ? parseInt(match[1], 10) : -1;
        assert.ok(flooded >= 1 && flooded <= 3, `Expected at most 3 RREQ floods with backoff, got ${flooded}`);
        console.log(`✅ RREQ backoff & cap check passed (floodedForwarded=${flooded} over 10.5s).`);
    } finally {
        try { if (nodeA.proc.pid) process.kill(-nodeA.proc.pid, "SIGKILL"); } catch {}
        try { if (nodeB.proc.pid) process.kill(-nodeB.proc.pid, "SIGKILL"); } catch {}
        execSync("fuser -k 7061/tcp 7062/tcp 2>/dev/null || true");
    }
}

async function main(): Promise<void> {
    testStorageFixes();
    await testRreqBackoffAndCap();
    console.log("🎉 All routing & storage fix checks passed!");
    process.exit(0);
}

main().catch((err) => {
    console.error("❌ Verification failed:", err);
    process.exit(1);
});
