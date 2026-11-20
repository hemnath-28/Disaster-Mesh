import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";
import { DatabaseSync } from "node:sqlite";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Phase 10 - Test 3: Diamond Multipath Deduplication");
console.log(" Topology: Diamond Multipath");
console.log("               ┌───> Node-B (7022) ───┐");
console.log(" Node-A (7021) │                      │───> Node-D (7024)");
console.log("               └───> Node-C (7023) ───┘");
console.log("===================================================================\n");

// Ensure clean test environment and ports
execSync("fuser -k 7021/tcp 7022/tcp 7023/tcp 7024/tcp 2>/dev/null || true");

const dbFiles = [
    "./data/store-Node-A.db",
    "./data/store-Node-B.db",
    "./data/store-Node-C.db",
    "./data/store-Node-D.db",
    "./data/store-Node-A.db-journal",
    "./data/store-Node-B.db-journal",
    "./data/store-Node-C.db-journal",
    "./data/store-Node-D.db-journal"
];
for (const f of dbFiles) {
    if (fs.existsSync(f)) fs.rmSync(f, { force: true });
}

interface NodeLogCapture {
    port: string;
    proc: ChildProcess;
    stdout: string[];
    stderr: string[];
}

function launchNode(port: string, name: string, peerUrl?: string): NodeLogCapture {
    const args = ["tsx", "src/node.ts", port, name];
    if (peerUrl) args.push(peerUrl);

    const proc = spawn("npx", args, {
        env: {
            ...process.env,
            ENABLE_MDNS: "false",
            HOST: "127.0.0.1"
        },
        stdio: ["pipe", "pipe", "pipe"],
        detached: true
    });

    const capture: NodeLogCapture = { port, proc, stdout: [], stderr: [] };

    proc.stdout?.on("data", (d) => {
        const line = d.toString().trim();
        capture.stdout.push(line);
        console.log(`[${name}] ${line}`);
    });

    proc.stderr?.on("data", (d) => {
        const line = d.toString().trim();
        capture.stderr.push(line);
        console.error(`[${name} ERR] ${line}`);
    });

    return capture;
}

function killNodeProcess(node: NodeLogCapture): void {
    try {
        if (node.proc.pid) process.kill(-node.proc.pid, "SIGKILL");
    } catch (e) {}
    try {
        execSync(`fuser -k ${node.port}/tcp 2>/dev/null || true`);
    } catch (e) {}
}

async function runDedupeTest() {
    let nodeA: NodeLogCapture | null = null;
    let nodeB: NodeLogCapture | null = null;
    let nodeC: NodeLogCapture | null = null;
    let nodeD: NodeLogCapture | null = null;

    try {
        // Step 1: Launch destination Node-D on port 7024
        console.log("\n>>> [1/4] Launching Destination Node-D (Port 7024)...");
        nodeD = launchNode("7024", "Node-D");
        await delay(1200);

        // Step 2: Launch intermediate relays Node-B and Node-C, both connected to Node-D
        console.log("\n>>> [2/4] Launching Parallel Relays Node-B (7022) & Node-C (7023) -> Node-D...");
        nodeB = launchNode("7022", "Node-B", "ws://127.0.0.1:7024");
        nodeC = launchNode("7023", "Node-C", "ws://127.0.0.1:7024");
        await delay(2000);

        // Step 3: Launch origin Node-A connected to BOTH Node-B and Node-C
        console.log("\n>>> [3/4] Launching Origin Node-A (7021) connected to BOTH Node-B and Node-C...");
        nodeA = launchNode("7021", "Node-A", "ws://127.0.0.1:7022");
        await delay(2000);
        nodeA.proc.stdin?.write("connect ws://127.0.0.1:7023\n");
        await delay(2500);

        console.log("\n✅ Diamond topology established. Dual concurrent paths active:");
        console.log("   Path 1: Node-A -> Node-B -> Node-D");
        console.log("   Path 2: Node-A -> Node-C -> Node-D");

        // Step 4: Broadcast an Emergency Disaster Bulletin from Node-A
        console.log("\n=======================================================");
        console.log(">>> [4/4] Node-A broadcasts Hazard Bulletin across Diamond Mesh");
        console.log("Command: warn FLOOD_SURGE Flash_Flood_Evacuate_Zone_7");
        console.log("=======================================================");
        nodeA.proc.stdin?.write("warn FLOOD_SURGE Flash_Flood_Evacuate_Zone_7\n");
        await delay(3500);

        // Telemetry analysis
        const bRelayed = nodeB.stdout.some((l) => l.includes("Flash_Flood_Evacuate_Zone_7"));
        const cRelayed = nodeC.stdout.some((l) => l.includes("Flash_Flood_Evacuate_Zone_7"));
        const dReceived = nodeD.stdout.some((l) => l.includes("Flash_Flood_Evacuate_Zone_7"));
        const dSuppressedDuplicate = nodeD.stdout.some((l) => l.includes("[DEDUPE] Suppressed duplicate packet"));

        // Query Node-D's SQLite database to confirm exactly ONE record exists (no duplicate DB insertion)
        const dbPath = "./data/store-Node-D.db";
        let uniqueInDb = false;
        let bulletinCount = 0;
        if (fs.existsSync(dbPath)) {
            const db = new DatabaseSync(dbPath);
            const rows = db.prepare("SELECT * FROM disaster_bulletins WHERE announcement LIKE '%Flash_Flood_Evacuate_Zone_7%'").all() as any[];
            bulletinCount = rows.length;
            uniqueInDb = (bulletinCount === 1);
            db.close();
        }

        console.log("\n=======================================================");
        console.log("             TEST 3 VERIFICATION RESULTS              ");
        console.log("=======================================================");
        console.log(`* Path 1 forward (via Node-B):                  ${bRelayed ? "PASSED" : "FAILED"}`);
        console.log(`* Path 2 forward (via Node-C):                  ${cRelayed ? "PASSED" : "FAILED"}`);
        console.log(`* Node-D processed message:                     ${dReceived ? "PASSED" : "FAILED"}`);
        console.log(`* Node-D suppressed 2nd duplicate packet:       ${dSuppressedDuplicate ? "PASSED (Drop logged)" : "FAILED"}`);
        console.log(`* SQLite idempotency check (exactly 1 record):  ${uniqueInDb ? `PASSED (${bulletinCount} record)` : `FAILED (${bulletinCount} records)`}`);
        console.log("=======================================================");

        if (dReceived && dSuppressedDuplicate && uniqueInDb) {
            console.log("\n🎉 TEST 3 PASSED: Diamond multipath deduplication verified! Duplicate packet dropped with 0 side-effects.\n");
            process.exit(0);
        } else {
            console.error("\n❌ TEST 3 FAILED: Duplicate packet was not properly suppressed.\n");
            process.exit(1);
        }
    } catch (err: any) {
        console.error("Test execution error:", err.message);
        process.exit(1);
    } finally {
        if (nodeA) killNodeProcess(nodeA);
        if (nodeB) killNodeProcess(nodeB);
        if (nodeC) killNodeProcess(nodeC);
        if (nodeD) killNodeProcess(nodeD);
    }
}

runDedupeTest();
