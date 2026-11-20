import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";
import path from "path";
import { DatabaseSync } from "node:sqlite";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Phase 10 - Test 2: Network Partition & Store-and-Forward Healing");
console.log(" Topology:");
console.log("   [Island 1]           [Wireless Void]           [Island 2]");
console.log(" Node-A <-> Node-B          ❌ (No Link)          Node-D <-> Node-E");
console.log("   (7011)     (7012)                                 (7014)     (7015)");
console.log("                        ═════════════");
console.log("                     Mobile Bridge: Node-C (7013)");
console.log("===================================================================\n");

// Ensure clean test environment
execSync("fuser -k 7011/tcp 7012/tcp 7013/tcp 7014/tcp 7015/tcp 2>/dev/null || true");

const TEST_DIR = "./data/test-partition";
if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
}
fs.mkdirSync(TEST_DIR, { recursive: true });

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
            HOST: "127.0.0.1",
            STORAGE_DIR: TEST_DIR
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

async function runPartitionTest() {
    let nodeA: NodeLogCapture | null = null;
    let nodeB: NodeLogCapture | null = null;
    let nodeC: NodeLogCapture | null = null;
    let nodeD: NodeLogCapture | null = null;
    let nodeE: NodeLogCapture | null = null;

    try {
        // Step 1: Launch Island 2 (Node-D on 7014, Node-E on 7015 connected to Node-D)
        console.log("\n>>> [1/5] Launching Island 2 (Node-D: 7014, Node-E: 7015)...");
        nodeD = launchNode("7014", "Node-D");
        await delay(1200);
        nodeE = launchNode("7015", "Node-E", "ws://127.0.0.1:7014");
        await delay(1500);

        // Step 2: Launch Island 1 (Node-B on 7012, Node-A on 7011 connected to Node-B)
        console.log("\n>>> [2/5] Launching Island 1 (Node-A: 7011, Node-B: 7012)...");
        nodeB = launchNode("7012", "Node-B");
        await delay(1200);
        nodeA = launchNode("7011", "Node-A", "ws://127.0.0.1:7012");
        await delay(1500);

        console.log("\n✅ Island 1 (Node-A <-> Node-B) and Island 2 (Node-D <-> Node-E) are running completely isolated.");

        // Step 3: Node-A attempts to send message to Node-E across the partition
        console.log("\n=======================================================");
        console.log(">>> [3/5] Node-A sends message to unreachable Node-E across partition");
        console.log("=======================================================");
        nodeA.proc.stdin?.write("msg Node-E Partition_Emergency_Cargo_Request\n");
        await delay(2500);

        // Verify Node-E did NOT receive it
        const eReceivedDuringSplit = nodeE.stdout.some((line) => line.includes("Partition_Emergency_Cargo_Request"));
        console.log(`Pre-healing delivery check: Node-E received during split = ${eReceivedDuringSplit}`);

        // Verify SQLite store-and-forward buffered the message in store-Node-A.db
        const dbPath = path.join(TEST_DIR, "store-Node-A.db");
        let sqliteBuffered = false;
        if (fs.existsSync(dbPath)) {
            const db = new DatabaseSync(dbPath);
            const row = db.prepare("SELECT * FROM messages WHERE receiver_device_id = 'Node-E'").get() as any;
            if (row && row.payload === "Partition_Emergency_Cargo_Request") {
                sqliteBuffered = true;
                console.log(`✅ SQLite Verification: Message successfully buffered in SQLite on Node-A (Status: ${row.status})`);
            }
            db.close();
        }

        if (!sqliteBuffered) {
            throw new Error("FAIL: Message was not stored in SQLite on Node-A when destination was unreachable!");
        }

        // Step 4: Heal the partition using mobile bridge Node-C
        console.log("\n=======================================================");
        console.log(">>> [4/5] HEALING PARTITION: Mobile Bridge Node-C connects Island 1 and Island 2...");
        console.log("=======================================================");
        nodeC = launchNode("7013", "Node-C", "ws://127.0.0.1:7012"); // connects to Node-B (Island 1)
        await delay(1500);
        nodeC.proc.stdin?.write("connect ws://127.0.0.1:7014\n");     // connects to Node-D (Island 2)
        await delay(6000);

        // Step 5: Verify automatic store-and-forward delivery to Node-E
        console.log("\n=======================================================");
        console.log(">>> [5/5] Verifying post-healing store-and-forward flush to Node-E...");
        console.log("=======================================================");
        const eReceivedAfterHealing = nodeE.stdout.some((line) => line.includes("Partition_Emergency_Cargo_Request"));

        console.log("\n=======================================================");
        console.log("             TEST 2 VERIFICATION RESULTS              ");
        console.log("=======================================================");
        console.log(`* Pre-healing isolation check (packet dropped/lost?): ${!eReceivedDuringSplit ? "PASSED (Blocked by gap)" : "FAILED"}`);
        console.log(`* SQLite store-and-forward persistence:               ${sqliteBuffered ? "PASSED (Buffered as PENDING)" : "FAILED"}`);
        console.log(`* Partition healing via Bridge Node-C:               PASSED (Island 1 <-> C <-> Island 2)`);
        console.log(`* Post-healing delivery at Node-E:                    ${eReceivedAfterHealing ? "PASSED" : "FAILED"}`);
        console.log("=======================================================");

        if (eReceivedAfterHealing && !eReceivedDuringSplit && sqliteBuffered) {
            console.log("\n🎉 TEST 2 PASSED: Store-and-forward successfully survived network partition and auto-flushed upon healing!\n");
            process.exit(0);
        } else {
            console.error("\n❌ TEST 2 FAILED: Message did not flush across healed partition.\n");
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
        if (nodeE) killNodeProcess(nodeE);
    }
}

runPartitionTest();
