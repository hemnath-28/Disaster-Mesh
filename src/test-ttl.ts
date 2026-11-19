import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Phase 10 - Test 4: TTL Expiration & Routing Loop Suppression");
console.log(" Topology: Multi-Hop Line (3 Hops Distance)");
console.log(" Node-A (7031) <──> Node-B (7032) <──> Node-C (7033) <──> Node-D (7034)");
console.log("===================================================================\n");

// Ensure clean test environment and ports
execSync("fuser -k 7031/tcp 7032/tcp 7033/tcp 7034/tcp 2>/dev/null || true");

const TEST_DIR = "./data/test-ttl";
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

async function runTtlTest() {
    let nodeA: NodeLogCapture | null = null;
    let nodeB: NodeLogCapture | null = null;
    let nodeC: NodeLogCapture | null = null;
    let nodeD: NodeLogCapture | null = null;

    try {
        // Step 1: Launch line topology from destination backwards
        console.log("\n>>> [1/4] Launching 4-Node Line Topology...");
        nodeD = launchNode("7034", "Node-D");
        await delay(1200);

        nodeC = launchNode("7033", "Node-C", "ws://127.0.0.1:7034");
        await delay(1200);

        nodeB = launchNode("7032", "Node-B", "ws://127.0.0.1:7033");
        await delay(1200);

        nodeA = launchNode("7031", "Node-A", "ws://127.0.0.1:7032");
        await delay(2000);

        console.log("\n✅ Line topology active: Node-A -> Node-B -> Node-C -> Node-D (3 hops away)");

        // Step 2: Establish route from A to D with normal discovery
        console.log("\n=======================================================");
        console.log(">>> [2/4] Sending baseline message to map end-to-end path");
        console.log("=======================================================");
        nodeA.proc.stdin?.write("msg Node-D Route_Discovery_Probe 5\n");
        await delay(3000);

        const dReceivedBaseline = nodeD.stdout.some((l) => l.includes("Route_Discovery_Probe"));
        if (!dReceivedBaseline) {
            throw new Error("FAIL: Baseline 3-hop delivery failed. Route could not be established.");
        }
        console.log("✅ Baseline 3-hop route verified: Node-A reached Node-D.");

        // Step 3: Send message with insufficient TTL (TTL = 2 over 3 hops)
        console.log("\n=======================================================");
        console.log(">>> [3/4] Transmitting packet with TTL = 2 (Requires 3 hops to reach Node-D)");
        console.log("Expected: Node-A -> Node-B -> Node-C (TTL hits 0 and DROPS at Node-C)");
        console.log("=======================================================");
        nodeA.proc.stdin?.write("msg Node-D Dead_Packet_TTL_Expired 2\n");
        await delay(3000);

        // Analyze hop outputs
        const bRelayed = nodeB.stdout.some((l) => l.includes("Dead_Packet_TTL_Expired"));
        const cDroppedTtl = nodeC.stdout.some((l) => l.includes("[TTL] Dropped expired packet"));
        const dReceivedTtlExpired = nodeD.stdout.some((l) => l.includes("Dead_Packet_TTL_Expired"));

        console.log("\n=======================================================");
        console.log("             TEST 4 VERIFICATION RESULTS              ");
        console.log("=======================================================");
        console.log(`* Hop 1 (Node-A -> Node-B, TTL decremented):    ${bRelayed ? "PASSED" : "FAILED"}`);
        console.log(`* Hop 2 (Node-C detected TTL = 0 and DROPPED):  ${cDroppedTtl ? "PASSED (Drop logged)" : "FAILED"}`);
        console.log(`* Hop 3 (Node-D never received dead packet):    ${!dReceivedTtlExpired ? "PASSED (Protected)" : "FAILED"}`);
        console.log("=======================================================");

        if (bRelayed && cDroppedTtl && !dReceivedTtlExpired) {
            console.log("\n🎉 TEST 4 PASSED: TTL expiration successfully dropped expired packet and prevented forwarding waste!\n");
            process.exit(0);
        } else {
            console.error("\n❌ TEST 4 FAILED: Packet was not cleanly dropped on TTL expiration.\n");
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

runTtlTest();
