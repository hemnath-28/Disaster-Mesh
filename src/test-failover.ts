import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Phase 10 - Test 1: Mid-Route Node Crash & Dynamic Route Failover");
console.log(" Topology: Diamond Topology");
console.log("               ┌───> Node-B (7002) ───┐");
console.log(" Node-A (7001) │                      │───> Node-D (7004)");
console.log("               └───> Node-C (7003) ───┘");
console.log("===================================================================\n");

// Ensure clean database directory for this test
// Ensure clean test environment and ports
execSync("fuser -k 7001/tcp 7002/tcp 7003/tcp 7004/tcp 2>/dev/null || true");
const TEST_DIR = "./data/test-failover";
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
        if (node.proc.pid) {
            process.kill(-node.proc.pid, "SIGKILL");
        }
    } catch (e) {}
    try {
        execSync(`fuser -k ${node.port}/tcp 2>/dev/null || true`);
    } catch (e) {}
}

async function runFailoverTest() {
    let nodeA: NodeLogCapture | null = null;
    let nodeB: NodeLogCapture | null = null;
    let nodeC: NodeLogCapture | null = null;
    let nodeD: NodeLogCapture | null = null;

    try {
        // Step 1: Launch destination Node-D
        console.log("\n>>> [1/5] Launching Node-D (Port 7004)...");
        nodeD = launchNode("7004", "Node-D");
        await delay(1200);

        // Step 2: Launch intermediate relays Node-B and Node-C, both connecting to Node-D
        console.log(">>> [2/5] Launching Relays Node-B (Port 7002) & Node-C (Port 7003) -> Node-D...");
        nodeB = launchNode("7002", "Node-B", "ws://127.0.0.1:7004");
        nodeC = launchNode("7003", "Node-C", "ws://127.0.0.1:7004");
        await delay(1500);

        // Step 3: Launch origin Node-A connecting to Node-B and Node-C
        console.log(">>> [3/5] Launching Node-A (Port 7001) connected to Node-B & Node-C...");
        nodeA = launchNode("7001", "Node-A", "ws://127.0.0.1:7002");
        await delay(1500);
        // Connect Node-A to Node-C via command line peer connection
        nodeA.proc.stdin?.write("connect ws://127.0.0.1:7003\n");
        await delay(2000);

        // Step 4: Send first message from Node-A to Node-D
        console.log("\n=======================================================");
        console.log(">>> [4/5] Sending Message 1: Node-A -> Node-D via primary route");
        console.log("=======================================================");
        nodeA.proc.stdin?.write("msg Node-D Hello_Before_Crash\n");
        await delay(2500);

        const dReceivedFirst = nodeD.stdout.some((line) => line.includes("Hello_Before_Crash"));
        if (!dReceivedFirst) {
            throw new Error("FAIL: Node-D did not receive the initial message before the crash!");
        }
        console.log("✅ Message 1 successfully reached Node-D.");

        // Check which intermediate node relayed Message 1
        const bRelayed = nodeB.stdout.some((line) => line.includes("Relayed message"));
        const cRelayed = nodeC.stdout.some((line) => line.includes("Relayed message"));
        console.log(`Initial relay telemetry: Node-B relayed=${bRelayed}, Node-C relayed=${cRelayed}`);

        // Kill the relay that handled the traffic (or Node-B by default)
        const victimRelay = bRelayed ? nodeB : (cRelayed ? nodeC : nodeB);
        const victimName = victimRelay === nodeB ? "Node-B" : "Node-C";
        const survivingRelay = victimRelay === nodeB ? nodeC : nodeB;
        const survivingName = survivingRelay === nodeB ? "Node-B" : "Node-C";

        console.log(`\n=======================================================`);
        console.log(`>>> [5/5] INJECTING CHAOS: Simulating instant death of ${victimName}...`);
        console.log(`=======================================================`);
        killNodeProcess(victimRelay);
        await delay(2000);

        console.log(`\nSending Message 2 from Node-A to Node-D after ${victimName} is DEAD...`);
        nodeA.proc.stdin?.write("msg Node-D Hello_After_Crash_Failover_Active\n");
        await delay(3500);

        const dReceivedSecond = nodeD.stdout.some((line) => line.includes("Hello_After_Crash_Failover_Active"));
        const survivingRelayedSecond = survivingRelay.stdout.some((line) => line.includes("Hello_After_Crash_Failover_Active"));

        console.log("\n=======================================================");
        console.log("             TEST 1 VERIFICATION RESULTS              ");
        console.log("=======================================================");
        console.log(`* Pre-crash delivery (Node-A -> Node-D):       ${dReceivedFirst ? "PASSED" : "FAILED"}`);
        console.log(`* Chaos injection:                             ${victimName} killed (SIGKILL)`);
        console.log(`* Alternate relay (${survivingName}) carried packet:  ${survivingRelayedSecond ? "PASSED" : "FAILED"}`);
        console.log(`* Post-crash delivery (Node-A -> Node-D):      ${dReceivedSecond ? "PASSED" : "FAILED"}`);
        console.log("=======================================================");

        if (dReceivedSecond) {
            console.log("\n🎉 TEST 1 PASSED: Dynamic route failover successfully routed around dead node!\n");
            process.exit(0);
        } else {
            console.error("\n❌ TEST 1 FAILED: Message 2 was lost after intermediate node crashed.\n");
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

runFailoverTest();
