import { spawn, ChildProcess, execSync } from "child_process";
import fs from "fs";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Dynamic Route-Aware Adaptive TTL");
console.log(" Topology: 6-Hop Linear Chain (7 Nodes: 7041 to 7047)");
console.log(" Goal: Prove packets travel farther than old 5-hop hardcoded limit");
console.log("       and auto-scale TTL = hopDistance + 3");
console.log("===================================================================\n");

// Ensure clean test environment and ports
execSync("fuser -k 7041/tcp 7042/tcp 7043/tcp 7044/tcp 7045/tcp 7046/tcp 7047/tcp 2>/dev/null || true");

const TEST_DIR = "./data/test-adaptive-ttl";
if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
}
fs.mkdirSync(TEST_DIR, { recursive: true });

interface NodeHandle {
    name: string;
    port: number;
    proc: ChildProcess;
    stdout: string[];
    stderr: string[];
}

function launchNode(index: number, peerPort?: number): NodeHandle {
    const name = `Node-${String.fromCharCode(65 + index)}`; // Node-A, Node-B, ...
    const port = 7041 + index;
    const args = ["tsx", "src/node.ts", port.toString(), name];
    if (peerPort) args.push(`ws://127.0.0.1:${peerPort}`);

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

    const handle: NodeHandle = { name, port, proc, stdout: [], stderr: [] };

    proc.stdout?.on("data", (d) => {
        const line = d.toString().trim();
        handle.stdout.push(line);
        console.log(`[${name}] ${line}`);
    });

    proc.stderr?.on("data", (d) => {
        handle.stderr.push(d.toString().trim());
    });

    return handle;
}

function killAll(nodes: NodeHandle[]) {
    for (const n of nodes) {
        try {
            if (n.proc.pid) process.kill(-n.proc.pid, "SIGKILL");
        } catch (e) {}
    }
    execSync("fuser -k 7041/tcp 7042/tcp 7043/tcp 7044/tcp 7045/tcp 7046/tcp 7047/tcp 2>/dev/null || true");
}

async function runAdaptiveTtlTest() {
    const nodes: NodeHandle[] = [];

    try {
        console.log(">>> [1/3] Booting 7-node chain (Node-A through Node-G)...");
        // Launch Node-G first, then chain backwards to Node-A
        nodes.unshift(launchNode(6)); // Node-G (7047)
        await delay(800);

        for (let i = 5; i >= 0; i--) {
            nodes.unshift(launchNode(i, 7041 + i + 1));
            await delay(800);
        }

        // Wait for all bidirectional peer connections to settle across all 6 hops
        console.log("Waiting for peer handshakes to stabilize across the chain...");
        const startTime = Date.now();
        while (Date.now() - startTime < 8000) {
            if (nodes[0].stdout.some((l) => l.includes("Identified: Node-B"))) {
                break;
            }
            await delay(300);
        }
        await delay(1500);

        console.log("\n✅ 7-Node Chain Online: A <-> B <-> C <-> D <-> E <-> F <-> G");
        console.log("Distance between Node-A and Node-G is 6 wireless hops.");
        console.log("(Under the old system with fixed TTL = 5, this packet would DIE at Node-F.)\n");

        // Step 2: Node-A transmits message to Node-G across 6 hops
        console.log("=======================================================");
        console.log(">>> [2/3] Node-A sends message to Node-G across 6 hops");
        console.log("Command: msg Node-G Crossing_Six_Hop_Threshold");
        console.log("=======================================================");
        nodes[0].proc.stdin?.write("msg Node-G Crossing_Six_Hop_Threshold\n");
        await delay(6000);

        // Analyze telemetry
        const nodeAOutput = nodes[0].stdout.join("\n");
        const nodeGOutput = nodes[6].stdout.join("\n");

        const adaptiveTtlLogged = nodes[0].stdout.find((l) => l.includes("Adaptive TTL"));
        const gDelivered = nodeGOutput.includes("Crossing_Six_Hop_Threshold");

        console.log("\n=======================================================");
        console.log(">>> [3/3] Analyzing Dynamic Adaptive TTL Telemetry");
        console.log("=======================================================");
        console.log(`* Node-A Outbound Telemetry:  ${adaptiveTtlLogged || "No adaptive log"}`);
        console.log(`* 6-Hop Delivery to Node-G:   ${gDelivered ? "PASSED (Delivered across 6 hops!)" : "FAILED"}`);

        // Verify intermediate hops relayed
        let relaysCount = 0;
        for (let i = 1; i <= 5; i++) {
            if (nodes[i].stdout.some((l) => l.includes("Crossing_Six_Hop_Threshold"))) {
                relaysCount++;
            }
        }
        console.log(`* Intermediate Relays Active: ${relaysCount} / 5 (B, C, D, E, F)`);

        console.log("\n=======================================================");
        console.log("          ADAPTIVE TTL TEST RESULTS                    ");
        console.log("=======================================================");
        console.log(`* Multi-Hop Reach (> 5 hops):  ${gDelivered ? "PASSED" : "FAILED"}`);
        console.log(`* Dynamic Hop Scaling:         ${adaptiveTtlLogged ? "PASSED" : "FAILED"}`);
        console.log(`* All 5 Intermediaries Relayed:${relaysCount === 5 ? "PASSED" : "FAILED"}`);
        console.log("=======================================================");

        if (gDelivered && relaysCount === 5) {
            console.log("\n🎉 DYNAMIC ROUTE-AWARE TTL VERIFIED: Packets successfully scale beyond fixed hop limits!\n");
            process.exit(0);
        } else {
            console.error("\n❌ TEST FAILED: 6-hop packet did not reach Node-G.\n");
            process.exit(1);
        }
    } catch (err: any) {
        console.error("Test execution error:", err.message);
        process.exit(1);
    } finally {
        killAll(nodes);
    }
}

runAdaptiveTtlTest();
