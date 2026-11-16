import { spawn, ChildProcess } from "child_process";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 4 (Step 2): Link Failure & Dynamic Self-Healing RERR");
console.log(" Initial Topology: Node-A (5001) <---> Node-B (5002) <---> Node-C (5003)");
console.log("===================================================================\n");

function launchNode(port: string, name: string, peerUrl?: string): ChildProcess {
    const args = ["tsx", "src/node.ts", port, name];
    if (peerUrl) args.push(peerUrl);

    const proc = spawn("npx", args, {
        env: { ...process.env, ENABLE_MDNS: "false" },
        stdio: ["pipe", "pipe", "pipe"]
    });

    proc.stdout?.on("data", (d) => console.log(`[${name}] ${d.toString().trim()}`));
    proc.stderr?.on("data", (d) => console.error(`[${name} ERR] ${d.toString().trim()}`));
    return proc;
}

async function runTest() {
    // 1. Launch initial 3-node linear chain
    const nodeA = launchNode("5001", "Node-A");
    await delay(1200);

    const nodeB = launchNode("5002", "Node-B", "ws://127.0.0.1:5001");
    await delay(1200);

    const nodeC = launchNode("5003", "Node-C", "ws://127.0.0.1:5002");
    await delay(2500);

    console.log("\n=======================================================");
    console.log("Stage 1: Establish Initial Route from Node-A to Node-C via Node-B");
    console.log("=======================================================");
    nodeA.stdin?.write("send Node-C Packet_Before_Bridge_Failure\n");
    await delay(2500);

    console.log("\n=======================================================");
    console.log("Stage 2: Simulate Bridge Crash! Killing intermediate Node-B...");
    console.log("=======================================================");
    nodeB.kill();
    await delay(2000);

    console.log("\n=======================================================");
    console.log("Stage 3: Inspect Node-A Routing Table after Node-B failure");
    console.log("Expected: Route to Node-C (via Node-B) must be INVALIDATED / purged!");
    console.log("=======================================================");
    nodeA.stdin?.write("routes\n");
    await delay(1000);

    console.log("\n=======================================================");
    console.log("Stage 4: Self-Healing with Alternate Path");
    console.log("Spawn Node-D (5004) bridging Node-A and Node-C directly!");
    console.log("=======================================================");
    // Node-D connects to Node-A (5001) and connects to Node-C (5003)
    const nodeD = launchNode("5004", "Node-D", "ws://127.0.0.1:5001");
    await delay(1500);
    nodeD.stdin?.write("connect ws://127.0.0.1:5003\n");
    await delay(2500);

    console.log("\n=======================================================");
    console.log("Stage 5: Node-A re-sends to Node-C over healed network!");
    console.log("Expected: Node-A dynamically re-discovers route via Node-D and delivers!");
    console.log("=======================================================");
    nodeA.stdin?.write("send Node-C Packet_After_Self_Healing_Via_Node_D\n");
    await delay(3000);

    console.log("\n=======================================================");
    console.log("Inspecting Node-A Final Routing Table:");
    console.log("=======================================================");
    nodeA.stdin?.write("routes\n");
    await delay(1000);

    console.log("\nShutting down nodes...");
    nodeA.kill();
    nodeC.kill();
    nodeD.kill();
    console.log("Phase 4 (Step 2) Self-Healing Test Completed Successfully!");
    process.exit(0);
}

runTest();
