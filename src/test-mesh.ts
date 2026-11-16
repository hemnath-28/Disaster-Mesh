import { spawn, ChildProcess } from "child_process";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("=== Testing Phase 3: Multi-Hop Mesh Network ===");
console.log("Topology setup: Node-A (5001) <---> Node-B (5002) <---> Node-C (5003)");

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
    // 1. Launch Node A first and wait for it to listen
    const nodeA = launchNode("5001", "Node-A");
    await delay(1500);

    // 2. Launch Node B, connecting to Node A
    const nodeB = launchNode("5002", "Node-B", "ws://127.0.0.1:5001");
    await delay(1500);

    // 3. Launch Node C, connecting to Node B
    const nodeC = launchNode("5003", "Node-C", "ws://127.0.0.1:5002");
    await delay(1500);

    console.log("\n=======================================================");
    console.log("Test 1: Unicast from Node-A to Node-C (A -> B -> C)");
    console.log("=======================================================");
    nodeA.stdin?.write("send Node-C Hello_Node_C_via_Node_B!\n");
    await delay(2000);

    console.log("\n=======================================================");
    console.log("Test 2: Broadcast from Node-C to all (C -> B -> A)");
    console.log("=======================================================");
    nodeC.stdin?.write("send * Disaster_Warning_Level_1\n");
    await delay(2000);

    console.log("\n=======================================================");
    console.log("Shutting down test mesh...");
    console.log("=======================================================");
    nodeA.kill();
    nodeB.kill();
    nodeC.kill();
    console.log("Finished cleanly!");
    process.exit(0);
}

runTest();
