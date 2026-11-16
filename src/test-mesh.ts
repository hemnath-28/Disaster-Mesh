import { spawn, ChildProcess } from "child_process";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 4 (Step 1): Intelligent Routing & Directed Forwarding");
console.log(" Topology: Node-A (5001) <---> Node-B (5002) <---> Node-C (5003)");
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
    // 1. Launch Node A
    const nodeA = launchNode("5001", "Node-A");
    await delay(1200);

    // 2. Launch Node B, connecting to Node A
    const nodeB = launchNode("5002", "Node-B", "ws://127.0.0.1:5001");
    await delay(1200);

    // 3. Launch Node C, connecting to Node B
    const nodeC = launchNode("5003", "Node-C", "ws://127.0.0.1:5002");
    // Give enough time for all three nodes to finish their WebSocket IDENTIFY handshakes
    await delay(3000);

    console.log("\n=======================================================");
    console.log("Test: Node-A sends to Node-C (Unknown 2-hop route)");
    console.log("Expected:");
    console.log("1. Node-A triggers RREQ for Node-C");
    console.log("2. Node-C replies with RREP");
    console.log("3. Node-A establishes nextHop = Node-B");
    console.log("4. Message & ACK are routed via DIRECTED FORWARDING!");
    console.log("=======================================================\n");

    nodeA.stdin?.write("send Node-C Directed_Intelligent_Route_Message\n");
    await delay(3000);

    console.log("\n=======================================================");
    console.log("Inspecting Node-A Routing Table & Stats:");
    console.log("=======================================================");
    nodeA.stdin?.write("routes\n");
    await delay(500);
    nodeA.stdin?.write("stats\n");
    await delay(1000);

    console.log("\n=======================================================");
    console.log("Shutting down test mesh...");
    console.log("=======================================================");
    nodeA.kill();
    nodeB.kill();
    nodeC.kill();
    console.log("Phase 4 (Step 1) Test Finished Successfully!");
    process.exit(0);
}

runTest();
