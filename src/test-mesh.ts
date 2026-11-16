import { spawn, ChildProcess } from "child_process";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 3: Round-Trip Delivery Acknowledgments (ACKs)");
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
    await delay(1200);

    console.log("\n=======================================================");
    console.log("Test: Unicast with End-to-End Delivery ACK");
    console.log("Node-A sends to Node-C -> Node-C delivers -> Node-C ACKs back to Node-A");
    console.log("=======================================================");
    nodeA.stdin?.write("send Node-C Emergency_Medical_Supplies_Needed\n");
    await delay(2500);

    console.log("\n=======================================================");
    console.log("Checking Node-A statistics (Expect 1 originated, 1 ACK received):");
    console.log("=======================================================");
    nodeA.stdin?.write("stats\n");
    await delay(1000);

    console.log("\n=======================================================");
    console.log("Shutting down test mesh...");
    console.log("=======================================================");
    nodeA.kill();
    nodeB.kill();
    nodeC.kill();
    console.log("Phase 3 Full Verification Test Completed!");
    process.exit(0);
}

runTest();
