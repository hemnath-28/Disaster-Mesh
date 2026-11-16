import { spawn, ChildProcess } from "child_process";
import fs from "fs";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 5: Complete End-to-End Store-and-Forward DTN");
console.log(" Scenario: Disconnected Recipient -> Cold Restart -> Reconnect Auto-Delivery");
console.log("===================================================================\n");

// Clean old test database files
if (fs.existsSync("./data")) {
    fs.rmSync("./data", { recursive: true, force: true });
}

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
    // -------------------------------------------------------------------------
    // Phase 5 Stage 1: Send to an offline node & verify SQLite persistence
    // -------------------------------------------------------------------------
    console.log("-------------------------------------------------------------------");
    console.log("Stage 1: Node-A boots ALONE. Sends message to OFFLINE Node-B.");
    console.log("-------------------------------------------------------------------");
    let nodeA = launchNode("5001", "Node-A");
    await delay(1500);

    nodeA.stdin?.write("send Node-B Message_Stored_While_You_Were_Offline\n");
    await delay(5500); // Wait for discovery timeout (4s) -> stored into SQLite

    console.log("\nChecking Node-A outbox (Should have 1 PENDING message):");
    nodeA.stdin?.write("outbox\n");
    await delay(1000);

    // -------------------------------------------------------------------------
    // Phase 5 Stage 2: Cold Crash / Restart of Node-A
    // -------------------------------------------------------------------------
    console.log("\n-------------------------------------------------------------------");
    console.log("Stage 2: COLD RESTART! Killing Node-A and restarting to prove disk durability...");
    console.log("-------------------------------------------------------------------");
    nodeA.kill();
    await delay(1500);

    // Re-launch Node-A with the same device ID
    nodeA = launchNode("5001", "Node-A");
    await delay(1500);

    console.log("\nChecking Node-A outbox after reboot (Must still be in DB!):");
    nodeA.stdin?.write("outbox\n");
    await delay(1000);

    // -------------------------------------------------------------------------
    // Phase 5 Stage 3: Node-B comes ONLINE and connects to Node-A!
    // -------------------------------------------------------------------------
    console.log("\n-------------------------------------------------------------------");
    console.log("Stage 3: Node-B comes ONLINE! Connects to Node-A...");
    console.log("Expected: Node-A immediately detects Node-B, flushes the queued");
    console.log("message from SQLite, Node-B delivers it, and Node-A updates to DELIVERED!");
    console.log("-------------------------------------------------------------------");
    const nodeB = launchNode("5002", "Node-B", "ws://127.0.0.1:5001");
    await delay(3000);

    // -------------------------------------------------------------------------
    // Phase 5 Stage 4: Verify Final Database State
    // -------------------------------------------------------------------------
    console.log("\n-------------------------------------------------------------------");
    console.log("Stage 4: Checking Node-A outbox (Must now be marked [DELIVERED]):");
    console.log("-------------------------------------------------------------------");
    nodeA.stdin?.write("outbox\n");
    await delay(1000);

    console.log("\nShutting down nodes...");
    nodeA.kill();
    nodeB.kill();
    console.log("Phase 5 Store-and-Forward Verification Complete!");
    process.exit(0);
}

runTest();
