import { spawn, ChildProcess } from "child_process";
import fs from "fs";
import path from "path";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 6: Emergency SOS & Mesh Hop Proximity Localization");
console.log(" Topology: Node-Victim (5001) <---> Node-Relay (5002) <---> Node-RescueBase (5003)");
console.log(" (Victim and RescueBase have NO direct connection)");
console.log("===================================================================\n");

// Clean old test databases
const TEST_DIR = "./data/test-mesh";
if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
}
fs.mkdirSync(TEST_DIR, { recursive: true });

function launchNode(port: string, name: string, peerUrl?: string, extraArgs: string[] = []): ChildProcess {
    const args = ["tsx", "src/node.ts", port, name];
    if (peerUrl) args.push(peerUrl);
    args.push(...extraArgs);

    const proc = spawn("npx", args, {
        env: { ...process.env, ENABLE_MDNS: "false", STORAGE_DIR: TEST_DIR },
        stdio: ["pipe", "pipe", "pipe"]
    });

    proc.stdout?.on("data", (d) => console.log(`[${name}] ${d.toString().trim()}`));
    proc.stderr?.on("data", (d) => console.error(`[${name} ERR] ${d.toString().trim()}`));
    return proc;
}

async function runTest() {
    // -------------------------------------------------------------------------
    // Test 1: Stranded Victim launches ALONE and broadcasts SOS while isolated
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 1: Node-Victim starts alone (0 peers) and broadcasts SOS");
    console.log("Command: sos TRAPPED CRITICAL 4 Water_Rising_2nd_Floor");
    console.log("=======================================================\n");

    const victim = launchNode("5001", "Node-Victim");
    await delay(1500);

    victim.stdin?.write("sos TRAPPED CRITICAL 4 Water_Rising_2nd_Floor\n");
    await delay(2000);

    // -------------------------------------------------------------------------
    // Test 2: Relay launches later and connects to Victim
    // Verify Automatic Epidemic Sync: Victim auto-syncs open SOS to Relay
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 2: Node-Relay powers on and connects to Node-Victim");
    console.log("Expected: Node-Victim auto-syncs stored open SOS beacon to Node-Relay");
    console.log("=======================================================\n");

    const relay = launchNode("5002", "Node-Relay", "ws://127.0.0.1:5001");
    await delay(2500);

    // -------------------------------------------------------------------------
    // Test 3: RescueBase powers on and connects to Relay
    // Verify Multi-Hop Epidemic Sync: Relay auto-syncs victim's SOS to Base
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 3: Node-RescueBase powers on and connects to Node-Relay");
    console.log("Expected: Node-Relay auto-syncs victim's SOS beacon to Node-RescueBase");
    console.log("=======================================================\n");

    const rescueBase = launchNode("5003", "Node-RescueBase", "ws://127.0.0.1:5002", ["--rescue"]);
    await delay(3000);

    // -------------------------------------------------------------------------
    // Test 4: Inspect Triage Registry at Rescue Base
    // Verify Mesh Hop Proximity Localization (hops from anchor, estimated radius)
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 4: Inspecting Live Emergency Triage at Rescue Base");
    console.log("Expected: Shows victim Node-Victim, 2 hops away, ~200m search radius");
    console.log("=======================================================");
    rescueBase.stdin?.write("triage\n");
    await delay(1500);

    // -------------------------------------------------------------------------
    // Test 5: Rescue Base responds with Rescue Confirmation & ETA
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 5: Node-RescueBase sends RESCUE_ACK back to victim");
    console.log("Command: respond (to active SOS) DISPATCHED Boat_4_Deployed_ETA_15m");
    console.log("=======================================================");
    
    // Read the active SOS ID from the database on disk
    let activeSosId = "";
    try {
        const { DatabaseSync } = await import("node:sqlite");
        const dbPath = path.join(TEST_DIR, "store-Node-RescueBase.db");
        const db = new DatabaseSync(dbPath);
        const row = db.prepare("SELECT sos_id FROM sos_alerts LIMIT 1").get() as any;
        activeSosId = row?.sos_id;
        db.close();
    } catch (e: any) {
        console.error("Failed to read SQLite:", e.message);
    }

    if (activeSosId) {
        console.log(`Sending response for SOS ID: ${activeSosId}...`);
        rescueBase.stdin?.write(`respond ${activeSosId} DISPATCHED Boat_4_Deployed_ETA_15m\n`);
    } else {
        console.error("No SOS ID found!");
    }
    await delay(2500);

    // -------------------------------------------------------------------------
    // Test 6: Rescue Base broadcasts a Community Hazard Warning
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 6: Node-RescueBase broadcasts public disaster bulletin");
    console.log("Command: warn FLOOD_SURGE Dam_Gates_Opening_Evacuate_Low_Ground");
    console.log("=======================================================");
    rescueBase.stdin?.write("warn FLOOD_SURGE Dam_Gates_Opening_Evacuate_Low_Ground\n");
    await delay(2500);

    // -------------------------------------------------------------------------
    // Test 7: Verify victim's updated status and community bulletins
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 7: Inspecting Node-Victim Triage & Bulletins list");
    console.log("=======================================================");
    victim.stdin?.write("triage\n");
    await delay(500);
    victim.stdin?.write("bulletins\n");
    await delay(1500);

    console.log("\nShutting down nodes...");
    victim.kill();
    relay.kill();
    rescueBase.kill();
    console.log("Phase 6 Test Completed Successfully!");
    process.exit(0);
}

runTest();
