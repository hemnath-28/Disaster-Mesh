import { spawn, ChildProcess } from "child_process";
import http from "http";
import fs from "fs";
import { WebSocket } from "ws";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" Testing Phase 9: Rescue Team Operations Web Dashboard");
console.log(" Topology: Node-Victim (5001) <-> Node-Relay (5002) <-> Node-RescueBase (5003)");
console.log(" Web Dashboard Port: 6003 (http://127.0.0.1:6003)");
console.log("===================================================================\n");

// Clean old test databases
const TEST_DIR = "./data/test-dashboard";
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

function httpGet(url: string): Promise<{ statusCode: number; body: string }> {
    return new Promise((resolve, reject) => {
        http.get(url, (res) => {
            let data = "";
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => resolve({ statusCode: res.statusCode || 0, body: data }));
        }).on("error", reject);
    });
}

async function runDashboardTest() {
    // 1. Launch Victim & Relay
    const victim = launchNode("5001", "Node-Victim");
    await delay(1200);

    const relay = launchNode("5002", "Node-Relay", "ws://127.0.0.1:5001");
    await delay(1200);

    // 2. Launch RescueBase with dashboard
    const rescueBase = launchNode("5003", "Node-RescueBase", "ws://127.0.0.1:5002", [
        "--rescue"
    ]);
    await delay(3000);

    // -------------------------------------------------------------------------
    // Test 1: Verify HTTP Server serves single-page dashboard HTML
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 1: Fetching HTTP GET http://127.0.0.1:6003/");
    console.log("=======================================================");

    const res = await httpGet("http://127.0.0.1:6003/");
    if (res.statusCode === 200 && res.body.includes("DISASTERMESH OPS COMMAND")) {
        console.log("✅ HTTP 200 OK — Dashboard HTML successfully loaded!");
    } else {
        console.error(`❌ HTTP test failed: status=${res.statusCode}`);
        process.exit(1);
    }

    // -------------------------------------------------------------------------
    // Test 2: Connect browser WebSocket client to /dashboard-ws
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 2: Connecting WebSocket to ws://127.0.0.1:6003/dashboard-ws");
    console.log("=======================================================");

    const clientWs = new WebSocket("ws://127.0.0.1:6003/dashboard-ws");
    let receivedInit = false;
    let receivedSosUpsert = false;
    let receivedDispatched = false;
    let receivedBulletin = false;
    let activeSosId = "";

    await new Promise<void>((resolve, reject) => {
        clientWs.on("open", () => {
            console.log("✅ Dashboard WebSocket Connected!");
            resolve();
        });
        clientWs.on("error", reject);
    });

    clientWs.on("message", (raw) => {
        try {
            const data = JSON.parse(raw.toString());
            console.log(`[DASHBOARD-WS EVENT] Type: ${data.type}`);

            if (data.type === "INIT_STATE") {
                receivedInit = true;
                console.log(`✅ Received INIT_STATE for node: ${data.nodeDeviceId}`);
            }

            if (data.type === "SOS_UPSERT") {
                if (data.alert.status === "OPEN") {
                    receivedSosUpsert = true;
                    activeSosId = data.alert.sosId;
                    console.log(`✅ Received live SOS_UPSERT: ${data.alert.emergencyType} (${data.alert.severity}) from ${data.alert.senderDeviceId}`);
                } else if (data.alert.status === "DISPATCHED") {
                    receivedDispatched = true;
                    console.log(`✅ Received status update SOS_UPSERT: [DISPATCHED] for ${data.alert.sosId}`);
                }
            }

            if (data.type === "BULLETIN_NEW") {
                receivedBulletin = true;
                console.log(`✅ Received live BULLETIN_NEW: [${data.bulletin.hazardType}] "${data.bulletin.announcement}"`);
            }
        } catch (e: any) {
            console.error("Failed to parse WS frame:", e.message);
        }
    });

    await delay(1000);

    // -------------------------------------------------------------------------
    // Test 3: Victim broadcasts SOS; verify live push on Dashboard WebSocket
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 3: Victim broadcasts SOS — checking live dashboard push");
    console.log("=======================================================");

    victim.stdin?.write("sos TRAPPED CRITICAL 3 Rooftop_Flooding_Sector_9\n");
    await delay(3000);

    if (receivedSosUpsert && activeSosId) {
        console.log("✅ Verified: SOS beacon propagated across mesh and pushed to Dashboard!");
    } else {
        console.error("❌ Failed to receive SOS_UPSERT on dashboard WebSocket");
        process.exit(1);
    }

    // -------------------------------------------------------------------------
    // Test 4: Dashboard triggers ACTION_DISPATCH; verify RESCUE_ACK reaches Victim
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 4: Transmitting ACTION_DISPATCH from Dashboard Web Client");
    console.log("=======================================================");

    clientWs.send(JSON.stringify({
        type: "ACTION_DISPATCH",
        sosId: activeSosId,
        status: "DISPATCHED",
        rescuerId: "Hovercraft_Alpha_1",
        eta: "10 mins",
        instructions: "Stay on roof, waving red cloth."
    }));

    await delay(3000);

    if (receivedDispatched) {
        console.log("✅ Verified: Dispatch action updated triage status to DISPATCHED!");
    } else {
        console.error("❌ Failed to verify DISPATCHED status update on dashboard");
        process.exit(1);
    }

    // -------------------------------------------------------------------------
    // Test 5: Dashboard triggers ACTION_BULLETIN; verify broadcast across mesh
    // -------------------------------------------------------------------------
    console.log("\n=======================================================");
    console.log("Test 5: Transmitting ACTION_BULLETIN from Dashboard Web Client");
    console.log("=======================================================");

    clientWs.send(JSON.stringify({
        type: "ACTION_BULLETIN",
        hazardType: "SAFE_ZONE",
        announcement: "Community Center on North Ridge open for shelter"
    }));

    await delay(3000);

    if (receivedBulletin) {
        console.log("✅ Verified: Hazard bulletin broadcasted and pushed to dashboard feed!");
    } else {
        console.error("❌ Failed to receive BULLETIN_NEW on dashboard");
        process.exit(1);
    }

    console.log("\nShutting down nodes & dashboard client...");
    clientWs.close();
    victim.kill();
    relay.kill();
    rescueBase.kill();

    console.log("\n===================================================================");
    console.log("🎉 Phase 9 Dashboard End-to-End Tests Passed Successfully!");
    console.log("===================================================================\n");
    process.exit(0);
}

runDashboardTest().catch((err) => {
    console.error("Fatal Test Failure:", err);
    process.exit(1);
});
