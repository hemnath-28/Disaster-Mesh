import { spawn, ChildProcess } from "child_process";
import fs from "fs";

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

console.log("===================================================================");
console.log(" 🚨 DISASTERMESH LIVE OPERATIONS DEMO");
console.log("===================================================================");
console.log(" Spawning 3-Node Topology:");
console.log(" [Node-Victim :5001] <---> [Node-Relay :5002] <---> [Node-RescueBase :5003]");
console.log("");
console.log(" 🌐 RESCUE WEB DASHBOARD:");
console.log(" 👉 http://localhost:6003");
console.log("===================================================================\n");

// Clean old test databases
if (fs.existsSync("./data")) {
    fs.rmSync("./data", { recursive: true, force: true });
}

function launchNode(port: string, name: string, peerUrl?: string, extraArgs: string[] = []): ChildProcess {
    const args = ["tsx", "src/node.ts", port, name];
    if (peerUrl) args.push(peerUrl);
    args.push(...extraArgs);

    const proc = spawn("npx", args, {
        env: { ...process.env, ENABLE_MDNS: "false", DASHBOARD_PORT: "6003" },
        stdio: ["pipe", "inherit", "inherit"]
    });
    return proc;
}

async function startDemo() {
    // 1. Launch Victim
    console.log("[1/3] Launching Node-Victim on port 5001...");
    const victim = launchNode("5001", "Node-Victim");
    await delay(1200);

    // 2. Launch Relay
    console.log("[2/3] Launching Node-Relay on port 5002 connected to Node-Victim...");
    const relay = launchNode("5002", "Node-Relay", "ws://127.0.0.1:5001");
    await delay(1200);

    // 3. Launch RescueBase
    console.log("[3/3] Launching Node-RescueBase on port 5003 with Web Dashboard on port 6003...");
    const rescueBase = launchNode("5003", "Node-RescueBase", "ws://127.0.0.1:5002", ["--rescue"]);
    await delay(3500);

    console.log("\n===================================================================");
    console.log(" ✅ All 3 nodes online and interconnected!");
    console.log(" 🌐 Open your browser and visit: http://localhost:6003");
    console.log("===================================================================\n");

    // Seed a sample emergency SOS beacon from Victim
    await delay(2000);
    console.log("📢 [SIMULATION] Node-Victim broadcasting initial Emergency SOS beacon...");
    victim.stdin?.write("sos TRAPPED CRITICAL 4 Water_Rising_2nd_Floor_Rooftop\n");

    // Seed an initial community hazard bulletin from RescueBase
    await delay(2000);
    console.log("📢 [SIMULATION] Node-RescueBase issuing Dam Gates Opening Flood Warning...");
    rescueBase.stdin?.write("warn FLOOD_SURGE Dam_Gates_Opened_Evacuate_Low_Ground_Immediate\n");

    console.log("\n✨ System running live! Keep this running while exploring http://localhost:6003");
    console.log("Press Ctrl+C to stop all nodes.\n");

    const cleanup = () => {
        console.log("\nStopping all nodes...");
        try { victim.kill(); } catch (_) {}
        try { relay.kill(); } catch (_) {}
        try { rescueBase.kill(); } catch (_) {}
        process.exit(0);
    };

    process.on("SIGINT", cleanup);
    process.on("SIGTERM", cleanup);

    // Keep process alive indefinitely
    await new Promise(() => {});
}

startDemo().catch(console.error);
