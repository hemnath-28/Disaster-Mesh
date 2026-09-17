import mdns from "multicast-dns";
import os from "os";
import { randomUUID } from "crypto";

const discovery = mdns();

const SERVICE = "_disastermesh._tcp.local";
const PORT = 5000;
const DEVICE_ID = randomUUID();


// Get this device's LAN IPv4 address
function getLocalIP(): string {

    const interfaces = os.networkInterfaces();

    for (const name of Object.keys(interfaces)) {

        const networkInterface = interfaces[name];

        if (!networkInterface) {
            continue;
        }

        for (const address of networkInterface) {

            if (
                address.family === "IPv4" &&
                !address.internal
            ) {
                return address.address;
            }

        }
    }

    throw new Error("Could not find local IP address");
}


const LOCAL_IP = getLocalIP();


// Create a unique mDNS hostname for this device
const HOSTNAME = `node-${DEVICE_ID}.local`;


console.log("================================");
console.log("DisasterMesh mDNS Discovery");
console.log("================================");

console.log("Device ID:", DEVICE_ID);
console.log("Local IP:", LOCAL_IP);
console.log("Port:", PORT);
console.log("Service:", SERVICE);
console.log();


// Handle mDNS errors
discovery.on("error", (error) => {

    console.error("mDNS error:", error);

});


// Respond when another device searches for us in the mesh
// It responds with its own device info
discovery.on("query", (query) => {

    for (const question of query.questions) {

        if (
            question.name === SERVICE &&
            question.type === "PTR"
        ) {

            console.log("mDNS query received");


            discovery.respond({

                // Service → Device
                answers: [
                    {
                        name: SERVICE,
                        type: "PTR",
                        ttl: 120,
                        data: HOSTNAME
                    }
                ],

                additionals: [

                    // Device → Port
                    {
                        name: HOSTNAME,
                        type: "SRV",
                        ttl: 120,
                        data: {
                            port: PORT,
                            priority: 0,
                            weight: 0,
                            target: HOSTNAME
                        }
                    },

                    // Hostname → IP
                    {
                        name: HOSTNAME,
                        type: "A",
                        ttl: 120,
                        data: LOCAL_IP
                    },

                    // Device information
                    {
                        name: HOSTNAME,
                        type: "TXT",
                        ttl: 120,
                        data: [
                            `deviceId=${DEVICE_ID}`
                        ]
                    }

                ]

            });

            console.log("mDNS response sent");
        }
    }
});


// Process discovered mDNS responses
discovery.on("response", (response) => {

    let discoveredHostname: string | undefined;
    let discoveredIP: string | undefined;
    let discoveredPort: number | undefined;
    let discoveredDeviceId: string | undefined;


    // Find the device hostname from PTR
    for (const answer of response.answers) {

        if (
            answer.name === SERVICE &&
            answer.type === "PTR"
        ) {

            discoveredHostname = answer.data;
        }
    }


    if (!discoveredHostname) {
        return;
    }


    // Extract IP, port and Device ID
    for (const answer of response.additionals ?? []) {

        // Get port
        if (
            answer.type === "SRV" &&
            answer.name === discoveredHostname
        ) {

            discoveredPort = answer.data.port;
        }


        // Get IP
        if (
            answer.type === "A" &&
            answer.name === discoveredHostname
        ) {

            discoveredIP = answer.data;
        }


        // Get Device ID
        if (
            answer.type === "TXT" &&
            answer.name === discoveredHostname
        ) {

            for (const text of answer.data) {

                const value = text.toString();

                if (value.startsWith("deviceId=")) {

                    discoveredDeviceId =
                        value.substring("deviceId=".length);
                }
            }
        }
    }


    // Ignore our own advertisement
    if (discoveredDeviceId === DEVICE_ID) {
        return;
    }


    // Display discovered peer
    if (
        discoveredIP &&
        discoveredPort &&
        discoveredDeviceId
    ) {

        console.log();
        console.log("==============================");
        console.log("DisasterMesh peer discovered");
        console.log("==============================");

        console.log(
            "Device ID:",
            discoveredDeviceId
        );

        console.log(
            "IP:",
            discoveredIP
        );

        console.log(
            "Port:",
            discoveredPort
        );

        console.log();
    }
});


// Search for DisasterMesh devices
function discoverPeers(): void {

    console.log("Searching for peers...");

    discovery.query([
        {
            name: SERVICE,
            type: "PTR"
        }
    ]);
}


// Start the first discovery
discoverPeers();


// Repeat discovery every 5 seconds
setInterval(() => {

    discoverPeers();

}, 5000);