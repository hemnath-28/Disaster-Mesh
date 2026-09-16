import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import { Message } from "./types";

const HOST="0.0.0.0"
const PORT=5000


// Change this on each device
const DEVICE_ID = "NODE-B";

// Optional peer to connect to
const PEER_IP: string = "10.12.234.137";

//Server Part
const peers=new Set<WebSocket>()
const server = new WebSocketServer({
    host: HOST,
    port: PORT
});

console.log("     DisasterMesh Node");
console.log(`Device ID: ${DEVICE_ID}`);
console.log(`Listening on port: ${PORT}`);

server.on("listening",()=>{
    console.log("Server is Listening")
})

server.on("connection", (socket) => {
    console.log("Peer connected");
    peers.add(socket);
     console.log("Peers:", peers.size);

    socket.on("message", (data) => {
        try {
            const message: Message =
                JSON.parse(data.toString());

            console.log("\nMessage received:");
            console.log(`From: ${message.senderDeviceId}`);
            console.log(`To: ${message.receiverDeviceId}`);
            console.log(`Message: ${message.payload}`);

        } catch (error) {

            console.log("Invalid message received");

        }

    });
    socket.on("close", () => {
        console.log("Peer disconnected");
        peers.delete(socket);
    });
    socket.on("error", (error) => {
        console.log("Peer socket error:",
            error.message);
        peers.delete(socket);
    });

});

// All connected Clients
console.log("Currently Connected Clients:",server.clients)

// Client Part

if (PEER_IP!==""){
    const socket = new WebSocket(
    `ws://${PEER_IP}:${PORT}`);
    socket.on("open",()=>{
        console.log(`Connected to peer ${PEER_IP}`,peers);

    console.log("Peers:", peers.size);
        peers.add(socket)
        const message: Message = {

                type: "MESSAGE",

                messageId: randomUUID(),

                senderDeviceId: DEVICE_ID,

                receiverDeviceId: "NODE-A",

                payload: `Hello from ${DEVICE_ID}!`,

                timestamp: Date.now()

            };
        socket.send(
                JSON.stringify(message)
            );
    });
    socket.on("message", (data) => {

        try {

            const message: Message =
                JSON.parse(data.toString());

            console.log("\nMessage received:");
            console.log(`From: ${message.senderDeviceId}`);
            console.log(`To: ${message.receiverDeviceId}`);
            console.log(`Message: ${message.payload}`);

        } catch (error) {

            console.log("Invalid message received");

        }

    });


    socket.on("close", () => {

        console.log("Connection to peer closed");

        peers.delete(socket);
        console.log("peers size:",peers.size)

    });


    socket.on("error", (error) => {

        console.log(
            "Connection error:",
            error.message
        );

})


}


console.log("peers:",peers)

