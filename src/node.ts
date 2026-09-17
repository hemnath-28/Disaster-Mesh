import { WebSocketServer, WebSocket } from "ws";
import { randomUUID } from "crypto";
import { Message } from "./types";
import {IdentifyMessage } from "./types"
const HOST="0.0.0.0"
const PORT=5000

// Change this on each device
const DEVICE_ID = randomUUID();

// Optional peer to connect to
const PEER_IP: string = "10.12.234.137";

// Server Setup
const server = new WebSocketServer({
    host: HOST,
    port: PORT
});

//Server Part
const peers=new Map<string,WebSocket>()

// Function to Send Messag to Everyone
function sendmessage(message:Message){
    for (const peer of peers.values()){
        if (peer.readyState===WebSocket.OPEN){
            peer.send(JSON.stringify(message))
        }
    }
}


function sendSpecific(deviceId:string,message:Message){
    const peer = peers.get(deviceId);

    if (peer && peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify(message));
    }
}

console.log("DisasterMesh Node");
console.log(`Device ID: ${DEVICE_ID}`);
console.log(`Listening on port: ${PORT}`);

server.on("listening",()=>{
    console.log("Server is Listening")
})

server.on("connection", (socket) => {
    console.log("Peer connected");
    console.log("Peers:", peers.size);
    
    
    const message:IdentifyMessage={
        type:"IDENTIFY",
        deviceId:DEVICE_ID


    }
    socket.send(JSON.stringify(message));


    socket.on("message", (data) => {
        try {
            const message: Message|IdentifyMessage =
                JSON.parse(data.toString());
            // Peer identification
            if (message.type==="IDENTIFY"){
                console.log(`peer identified as ${message.deviceId}`);

                peers.set(message.deviceId,socket)
                return
            }


            if (message.type==="MESSAGE"){
                console.log("\nMessage received:");
            console.log(`From: ${message.senderDeviceId}`);
            console.log(`To: ${message.receiverDeviceId}`);
            console.log(`Message: ${message.payload}`);

            }
            
        } catch (error) {

            console.log("Invalid message received");

        }

    });
    socket.on("close", () => {
        console.log("Peer disconnected");
        console.log("Socket disconnected")

        for (const [deviceId,peerSocket] of peers){
            if (peerSocket==socket){
                peers.delete(deviceId)
                console.log(
                    `Removed peer: ${deviceId}`);
                break;
            }
            
        }
        
       
    });
    socket.on("error", (error) => {
        console.log("Peer socket error:",
            error.message);
        for (const [deviceId, peerSocket] of peers) {
            if (peerSocket === socket) {
                peers.delete(deviceId);
                console.log(`Removed peer: ${deviceId}`);
            break
            }
            
        }
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
        const IdentifyMessage={
            type:"IDENTIFY",
            deviceId:DEVICE_ID
        }
        console.log("Peers:", peers.size);
        
        socket.send(JSON.stringify(IdentifyMessage))
        const message: Message = {

                type: "MESSAGE",

                messageId: randomUUID(),

                senderDeviceId: DEVICE_ID,

                receiverDeviceId: "NODE-A",

                payload: `Hello from ${DEVICE_ID}!`,

                timestamp: Date.now()

            };
        sendmessage(message)
    });
    socket.on("message", (data) => {

        try {

            const message: Message|IdentifyMessage =
                JSON.parse(data.toString());
            if (message.type=="IDENTIFY"){
                peers.set(message.deviceId,socket)
                return
            }
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

