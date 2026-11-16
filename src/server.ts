import { WebSocketServer } from "ws";
import {Message} from "./types"
const PORT = 5000;
const server = new WebSocketServer({
    host:"0.0.0.0",
    port: PORT
});

import { randomUUID } from "crypto";
// Temp Static Device ID
const DeviceId="Node-A"
console.log(`DisasterMesh node started`);
console.log(`Device ID: ${DeviceId}`);
console.log(`Listening on port: ${PORT}`);

console.log(`DisasterMesh node running on port ${PORT}`);

server.on("connection",(socket)=>{
    console.log("Socket is Connected:")

    socket.on("message",(data)=>{
        try{
            const message:Message =JSON.parse(data.toString())
            console.log("\nMessage received:");
            console.log(`From: ${message.senderDeviceId}`);
            console.log(`Message: ${message.payload}`);
            const response: Message = {
                type: "MESSAGE",
                messageId: randomUUID(),
                senderDeviceId: DeviceId,
                receiverDeviceId: message.senderDeviceId,
                payload: `Received your message: ${message.payload}`,
                timestamp: Date.now(),
                ttl: 5,
                hopCount: 0
            };
            socket.send(JSON.stringify(response))
        }
        
        catch(error){
            console.log("Invalid Message Detected")
        }
        
    })

    socket.on("close",()=>{
        console.log("Socket Connection is Closed Gracefully");
    })
    socket.on("error", (error) => {
        console.log("Socket error:", error.message);
    });
}
)


server.on("error", (error) => {
    console.log("Server error:", error.message);
});