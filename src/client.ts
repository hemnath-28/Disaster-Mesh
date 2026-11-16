import WebSocket from "ws";
import { randomUUID } from "crypto";
import { Message } from "./types";
const SERVER_IP = "10.12.235.84";
const SERVER_PORT = 5000;
const DEVICE_ID = "NODE-B";
const client = new WebSocket(
    `ws://${SERVER_IP}:${SERVER_PORT}`
);

client.on("open", () => {

    console.log("Connected to peer");
    const message: Message = {
        type: "MESSAGE",
        messageId: randomUUID(),
        senderDeviceId: DEVICE_ID,
        receiverDeviceId: "NODE-A",
        payload: "Hello from NODE-B!",
        timestamp: Date.now(),
        ttl: 5,
        hopCount: 0
    };
    client.send("Hello from Node B!");
    client.send(JSON.stringify(message));
});

client.on("message",(data)=>{
    try {

        const message: Message = JSON.parse(data.toString());
        console.log("\nMessage received:");
        console.log(`From: ${message.senderDeviceId}`);
        console.log(`To: ${message.receiverDeviceId}`);
        console.log(`Message: ${message.payload}`);

    } catch (error) {

        console.log("Invalid message received");
    }
    console.log(`Received : ${data}`)
})

client.on("close",()=>{
    console.log("Client Disconnected")
})

client.on("error",(err)=>{
    console.log("Client is Facif issues:",err.message)

})
