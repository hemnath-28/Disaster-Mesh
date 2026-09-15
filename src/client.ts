import WebSocket from "ws";

const SERVER_IP = "localhost";
const SERVER_PORT = 5000;

const client = new WebSocket(
    `ws://${SERVER_IP}:${SERVER_PORT}`
);

client.on("open", () => {

    console.log("Connected to peer");

    client.send("Hello from Node B!");
});

client.on("message",(data)=>{
    console.log(`Received : ${data}`)
})

client.on("close",()=>{
    console.log("Client Disconnected")
})

client.on("error",(err)=>{
    console.log("Client is Facif issues:",err.message)

})
