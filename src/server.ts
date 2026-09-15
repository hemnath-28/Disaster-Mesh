import { WebSocketServer } from "ws";
const PORT = 5000;
const server = new WebSocketServer({
    port: PORT
});

console.log(`DisasterMesh node running on port ${PORT}`);

server.on("connection",(socket)=>{
    console.log("Socket is Connected:")

    socket.on("message",(data)=>{
        const message=data.toString()
        console.log("Message received:",message)
        socket.send(`echo ${message}`)
    })

    socket.on("close",()=>{
        console.log("Socket Connection is Closed Gracefully")
    })
}
)