export interface Message {
    type: "MESSAGE";
    messageId: string;
    senderDeviceId: string;
    receiverDeviceId: string;
    payload: string;
    timestamp: number;
}
export interface IdentifyMessage {
    type: "IDENTIFY";

    deviceId: string;
}

export interface Peer {
    id: string;
    socket: WebSocket;
}