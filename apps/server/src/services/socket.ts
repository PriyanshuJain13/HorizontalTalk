import { Server, Socket } from "socket.io";
import Redis from "ioredis";
import prismaClient from "./prisma";
import {produceMessage} from "./kafka";

//publisher
const pub = new Redis({
    host:'redis-33f5cefc-pj-programs-13-4775.a.aivencloud.com',
    port: 17436,
    username: 'default',
    password: 'AVNS__szHPnxO1GrEJPtrDRU',
});

//subscriber
const sub = new Redis({
    host:'redis-33f5cefc-pj-programs-13-4775.a.aivencloud.com',
    port: 17436,
    username: 'default',
    password: 'AVNS__szHPnxO1GrEJPtrDRU',
});

class SocketService{
    private _io: Server;
    constructor(){
        console.log('Init Socket Server...');
        this._io = new Server({
            cors: {
                allowedHeaders: ["*"],
                origin: "*",
            }
        });
        sub.subscribe('MESSAGES');
    }

    get io(){
        return this._io
    }

    public initListeners(){
        const io = this.io;
        console.log("Init Socket Listeners...")
        io.on('connect', (socket) =>{
            console.log('New Socket connected',socket.id);

            socket.on('event:message',async ({message}:{message: string}) =>{
                console.log('New Message received: ',message);

                //publish this msg to redis - ioredis use
                await pub.publish('MESSAGES',JSON.stringify({message}));
            }); 
        });

        sub.on('message',async(channel,message)=>{
            if(channel === 'MESSAGES'){
                console.log('New message from redis: ',message)
                io.emit('message',message);
                await produceMessage(message);
                console.log('Message Produces to kafka Broker');
                
            }
        })
    }
}

export default SocketService;
