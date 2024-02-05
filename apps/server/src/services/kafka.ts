import { Kafka , Producer} from "kafkajs";
import fs from 'fs';
import path from 'path';
import prismaClient from "./prisma";

const kafka = new Kafka({
    brokers: ['kafka-2a4860fb-pj-programs-13-4775.a.aivencloud.com:17449'],
    ssl:{
        ca:[fs.readFileSync(path.resolve('./ca.pem'),'utf-8')],
    },
    sasl: {
        username: 'avnadmin',
        password: 'AVNS_aXtwI2-S6wZZ2nbuQn3',
        mechanism: 'plain',

    }
})

let producer: null | Producer = null;

export async function createProducer(){

    if(producer) return producer;

    const _producer = kafka.producer();
    await _producer.connect();
    producer = _producer;
    return producer
}

export async function produceMessage(message: string){
    const producer = await createProducer();
    await producer.send({
        messages: [{key : `message-${Date.now()}`,value: message,}],
        topic: 'MESSAGES',
    })
}

export async function startMessageConsumer(){
    console.log("Consumer started")
    const consumer = await kafka.consumer({groupId: 'default-group'});
    await consumer.connect();
    await consumer.subscribe({topic: 'MESSAGES',fromBeginning: true});
    await consumer.run({
        autoCommit: true,
        eachMessage: async ({message,pause})=>{
            console.log('New message! ');
            if(!message.value) return;
            try{
                await prismaClient.message.create({
                    data: {
                        text: message.value?.toString(),
                    }   
                })
            }
            catch(err){
                console.log('Something is wrong')
                pause();
                setTimeout(()=>{
                    consumer.resume([{topic:'MESSAGES'}]);
                },60000);
            }
            
        }
    })
}
export default kafka;