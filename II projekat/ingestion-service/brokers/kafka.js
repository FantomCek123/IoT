const { Kafka } = require('kafkajs');
const config = require('../config/config');

let kafkaProducer = null;

async function connectKafka() {
    const kafka = new Kafka({ clientId: 'iot-simulator', brokers: [config.KAFKA_BROKER] });
    kafkaProducer = kafka.producer({ allowAutoTopicCreation: true });
    
    while (true) {
        try {
            await kafkaProducer.connect();
            console.log(`✔ Uspešno povezan na Kafku (acks=${config.KAFKA_ACKS})!`);
            return kafkaProducer;
        } catch (error) {
            console.log("❌ Kafka nije dostupna, pokušavam ponovo za 3s...");
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
}

async function sendKafka(payload) {
    if (kafkaProducer) {
        const messageString = JSON.stringify(payload);
        try {
            await kafkaProducer.send({
                topic: config.TOPIC_NAME,
                acks: config.KAFKA_ACKS === 'all' ? -1 : parseInt(config.KAFKA_ACKS),
                messages: [{ key: payload.deviceId, value: messageString }],
            });

            console.log(` -> Poruka poslata na Kafku za: ${payload.deviceId}`); 
        } catch (error) {

            console.error("❌ Greška pri slanju na Kafku:", error.message);
        }
    } else {
        console.error("❌ Greška: kafkaProducer nije inicijalizovan!");
    }
}
module.exports = { connectKafka, sendKafka };