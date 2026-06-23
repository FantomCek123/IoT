const fs = require('fs');
const path = require('path');

const mqtt = require('mqtt');
const { Kafka } = require('kafkajs');

// === KONFIGURACIJA ===
const BROKER_TYPE = process.env.BROKER_TYPE || 'mqtt'; 
const MQTT_URL = 'mqtt://mosquitto:1883';
const KAFKA_BROKER = 'kafka:9092';
const TOPIC_NAME = 'iot_sensor_data';

const NUM_DEVICES = parseInt(process.env.NUM_DEVICES || '100'); // Scenario A: 100, 1000, 10000
const MQTT_QOS = parseInt(process.env.MQTT_QOS || '0');        // MQTT: 0, 1, 2
const KAFKA_ACKS = process.env.KAFKA_ACKS || '1';              // Kafka: 0, 1, all

console.log(`🚀 Simulator pokrenut! Tehnologija: ${BROKER_TYPE.toUpperCase()}`);
console.log(`📊 Broj simuliranih uređaja: ${NUM_DEVICES}`);

let mqttClient = null;
let kafkaProducer = null;

// === UCITAVANJE DATASETA ===
const datasetPath = path.join(__dirname, 'intel_data.json');
let dataset = [];

try {
    const rawData = fs.readFileSync(datasetPath, 'utf8');
    dataset = JSON.parse(rawData);
    console.log(`📦 Uspešno učitan dataset! Ukupno zapisa u fajlu: ${dataset.length}`);
} catch (err) {
    console.error('❌ Greška prilikom učitavanja intel_data.json fajla:', err.message);
    process.exit(1);
}

// === FUNKCIJA ZA POVEZIVANJE NA KAFKU SA RETRY LOGIKOM ===
async function connectKafkaWithRetry() {
    const kafka = new Kafka({ clientId: 'iot-simulator', brokers: [KAFKA_BROKER] });
    const producer = kafka.producer({ allowAutoTopicCreation: true });
    let connected = false;

    while (!connected) {
        try {
            console.log("⏳ Pokušavam povezivanje na Apache Kafku...");
            await producer.connect();
            connected = true;
            console.log(`✔ Uspešno povezan na Apache Kafku (acks=${KAFKA_ACKS})!`);
            return producer;
        } catch (error) {
            console.log("❌ Kafka broker nije dostupan, pokušavam ponovo za 3 sekunde...");
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
}

// === GLAVNA FUNKCIJA ZA START KONEKCIJE I SIMULACIJE ===
async function start() {
    if (BROKER_TYPE === 'mqtt') {
        mqttClient = mqtt.connect(MQTT_URL);
        mqttClient.on('connect', () => {
            console.log('✔ Uspešno povezan na MQTT (Mosquitto)!');
            startSimulating();
        });
        mqttClient.on('error', (err) => console.error('❌ MQTT Greška:', err));
    } else if (BROKER_TYPE === 'kafka') {
        kafkaProducer = await connectKafkaWithRetry();
        startSimulating();
    }
}

// === PETLJA ZA SIMULACIJU I SLANJE PORUKA ===
function startSimulating() {
    let datasetIndex = 0;

    setInterval(async () => {
        for (let i = 1; i <= NUM_DEVICES; i++) {
            if (datasetIndex >= dataset.length) {
                datasetIndex = 0;
            }

            const originalRecord = dataset[datasetIndex];
            datasetIndex++;

            const payload = {
                device_id: `sensor_${i}`, // Virtuelni ID zbog simulacije masovnosti uređaja
                temperature: originalRecord.temperature || originalRecord.value || 20.0,
                timestamp: new Date().toISOString() // Trenutno vreme radi real-time analitike
            };

            const messageString = JSON.stringify(payload);

            if (BROKER_TYPE === 'mqtt') {
                mqttClient.publish(TOPIC_NAME, messageString, { qos: MQTT_QOS });
            } else if (BROKER_TYPE === 'kafka') {
                try {
                    await kafkaProducer.send({
                        topic: TOPIC_NAME,
                        acks: KAFKA_ACKS === 'all' ? -1 : parseInt(KAFKA_ACKS),
                        messages: [{ key: payload.device_id, value: messageString }],
                    });
                } catch (error) {
                    console.error('❌ Greška pri slanju na Kafku:', error.message);
                }
            }
        }
    }, 1000); 
}

// Pokretanje aplikacije
start();