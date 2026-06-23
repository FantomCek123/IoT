const fs = require('fs');
const path = require('path');
const http = require('http');

const mqtt = require('mqtt');
const { Kafka } = require('kafkajs');

// === KONFIGURACIJA ===
const BROKER_TYPE = process.env.BROKER_TYPE || 'mqtt'; 
const MQTT_URL = 'mqtt://mosquitto:1883';
const KAFKA_BROKER = 'kafka:9092';
const TOPIC_NAME = 'iot_sensor_data';

const MQTT_QOS = parseInt(process.env.MQTT_QOS || '0');         
const KAFKA_ACKS = process.env.KAFKA_ACKS || '1';    

console.log(`🚀 Simulator pokrenut! Tehnologija: ${BROKER_TYPE.toUpperCase()}`);

let mqttClient = null;
let kafkaProducer = null;

// Učitavanje dataset-a
const datasetPath = path.join(__dirname, 'intel_data.json');
let dataset = [];
try {
    const rawData = fs.readFileSync(datasetPath, 'utf8');
    dataset = JSON.parse(rawData);
    console.log(`📦 Dataset uspešno učitan! Zapisa: ${dataset.length}`);
} catch (err) {
    console.error('❌ Greška pri učitavanju dataset-a:', err.message);
    process.exit(1);
}

// Kafka konekcija sa retry mehanizmom
async function connectKafkaWithRetry() {
    const kafka = new Kafka({ clientId: 'iot-simulator', brokers: [KAFKA_BROKER] });
    const producer = kafka.producer({ allowAutoTopicCreation: true });
    let connected = false;
    while (!connected) {
        try {
            await producer.connect();
            connected = true;
            console.log(`✔ Uspešno povezan na Kafku (acks=${KAFKA_ACKS})!`);
            return producer;
        } catch (error) {
            console.log("❌ Kafka nije dostupna, pokušavam ponovo za 3s...");
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
}

// Pomoćna funkcija za slanje jedne poruke na broker
async function sendToBroker(payload) {
    const messageString = JSON.stringify(payload);
    if (BROKER_TYPE === 'mqtt' && mqttClient && mqttClient.connected) {
        mqttClient.publish(TOPIC_NAME, messageString, { qos: MQTT_QOS });
    } else if (BROKER_TYPE === 'kafka' && kafkaProducer) {
        await kafkaProducer.send({
            topic: TOPIC_NAME,
            acks: KAFKA_ACKS === 'all' ? -1 : parseInt(KAFKA_ACKS),
            messages: [{ key: payload.device_id, value: messageString }],
        });
    }
}

// Startovanje brokera i HTTP servera za upravljanje testovima
async function start() {
    if (BROKER_TYPE === 'mqtt') {
        mqttClient = mqtt.connect(MQTT_URL, { reconnectPeriod: 3000 });
        mqttClient.on('connect', () => console.log('✔ Uspešno povezan na MQTT!'));
    } else if (BROKER_TYPE === 'kafka') {
        kafkaProducer = await connectKafkaWithRetry();
    }

    const server = http.createServer((req, res) => {
        if (req.method === 'POST' && req.url === '/run-scenario') {
            let body = '';
            req.on('data', chunk => { body += chunk.toString(); });
            req.on('end', async () => {
                const config = JSON.parse(body);
                const scenario = config.scenario;
                const numDevices = config.num_devices || 100;

                res.writeHead(200, { 'Content-Type': 'application/json' });

                // --- SCENARIO A: Masovni unos podataka ---
                if (scenario === 'A') {
                    let datasetIndex = 0;
                    for (let i = 1; i <= numDevices; i++) {
                        if (datasetIndex >= dataset.length) datasetIndex = 0;
                        const originalRecord = dataset[datasetIndex++];
                        
                        const payload = {
                            device_id: `sensor_${i}`,
                            temperature: originalRecord.temperature || originalRecord.value || 20.0,
                            timestamp: new Date().toISOString()
                        };
                        sendToBroker(payload);
                    }
                    return res.end(JSON.stringify({ status: `Scenario A pokrenut za ${numDevices} uređaja.` }));
                }

                // --- SCENARIO C: Burst opterećenje ---
                if (scenario === 'C') {
                    // Nagli skok: šaljemo 5000 poruka odjednom u par sekundi
                    for (let i = 1; i <= 5000; i++) {
                        const payload = {
                            device_id: `burst_sensor_${i}`,
                            temperature: 25.0,
                            timestamp: new Date().toISOString()
                        };
                        sendToBroker(payload);
                    }
                    return res.end(JSON.stringify({ status: "Scenario C: Burst od 5000 poruka poslat!" }));
                }

                // --- SCENARIO D: Real-Time Alerting (Kritična vrednost) ---
                if (scenario === 'D') {
                    const payload = {
                        device_id: `critical_sensor_999`,
                        temperature: 65.5, // Direktno okida prosek iznad 50C
                        timestamp: new Date().toISOString()
                    };
                    console.log(`🚨 [Scenario D - START] Kritična vrednost generisana u simulatoru u: ${new Date().getTime()} ms`);
                    await sendToBroker(payload);
                    return res.end(JSON.stringify({ status: "Scenario D: Kritična vrednost poslata." }));
                }

                res.end(JSON.stringify({ error: "Nepoznat scenario" }));
            });
        } else {
            res.writeHead(404);
            res.end();
        }
    });

    server.listen(3000, () => console.log('🌐 Ingestion HTTP kontroler sluša na portu 3000...'));
}

start();