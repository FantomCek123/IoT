import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import mqtt from 'k6/x/mqtt';
import kafka from 'k6/x/kafka';

// Učitavanje dataset-a
const dataset = new SharedArray('intel telemetry data', function () {
    return JSON.parse(open('./intel_data.json'));
});

const BROKER_TYPE = __ENV.BROKER_TYPE || 'kafka'; 
const MQTT_URL = 'tcp://mosquitto:1883';
const KAFKA_BROKER = 'kafka:9092';
const TOPIC_NAME = 'iot_sensor_data';

const MQTT_QOS = parseInt(__ENV.MQTT_QOS || '0');
const KAFKA_ACKS = __ENV.KAFKA_ACKS || '1';

export const options = {
    scenarios: {

        scenario_A_massive_ingestion: {
            executor: 'constant-vus',
            vus: 100, // 👈 Ovde menjaš 100 -> 1000 -> 10000 za odbranu projekta
            duration: '30s',    
            exec: 'runScenarioA',
            startTime: '0s', 
        },


        //scenario_B_network_failure: {
        //    executor: 'per-vu-iterations',
        //    vus: 1,
        //    iterations: 1, 
        //    maxDuration: '70s',
        //    exec: 'runScenarioB',
        //    startTime: '40s', 
        //},


        scenario_C_burst_load: {
            executor: 'ramping-arrival-rate',
            startRate: 50,          
            timeUnit: '1s',
            preAllocatedVUs: 100,   
            maxVUs: 2000,           
            stages: [
                { duration: '10s', target: 50 },   
                { duration: '3s', target: 5000 },  
                { duration: '5s', target: 5000 },  
                { duration: '5s', target: 5 }, 
            ],
            exec: 'runScenarioC',
            startTime: '120s',
        },

        scenario_D_realtime_alerting: {
            executor: 'per-vu-iterations',
            vus: 1,
            iterations: 1,
            maxDuration: '20s',
            exec: 'runScenarioD',
            startTime: '150s',
        },
    },
    thresholds: {
        checks: ['rate>0.95'], 
    },
};


let kafkaWriter = null;
let mqttClient = null;

if (BROKER_TYPE === 'kafka') {
    kafkaWriter = new kafka.Writer({ brokers: [KAFKA_BROKER], topic: TOPIC_NAME, acks: KAFKA_ACKS === 'all' ? -1 : parseInt(KAFKA_ACKS) });
} else if (BROKER_TYPE === 'mqtt') {
    mqttClient = mqtt.connect(MQTT_URL);
}

function sendPayload(payload) {
    if (BROKER_TYPE === 'mqtt' && mqttClient) mqttClient.publish(TOPIC_NAME, payload, { qos: MQTT_QOS });
    else if (BROKER_TYPE === 'kafka' && kafkaWriter) kafkaWriter.write({ value: payload });
}


export function runScenarioA() {
    const index = exec.scenario.iterationInScenario % dataset.length;
    const record = dataset[index];
    const payload = JSON.stringify({
        deviceId: `k6_device_${exec.vu.idInTest}`,
        temperature: record.temperature,
        timestamp: new Date().toISOString()
    });
    sendPayload(payload);
    check(payload, { 'Scenario A: Uspešno poslato': (p) => p.length > 0 });
    sleep(0.1); // Svaki uređaj šalje podatke na svakih 100ms
}


export function runScenarioB() {
    const startUrl = 'http://ingestion_service:3000/start-simulation';
    const stopUrl = 'http://ingestion_service:3000/stop-simulation';
    const params = { headers: { 'Content-Type': 'application/json' } };

    console.log("📡 [k6 - SCENARIO B] Pokrećem simulaciju (50 uređaja)...");
    http.post(startUrl, JSON.stringify({ num_devices: 50 }), params);


    sleep(60); 

    console.log("🛑 [k6 - SCENARIO B] Gasim simulaciju.");
    http.post(stopUrl, JSON.stringify({}), params);
}

export function runScenarioC() {
    const index = exec.scenario.iterationInScenario % dataset.length;
    const record = dataset[index];
    const payload = JSON.stringify({
        deviceId: `k6_burst_device`,
        temperature: record.temperature,
        timestamp: new Date().toISOString()
    });
    sendPayload(payload);
    check(payload, { 'Scenario C: Burst uspešan': (p) => p.length > 0 });
}

export function runScenarioD() {
    const url = 'http://ingestion_service:3000/trigger-alarm';
    console.log("🔥 [k6 - SCENARIO D] Šaljem zahtev za toplotni udar...");
    const res = http.post(url);
    check(res, { 'Scenario D: Alarm pokrenut': (r) => r.status === 200 });
    sleep(15);
}

export function teardown() {
    if (kafkaWriter) kafkaWriter.close();
    if (mqttClient) mqttClient.close();
}