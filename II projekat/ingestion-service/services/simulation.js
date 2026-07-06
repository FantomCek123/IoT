const fs = require('fs');
const config = require('../config/config');
const mqttBroker = require('../brokers/mqtt');
const kafkaBroker = require('../brokers/kafka');

let simulationInterval = null;
let alarmInterval = null;
let datasetIndex = 0;
let dataset = [];
let currentNumDevices = 0;

try {
    dataset = JSON.parse(fs.readFileSync(config.datasetPath, 'utf8'));
    console.log(`📦 Dataset uspešno učitan! Zapisa: ${dataset.length}`);
} catch (err) {
    console.error('❌ Greška pri učitavanju dataset-a:', err.message);
    process.exit(1);
}

function sendToActiveBroker(payload) {
    if (config.BROKER_TYPE === 'mqtt') mqttBroker.publishMQTT(payload);
    else if (config.BROKER_TYPE === 'kafka') kafkaBroker.sendKafka(payload);
}

function startRealTimeSimulation(numDevices) {


    if (simulationInterval) {
        clearInterval(simulationInterval);
        console.log(`🔄 Resetujem simulaciju za novi broj uređaja: ${numDevices}`);
    } else {
        console.log(`📡 Pokrenuta kontinuirana simulacija za ${numDevices} uređaja`);
    }
    
    currentNumDevices = numDevices;

    simulationInterval = setInterval(() => {
    console.log(`Sending batch for ${currentNumDevices} devices...`); 
    for (let i = 1; i <= currentNumDevices; i++) {
        try {                                                   
            if (datasetIndex >= dataset.length) datasetIndex = 0;
            const originalRecord = dataset[datasetIndex++];

            if (!originalRecord) continue;                     

            const payload = {
                deviceId: `${originalRecord.deviceId || 'sensor'}_sensor_${i}`, 
                temperature: originalRecord.temperature,
                humidity: originalRecord.humidity || 0.0,
                lightIntensity: originalRecord.lightIntensity || originalRecord.light_intensity || 0.0,
                voltage: originalRecord.voltage || 0.0,
                timestamp: new Date().toISOString()
            };
            sendToActiveBroker(payload);
        } catch (err) {
            console.error("Greška unutar petlje simulacije:", err.message);
        }
    }
}, 1000); 
}

function stopRealTimeSimulation() {
    if (simulationInterval) {
        clearInterval(simulationInterval);
        simulationInterval = null;
        return true;
    }
    return false;
}

function runCriticalAlarmStream() {
    if (alarmInterval) clearInterval(alarmInterval);
    console.log(`🚨 [Scenario D] Pokretanje toplotnog udara...`);

    alarmInterval = setInterval(() => {
        const currentTimestamp = new Date().toISOString();
        
        const payload = {
            deviceId: `critical_sensor_999`,
            temperature: 65.5,
            timestamp: currentTimestamp
        };
        
        console.log(`📤 [Poslato] Temp: ${payload.temperature}°C | Time: ${currentTimestamp}`);
        
        sendToActiveBroker(payload);
    }, 1000);

    setTimeout(() => {
        clearInterval(alarmInterval);
        console.log(`🛑 [Scenario D] Toplotni udar završen.`);
    }, 12000);
}
module.exports = { startRealTimeSimulation, stopRealTimeSimulation, runCriticalAlarmStream };