const mqtt = require('mqtt');
const config = require('../config/config');

let mqttClient = null;

function connectMQTT() {
    mqttClient = mqtt.connect(config.MQTT_URL, { reconnectPeriod: 3000 });
    mqttClient.on('connect', () => console.log('✔ Uspešno povezan na MQTT!'));
    return mqttClient;
}

function publishMQTT(payload) {
    if (mqttClient && mqttClient.connected) {
        mqttClient.publish(config.TOPIC_NAME, JSON.stringify(payload), { qos: config.MQTT_QOS });
    }
}

module.exports = { connectMQTT, publishMQTT };