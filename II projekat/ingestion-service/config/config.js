const path = require('path');

module.exports = {
    BROKER_TYPE: process.env.BROKER_TYPE || 'mqtt',
    MQTT_URL: 'mqtt://mosquitto:1883',
    KAFKA_BROKER: 'kafka:9092',
    TOPIC_NAME: 'iot_sensor_data',
    MQTT_QOS: parseInt(process.env.MQTT_QOS || '0'),
    KAFKA_ACKS: process.env.KAFKA_ACKS || '1',
    PORT: 3000,
    datasetPath: path.join(__dirname, '../intel_data.json')
};