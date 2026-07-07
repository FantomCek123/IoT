const http = require('http');
const config = require('./config/config');
const mqttBroker = require('./brokers/mqtt');
const kafkaBroker = require('./brokers/kafka');
const { handleRoutes } = require('./routes/router');

async function init() {
    console.log(`🚀 Ingestion servis se pokreće u [${config.BROKER_TYPE.toUpperCase()}] režimu...`);

    if (config.BROKER_TYPE === 'mqtt') {
        mqttBroker.connectMQTT();
    } else if (config.BROKER_TYPE === 'kafka') {
        await kafkaBroker.connectKafka();
    }


    const server = http.createServer(handleRoutes);

    server.listen(config.PORT, () => {
        console.log(`🌐 HTTP kontroler spreman na portu ${config.PORT}. Čekam k6 komande...`);
    });
}

init();