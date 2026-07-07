import paho.mqtt.client as mqtt
from config import TOPIC_NAME
from services.batch_processor import processor

import logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def start_mqtt():
    def on_message(client, userdata, msg):
        payload = msg.payload.decode()
        # Vreme se automatski dodaje ovde
        logger.info(f"📥 [MQTT] Primljena poruka: {payload[:30]}...")
        processor.handle_incoming_message(payload)

    client = mqtt.Client()
    client.on_message = on_message
    
    # Dodajemo logovanje za konekciju
    try:
        client.connect("mosquitto", 1883, 60)
        client.subscribe(TOPIC_NAME, qos=0)
        logger.info("✔ [MQTT Consumer] Uspešno povezan na broker!")
        client.loop_forever()
    except Exception as e:
        logger.error(f"❌ Greška pri konekciji na MQTT: {e}")