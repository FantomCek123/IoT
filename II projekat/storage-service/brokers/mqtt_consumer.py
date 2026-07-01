import paho.mqtt.client as mqtt
from config import TOPIC_NAME
from services.batch_processor import processor

def start_mqtt():
    def on_message(client, userdata, msg):
        processor.handle_incoming_message(msg.payload.decode())

    client = mqtt.Client()
    client.on_message = on_message
    client.connect("mosquitto", 1883, 60)
    client.subscribe(TOPIC_NAME, qos=0)
    print("✔ [MQTT Consumer] Slušam poruke na Mosquitto brokeru...")
    client.loop_forever()