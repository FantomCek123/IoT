import time
import paho.mqtt.client as mqtt
from config import TOPIC_NAME
from services.window_processor import window_processor

def start_mqtt():
    def on_message(client, userdata, msg):
        window_processor.collect_message_data(msg.payload.decode())

    client = mqtt.Client()
    client.on_message = on_message
    
    connected = False
    while not connected:
        try:
            client.connect("mosquitto", 1883, 60)
            connected = True
            print("✅ [MQTT Analytics] Uspešno povezan na Mosquitto!")
        except Exception:
            print("⏳ [MQTT Analytics] Mosquitto još nije spreman, pokušavam ponovo za 3s...")
            time.sleep(3)
            
    client.subscribe(TOPIC_NAME, qos=0)
    client.loop_forever()