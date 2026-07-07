import paho.mqtt.client as mqtt
import json
import requests
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

MAAS_URL = "http://iot_maas_p3:8000/predict"
MQTT_BROKER = "iot_mosquitto_p2"

def on_connect(client, userdata, flags, rc):
    logger.info("✔ Analytics povezan na MQTT broker.")
    # Slušamo oba kanala
    client.subscribe("iot_sensor_data")
    client.subscribe("sensors/ekuiper_alarms")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    
    # 1. Hvatanje eKuiper CEP alarma
    if msg.topic == "sensors/ekuiper_alarms":
        logger.warning(f"🚨 [eKuiper CEP ALERT] Detektovano pravilo: {payload}")
        return

    # 2. Hvatanje sirovih senzora i slanje na MaaS ML
    if msg.topic == "iot_sensor_data":  # ISPRAVLJENO OVDE!
        try:
            data = json.loads(payload)
            temp = float(data.get("temperature", 0))
            
            # Šaljemo temperaturu MaaS servisu na procenu
            response = requests.post(MAAS_URL, json={"temperature": temp}, timeout=2)
            if response.status_code == 200:
                result = response.json()
                
                # Ispisujemo šta je ML model rekao
                if result["status"] == "CRITICAL_ANOMALY":
                    logger.error(f"🤖 [MaaS ML Prediction] {result['status']} (Verovatnoća: {result['probability']*100}%) za Temp: {temp}°C")
                else:
                    logger.info(f"🤖 [MaaS ML Prediction] {result['status']} za Temp: {temp}°C")
                    
        except Exception as e:
            logger.error(f"Greška u obradi MaaS predikcije: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, 1883, 60)
client.loop_forever()