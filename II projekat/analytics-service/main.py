import os
import json
import threading
import time
from fastapi import FastAPI
import paho.mqtt.client as mqtt
from kafka import KafkaConsumer, errors

app = FastAPI(title="Analytics Service")

# === KONFIGURACIJA ===
BROKER_TYPE = os.getenv("BROKER_TYPE", "kafka")
TOPIC_NAME = "iot_sensor_data"
WINDOW_DURATION = 10  

current_window_temperatures = []
window_lock = threading.Lock()

print(f"📈 Analytics servis pokrenut! Prati se: {BROKER_TYPE.upper()}")

def process_tumbling_window():
    global current_window_temperatures
    while True:
        time.sleep(WINDOW_DURATION)
        
        with window_lock:
            temperatures_to_process = current_window_temperatures.copy()
            current_window_temperatures.clear()
        
        if len(temperatures_to_process) > 0:
            avg_temp = sum(temperatures_to_process) / len(temperatures_to_process)
            print(f"⏱️ [Prozor 10s] Obrađeno poruka: {len(temperatures_to_process)} | Prosečna temp: {avg_temp:.2f}°C")
            
            if avg_temp > 50.0:
                trenutno_vreme_ms = int(time.time() * 1000)
                print(f"🚨 🔥 [CRITICAL ALERT] Detektovana anomalija! Prosečna temperatura prozora iznosi {avg_temp:.2f}°C")
                print(f"⏱️ [Scenario D - END] Alarm ispisan u logu u: {trenutno_vreme_ms} ms")
        else:
            print("⏱️ [Prozor 10s] Nema pristiglih podataka u ovom vremenskom prozoru.")

# Funkcija koja hvata temperaturu iz pristigle poruke i stavlja je u prozor
def collect_message_data(msg_body):
    global current_window_temperatures
    try:
        data = json.loads(msg_body)
        temp = float(data.get("temperature") or data.get("value") or 0)
        
        with window_lock:
            current_window_temperatures.append(temp)
    except Exception as e:
        print(f"❌ Greška pri analizi poruke: {e}")

# --- MQTT KLIJENT ---
def start_mqtt():
    def on_message(client, userdata, msg):
        collect_message_data(msg.payload.decode())

    client = mqtt.Client()
    client.on_message = on_message
    
    connected = False
    while not connected:
        try:
            client.connect("mosquitto", 1883, 60)
            connected = True
            print("✅ Analytics se uspešno povezao na Mosquitto!")
        except Exception:
            print("⏳ Mosquitto još nije spreman, pokušavam ponovo za 3 sekunde...")
            time.sleep(3)
            
    client.subscribe(TOPIC_NAME, qos=0)
    client.loop_forever()

# --- KAFKA KONSUMER ---
def start_kafka():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='analytics-group',
                auto_offset_reset='earliest',  # Čita sve od početka ako je grupa nova
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("✅ Analytics se uspešno povezao na Kafku!")
        except errors.NoBrokersAvailable:
            print("⏳ Kafka još nije spremna, pokušavam ponovo za 3 sekunde...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Neočekivana greška pri povezivanju na Kafku: {e}")
            time.sleep(3)

    for msg in consumer:
        collect_message_data(msg.value)

threading.Thread(target=process_tumbling_window, daemon=True).start()

if BROKER_TYPE == "mqtt":
    threading.Thread(target=start_mqtt, daemon=True).start()
elif BROKER_TYPE == "kafka":
    threading.Thread(target=start_kafka, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "analytics_running", "broker": BROKER_TYPE}