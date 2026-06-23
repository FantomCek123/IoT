import os
import json
import threading
import time
from fastapi import FastAPI
import paho.mqtt.client as mqtt
from kafka import KafkaConsumer, errors
app = FastAPI(title="Analytics Service")

# === KONFIGURACIJA ===
BROKER_TYPE = os.getenv("BROKER_TYPE", "mqtt")
TOPIC_NAME = "iot_sensor_data"
WINDOW_DURATION = 10  # Fiksni prozor od 10 sekundi

# Memorija za čuvanje temperatura unutar trenutnog prozora
current_window_temperatures = []
window_lock = threading.Lock()

print(f"📈 Analytics servis pokrenut! Prati se: {BROKER_TYPE.upper()}")

# --- LOGIKA ZA TUMBLING WINDOW (Pokreće se na svakih 10s) ---
def process_tumbling_window():
    global current_window_temperatures
    while True:
        time.sleep(WINDOW_DURATION)
        
        with window_lock:
            # Kopiramo podatke iz trenutnog prozora i odmah praznimo listu za sledeći prozor
            temperatures_to_process = current_window_temperatures.copy()
            current_window_temperatures.clear()
        
        if len(temperatures_to_process) > 0:
            # Računanje prosečne vrednosti senzora u prozoru
            avg_temp = sum(temperatures_to_process) / len(temperatures_to_process)
            print(f"⏱️ [Prozor 10s] Obrađeno poruka: {len(temperatures_to_process)} | Prosečna temp: {avg_temp:.2f}°C")
            
            # Ako je prosek veći od definisanog praga (> 50°C), ispisuje kritičan alarm u log
            if avg_temp > 50.0:
                print(f"🚨 🔥 [CRITICAL ALERT] Detektovana anomalija! Prosečna temperatura prozora iznosi {avg_temp:.2f}°C, što je iznad praga od 50°C!")
        else:
            print("⏱️ [Prozor 10s] Nema pristiglih podataka u ovom vremenskom prozoru.")

# Funkcija koja hvata temperaturu iz pristigle poruke i stavlja je u prozor
def collect_message_data(msg_body):
    global current_window_temperatures
    try:
        data = json.loads(msg_body)
        temp = float(data.get("temperature", 0))
        
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
    client.connect("mosquitto", 1883, 60)
    client.subscribe(TOPIC_NAME, qos=0)
    client.loop_forever()

# --- KAFKA KONSUMER ---
def start_kafka():
    consumer = None
    # Pokušavaj sve dok se ne povežeš na Kafku
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='analytics-group',
                auto_offset_reset='earliest',  # <--- PROMENI OVDE SA 'latest' NA 'earliest'
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("✅ Analytics se uspešno povezao na Kafku!")
        except errors.NoBrokersAvailable:
            print("⏳ Kafka još nije spremna, pokušavam ponovo za 3 sekunde...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Neočekivana greška pri povezivanju na Kafku: {e}")
            time.sleep(3)

    # Kada se konačno poveže, kreni da čitaš poruke
    for msg in consumer:
        collect_message_data(msg.value)

# Pokretanje tajmera za Tumbling Window u pozadini
threading.Thread(target=process_tumbling_window, daemon=True).start()

# Pokretanje slušanja brokera
if BROKER_TYPE == "mqtt":
    threading.Thread(target=start_mqtt, daemon=True).start()
elif BROKER_TYPE == "kafka":
    threading.Thread(target=start_kafka, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "analytics_running", "broker": BROKER_TYPE}