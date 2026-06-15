import os
import json
import threading
import psycopg2
from fastapi import FastAPI
import paho.mqtt.client as mqtt
from kafka import KafkaConsumer

app = FastAPI(title="Data Storage Service")

# === KONFIGURACIJA ===
BROKER_TYPE = os.getenv("BROKER_TYPE", "mqtt")
DB_HOST = "postgres_db"
DB_NAME = "iot_p2_db"
DB_USER = "vukasin"
DB_PASS = "iotpassword"
TOPIC_NAME = "iot_sensor_data"

BATCH_SIZE = 500
msg_batch = []
batch_lock = threading.Lock()

print(f"📊 Storage servis pokrenut! Sluša se: {BROKER_TYPE.upper()}")

# --- INICIJALIZACIJA BAZE ---
def init_db():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_measurements (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(50),
            temperature REAL,
            timestamp TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✔ Baza podataka je uspešno inicijalizovana!")

init_db()

# --- FUNKCIJA ZA BATCH UPIS ---
def flush_batch(batch_to_write):
    if not batch_to_write:
        return
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        
        # SQL za masovni insert (brže od pojedinačnih upisa)
        query = "INSERT INTO sensor_measurements (device_id, temperature, timestamp) VALUES (%s, %s, %s)"
        cursor.executemany(query, batch_to_write)
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Uspešno upisan batch od {len(batch_to_write)} poruka u PostgreSQL.")
    except Exception as e:
        print(f"❌ Greška prilikom upisa u bazu: {e}")

# Funkcija koja dodaje poruku u batch
def handle_incoming_message(msg_body):
    global msg_batch
    try:
        data = json.loads(msg_body)
        row = (data["device_id"], data["temperature"], data["timestamp"])
        
        with batch_lock:
            msg_batch.append(row)
            if len(msg_batch) >= BATCH_SIZE:
                # Ako smo stigli do 500, uzmi ih i isprazni glavnu listu odmah
                current_batch = msg_batch.copy()
                msg_batch.clear()
                # Pokreni upis u bazu u posebnom thread-u da ne koči prijem novih poruka
                threading.Thread(target=flush_batch, args=(current_batch,)).start()
                
    except Exception as e:
        print(f"❌ Greška prilikom obrade poruke: {e}")

# --- MQTT KLIJENT ---
def start_mqtt():
    def on_message(client, userdata, msg):
        handle_incoming_message(msg.payload.decode())

    client = mqtt.Client()
    client.on_message = on_message
    client.connect("mosquitto", 1883, 60)
    client.subscribe(TOPIC_NAME, qos=0) # QoS menjamo u zavisnosti od testa
    client.loop_forever()

# --- KAFKA KONSUMER ---
def start_kafka():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['kafka:9092'],
        group_id='storage-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    for msg in consumer:
        handle_incoming_message(msg.value)

# Pokretanje slušanja u pozadinskom thread-u da ne blokira FastAPI
if BROKER_TYPE == "mqtt":
    threading.Thread(target=start_mqtt, daemon=True).start()
elif BROKER_TYPE == "kafka":
    threading.Thread(target=start_kafka, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "running", "broker": BROKER_TYPE}