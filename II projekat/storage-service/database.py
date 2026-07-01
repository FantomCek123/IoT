import time
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASS

def init_db():
    connected = False
    while not connected:
        try:
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
            connected = True
        except Exception as e:
            print(f"⏳ Baza još nije spremna ({e}), pokušavam ponovo za 3 sekunde...")
            time.sleep(3)

def insert_batch(batch_to_write):
    if not batch_to_write:
        return
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        query = "INSERT INTO sensor_measurements (device_id, temperature, timestamp) VALUES (%s, %s, %s)"
        cursor.executemany(query, batch_to_write)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"💾 Uspešno upisan batch od {len(batch_to_write)} poruka u PostgreSQL.")
    except Exception as e:
        print(f"❌ Greška prilikom upisa u bazu: {e}")