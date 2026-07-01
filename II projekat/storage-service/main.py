import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import config
from database import init_db
from services.batch_processor import processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- ON STARTUP ---
    init_db()  # Prvo proveri i kreiraj tabelu
    
    # Pokretanje tajmera za flush
    timer_thread = threading.Thread(target=processor.start_periodic_flush_timer, daemon=True)
    timer_thread.start()
    
    if config.BROKER_TYPE == "mqtt":
        from brokers.mqtt_consumer import start_mqtt
        broker_thread = threading.Thread(target=start_mqtt, daemon=True)
        broker_thread.start()
    elif config.BROKER_TYPE == "kafka":
        from brokers.kafka_consumer import start_kafka
        broker_thread = threading.Thread(target=start_kafka, daemon=True)
        broker_thread.start()
        
    yield
    print("🛑 Zaustavljam Storage Service i čistim preostale bafere...")
    processor.stop()

app = FastAPI(title="Data Storage Service", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "running", "broker": config.BROKER_TYPE}


import json
import time
from pydantic import BaseModel

class TestPayload(BaseModel):
    deviceId: str
    temperature: float
    timestamp: str = None  # Ako ne pošalješ, generisaćemo ga dole

@app.post("/test-direct-insert")
async def test_direct_insert(payload: TestPayload):
    try:
        print(f"👉 Manuelno primljen podatak za {payload.deviceId}, procesiram...")
        
        # 1. Pošto handle_incoming_message očekuje string (JSON), spakovaćemo rečnik
        # i pretvoriti ga u tekst preko json.dumps. 
        # Pazimo da timestamp ima vrednost jer ti je obavezan u bazi.
        msg_dict = {
            "deviceId": payload.deviceId,
            "temperature": payload.temperature,
            "timestamp": payload.timestamp or time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        msg_body_string = json.dumps(msg_dict)
        
        # 2. Pozivamo tvoju funkciju i prosleđujemo joj JSON string
        processor.handle_incoming_message(msg_body_string)
        
        return {
            "status": "success", 
            "message": f"Podatak ubačen u BatchProcessor za {payload.deviceId}. Sačekaj 3 sekunde da tajmer odradi flush u bazu."
        }
    except Exception as e:
        print(f"❌ Greška unutar test endpointa: {str(e)}")
        return {"status": "error", "message": str(e)}