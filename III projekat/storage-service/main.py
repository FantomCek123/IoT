import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import config
from database import init_db
from services.batch_processor import processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    
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
