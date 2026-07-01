import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
import config
from services.window_processor import window_processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- ON STARTUP ---
    print(f"📈 Analytics servis se pokreće! Režim rada: {config.BROKER_TYPE.upper()}")
    
    # Pokretanje niti za obradu vremenskog prozora (Tumbling Window)
    window_thread = threading.Thread(target=window_processor.process_tumbling_window, daemon=True)
    window_thread.start()
    
    # Pokretanje niti za izabranog brokera
    if config.BROKER_TYPE == "mqtt":
        from brokers.mqtt_consumer import start_mqtt
        broker_thread = threading.Thread(target=start_mqtt, daemon=True)
        broker_thread.start()
    elif config.BROKER_TYPE == "kafka":
        from brokers.kafka_consumer import start_kafka
        broker_thread = threading.Thread(target=start_kafka, daemon=True)
        broker_thread.start()
        
    yield
    # --- ON SHUTDOWN ---
    print("🛑 Zaustavljam Analytics Service...")
    window_processor.stop()

app = FastAPI(title="Analytics Service", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "analytics_running", "broker": config.BROKER_TYPE}