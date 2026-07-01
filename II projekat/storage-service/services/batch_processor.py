import json
import threading
import time
from config import BATCH_SIZE
from database import insert_batch

class BatchProcessor:
    def __init__(self):
        self.msg_batch = []
        self.batch_lock = threading.Lock()
        self.is_running = True

    def handle_incoming_message(self, msg_body):
        try:
            data = json.loads(msg_body)
            device_id = data.get("deviceId") or data.get("device_id") or "unknown_sensor"
            row = (device_id, data["temperature"], data["timestamp"])
            
            with self.batch_lock:
                self.msg_batch.append(row)
                if len(self.msg_batch) >= BATCH_SIZE:
                    current_batch = self.msg_batch.copy()
                    self.msg_batch.clear()
                    threading.Thread(target=insert_batch, args=(current_batch,)).start()
        except Exception as e:
            print(f"❌ Greška prilikom obrade poruke: {e}")

    def start_periodic_flush_timer(self):
        """Metoda koja se vrti u pozadinskom thread-u i radi preventivni flush"""
        while self.is_running:
            time.sleep(3)
            if self.msg_batch:
                with self.batch_lock:
                    if self.msg_batch:
                        current_batch = self.msg_batch.copy()
                        self.msg_batch.clear()
                        print(f"⏱ Tajmer aktiviran: Upisujem preostalih {len(current_batch)} poruka.")
                        threading.Thread(target=insert_batch, args=(current_batch,)).start()

    def stop(self):
        self.is_running = False
        # Pred gašenje pokupi preostale poruke ako ih ima
        if self.msg_batch:
            insert_batch(self.msg_batch)

processor = BatchProcessor()