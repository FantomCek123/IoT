import json
import threading
import time
from datetime import datetime, timezone
from config import WINDOW_DURATION

class WindowProcessor:
    def __init__(self):
        self.current_window_data = []
        self.window_lock = threading.Lock()
        self.is_running = True

    def collect_message_data(self, msg_body):
        try:
            data = json.loads(msg_body)
            temp = float(data.get("temperature") or data.get("value") or 0)
            
            timestamp_str = data.get("timestamp") 
            
            with self.window_lock:
                self.current_window_data.append((temp, timestamp_str))
        except Exception as e:
            print(f"❌ Greška pri analizi poruke: {e}")

    def process_tumbling_window(self):
        while self.is_running:
            time.sleep(WINDOW_DURATION)
            
            with self.window_lock:
                data_to_process = self.current_window_data.copy()
                self.current_window_data.clear()
            
            if len(data_to_process) > 0:
                avg_temp = sum(item[0] for item in data_to_process) / len(data_to_process)
                print(f"⏱️ [Prozor {WINDOW_DURATION}s] Obrađeno poruka: {len(data_to_process)} | Prosečna temp: {avg_temp:.2f}°C")
                
                if avg_temp > 50.0:
                    poslednji_timestamp_str = data_to_process[-1][1]
                    
                    if poslednji_timestamp_str:
                        t0 = datetime.fromisoformat(poslednji_timestamp_str.replace('Z', '+00:00'))
                    else:
                        t0 = datetime.now(timezone.utc)
                    
                    t1 = datetime.now(timezone.utc)
                    
                    latency_ms = (t1 - t0).total_seconds() * 1000
                    
                    print(f"🚨 🔥 [CRITICAL ALERT] Detektovana anomalija! Prosečna temperatura prozora iznosi {avg_temp:.2f}°C")
                    # Ovde štampamo 'latency_ms', a NE 'time.time()'
                    print(f"⚡ [Scenario D - END] End-to-End Latencija: {latency_ms:.2f} ms")
            else:
                print(f"⏱️ [Prozor {WINDOW_DURATION}s] Nema pristiglih podataka u ovom vremenskom prozoru.")

    def stop(self):
        self.is_running = False


window_processor = WindowProcessor()