import json
import threading
import time
from config import WINDOW_DURATION

class WindowProcessor:
    def __init__(self):
        self.current_window_temperatures = []
        self.window_lock = threading.Lock()
        self.is_running = True

    def collect_message_data(self, msg_body):
        try:
            data = json.loads(msg_body)
            temp = float(data.get("temperature") or data.get("value") or 0)
            
            with self.window_lock:
                self.current_window_temperatures.append(temp)
        except Exception as e:
            print(f"❌ Greška pri analizi poruke: {e}")

    def process_tumbling_window(self):
        while self.is_running:
            time.sleep(WINDOW_DURATION)
            
            with self.window_lock:
                temperatures_to_process = self.current_window_temperatures.copy()
                self.current_window_temperatures.clear()
            
            if len(temperatures_to_process) > 0:
                avg_temp = sum(temperatures_to_process) / len(temperatures_to_process)
                print(f"⏱️ [Prozor 10s] Obrađeno poruka: {len(temperatures_to_process)} | Prosečna temp: {avg_temp:.2f}°C")
                
                if avg_temp > 50.0:
                    trenutno_vreme_ms = int(time.time() * 1000)
                    print(f"🚨 🔥 [CRITICAL ALERT] Detektovana anomalija! Prosečna temperatura prozora iznosi {avg_temp:.2f}°C")
                    print(f"⏱️ [Scenario D - END] Alarm ispisan u logu u: {trenutno_vreme_ms} ms")
            else:
                print("⏱️ [Prozor 10s] Nema pristiglih podataka u ovom vremenskom prozoru.")

    def stop(self):
        self.is_running = False


window_processor = WindowProcessor()