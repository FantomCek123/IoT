import time
from kafka import KafkaConsumer, errors
from config import TOPIC_NAME
from services.window_processor import window_processor

def start_kafka():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='analytics-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("✅ [Kafka Analytics] Uspešno povezan na Kafku!")
        except errors.NoBrokersAvailable:
            print("⏳ [Kafka Analytics] Kafka još nije spremna, pokušavam ponovo za 3s...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ [Kafka Analytics] Neočekivana greška: {e}")
            time.sleep(3)

    for msg in consumer:
        window_processor.collect_message_data(msg.value)