import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from config import TOPIC_NAME
from services.batch_processor import processor

def start_kafka():
    consumer = None
    while consumer is None:
        try:
            print("⏳ [Kafka Consumer] Pokušavam povezivanje na Kafku...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='storage-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            print("✔ [Kafka Consumer] Uspešno povezan na Kafku! Potrošač je pokrenut.")
        except NoBrokersAvailable:
            print("❌ [Kafka Consumer] Kafka broker još nije spreman, ponovni pokušaj za 3s...")
            time.sleep(3)

    for msg in consumer:
        processor.handle_incoming_message(msg.value)