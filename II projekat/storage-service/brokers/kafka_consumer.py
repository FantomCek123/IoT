import time
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from config import TOPIC_NAME
from services.batch_processor import processor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KafkaConsumer")

def start_kafka():
    consumer = None
    
    while not consumer:
        try:
            logger.info("Pokušavam povezivanje na Kafku...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='storage-group-v2',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            logger.info("Povezan na Kafku!")
        except Exception:
            logger.warning("Kafka nije spremna, čekam 3s...")
            time.sleep(3)

    try:
        for msg in consumer:
            if not msg.value: continue
            logger.info(f"Primljeno | Offset: {msg.offset} | Uređaj: {msg.value[:20]}...")
            processor.handle_incoming_message(msg.value)
            
    except Exception as e:
        logger.error(f"Greška u petlji: {e}")
    finally:
        consumer.close()
        logger.info("Konekcija zatvorena.")