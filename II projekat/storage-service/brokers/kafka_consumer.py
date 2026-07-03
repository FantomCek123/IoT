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
    
    while consumer is None:
        try:
            logger.info("⏳ Pokušavam povezivanje na Kafku...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=['kafka:9092'],
                group_id='storage-group-v2',
                auto_offset_reset='earliest',
                value_deserializer=lambda x: x.decode('utf-8'),
                consumer_timeout_ms=1000  
            )
            logger.info("✔ Uspešno povezan na Kafku! Potrošač je pokrenut u pozadini.")
        except NoBrokersAvailable:
            logger.warning("❌ Kafka broker još nije spreman, ponovni pokušaj za 3 sekunde...")
            time.sleep(3)
        except Exception as e:
            logger.error(f"❌ Neočekivana greška pri inicijalizaciji consumera: {e}")
            time.sleep(3)

    try:
        while True:
            try:
                for msg in consumer:
                    if msg.value is None:
                        logger.warning("⚠️ Primljena je prazna poruka (tombstone) sa Kafke.")
                        continue
                    
                    # Logujemo tačne metapodatke da vidiš ŠTA se dešava, sa koje particije i ofseta
                    logger.info(f"📥 [Poruka primljena] Topic: {msg.topic} | Particija: {msg.partition} | Offset: {msg.offset}")
                    logger.info(f"📄 Sadržaj: {msg.value[:100]}...") # Printamo prvih 100 karaktera radi preglednosti
                    
                    # Prosleđujemo procesoru
                    processor.handle_incoming_message(msg.value)
                    
            except Exception as inner_error:
                # Ako pukne parsiranje JEDNE poruke, hvatamo grešku OVDE.
                # Petlja se NE prekida, idemo na sledeću poruku!
                logger.error(f"❌ Greška prilikom obrade pojedinačne poruke: {inner_error}")
            
            # Kratka pauza (cool-down) pre nego što ponovo pitamo Kafku za poruke
            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("🛑 Ručno zaustavljanje potrošača (Ctrl+C)...")
    finally:
        # 3. SIGURNO ZATVARANJE RESURSA (Garantuje da se konekcija očisti na gašenju)
        logger.info("🔌 Zatvaram Kafka consumer konekciju...")
        if consumer:
            consumer.close()
        logger.info("✔ Konekcija uspešno zatvorena.")