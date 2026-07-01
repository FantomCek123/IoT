import os

BROKER_TYPE = os.getenv("BROKER_TYPE", "kafka")
DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_NAME = "iot_p2_db"
DB_USER = "vukasin"
DB_PASS = "iotpassword"
TOPIC_NAME = "iot_sensor_data"

BATCH_SIZE = 500