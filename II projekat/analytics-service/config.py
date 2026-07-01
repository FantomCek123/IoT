import os

BROKER_TYPE = os.getenv("BROKER_TYPE", "kafka")
TOPIC_NAME = "iot_sensor_data"
WINDOW_DURATION = 10