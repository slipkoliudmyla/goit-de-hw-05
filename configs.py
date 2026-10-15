import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092").split(",")
PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "slipkoliudmyla")
SENSORS_TOPIC = f"{PREFIX}_building_sensors"
TEMPERATURE_TOPIC = f"{PREFIX}_temperature_alerts"
HUMIDITY_TOPIC = f"{PREFIX}_humidity_alerts"
