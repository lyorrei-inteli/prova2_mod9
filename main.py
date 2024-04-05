from confluent_kafka import Producer, Consumer

from config import read_config
from consumer import consume_sensor_messages
from producer import produce_sensor_message
import random
import time
import uuid

sensor_type = ["PM2.5", "PM10", "CO2", "CO", "NO2", "O3", "SO2"]

def generate_sensor_message():
    payload = {
        "idSensor": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tipoPoluente": sensor_type[random.randint(0, 6)],
        "nivel": round(random.uniform(0, 100), 1),
    }
    return payload

def main():
    config = read_config()
    topic = "sensor"
    num_messages = 3

    for _ in range(num_messages):
        produce_sensor_message(config, topic, message=generate_sensor_message())

    consume_sensor_messages(config, topic)
    

if __name__ == "__main__":
    main()
