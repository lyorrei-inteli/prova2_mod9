import asyncio
import json
import random
import time
import uuid

from confluent_kafka import Producer


def produce_sensor_message(config, topic, message):
    # creates a new producer instance
    producer = Producer(config)

    producer.produce(topic, key=message["idSensor"], value=json.dumps(message))
    print("----------------------------------------------------")
    print(f'Produced the following message to topic "{topic}":')
    print(f"key = {message['idSensor']} \nvalue = {message}")
    print("----------------------------------------------------")
    producer.flush()
