import sys
from pathlib import Path

# Adiciona o diretório pai ao sys.path para que possamos importar sensor_simulator
sys.path.append(str(Path(__file__).parent.parent))

from consumer import store_data
from main import generate_sensor_message
import pandas as pd
from config import read_config
from producer import produce_sensor_message
from confluent_kafka import Consumer
import json


def test_data_integrity():
    test_message = {
        "idSensor": "123",
        "timestamp": "2021-09-03T14:00:00Z",
        "tipoPoluente": "PM2.5",
        "nivel": 10.0,
    }

    config = read_config()
    topic = "sensor"
    produce_sensor_message(config, topic, message=test_message)

    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer and subscribes to your topic
    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                json_value = json.loads(value)

                if json_value == test_message:
                    assert True, "Data integrity test passed"
                    break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def test_producer_consumer_persistence():
    test_message = generate_sensor_message()

    config = read_config()
    topic = "sensor"
    produce_sensor_message(config, topic, message=test_message)

    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer and subscribes to your topic
    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                json_value = json.loads(value)

                store_data(json_value)
                break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    df = pd.read_csv("sensor_data.csv")
    df = df.drop('Unnamed: 0', axis=1)

    assert (
        df.iloc[-1].to_dict() == test_message
    ), "Producer-consumer integration test passed"
