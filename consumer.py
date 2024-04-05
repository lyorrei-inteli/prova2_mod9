from confluent_kafka import Consumer
import pandas as pd
import json

def store_data(data):
    df = pd.DataFrame(data, index=[data['idSensor']])

    # Write the DataFrame to a CSV file
    df.to_csv('sensor_data.csv', mode='a', header=False)

def consume_sensor_messages(config, topic):
    # sets the consumer group ID and offset
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer and subscribes to your topic
    consumer = Consumer(config)
    consumer.subscribe([topic])
    print("Starting consumer")
    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print("----------------------------------------------------")
                print(f'Consumed message from topic "{topic}": ')
                print(f"key = {key:12} \nvalue = {value:12}")
                print("----------------------------------------------------")

                store_data(json.loads(value))

    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()
