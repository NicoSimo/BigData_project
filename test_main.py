import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

import pandas as pd
import datetime 
from pathlib import Path
import sys
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import socket
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

from Sensor_core.sensor_object import Sensor
import Energy_consumption_sensors.test_KAFKA as Producer
import Consumers.test_consumer_kafka as Consumer

def main():
    # Create 'my-topic' Kafka topic
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

        topic = NewTopic(name='my-topic',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass

    tasks = [
        Producer(),
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()