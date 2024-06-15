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


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while not self.stop_event.is_set():
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()
