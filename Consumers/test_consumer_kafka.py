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

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()
