from kafka import KafkaProducer
import json
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def initialize_kafka_producer(broker):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info(f"KafkaProducer created successfully for broker {broker}.")
            return producer
        except Exception as e:
            log.error(f"Error initializing KafkaProducer for broker {broker}: {e}")
            time.sleep(5)  # Wait before retrying

producers = {}
kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')

# Initialize Kafka producers for each broker
for broker in kafka_brokers:
    producers[broker] = initialize_kafka_producer(broker)
