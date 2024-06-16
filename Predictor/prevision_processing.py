import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import logging
import json
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Add file handler to log to file
log_file = '/app/app.log'
file_handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# Redis and Kafka configuration
redis_host = os.getenv('REDIS_HOST', 'redis')
kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')
topic_name = os.getenv('KAFKA_TOPIC', 'energy_consumption_predictions')

# Initialize Redis and Kafka producers
r = redis.Redis(host=redis_host, port=6379, db=0)
producers = {}

def initialize_kafka_producer(broker):
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=[broker],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            log.info(f"KafkaProducer created successfully for broker {broker}.")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka broker {broker} not available, retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            log.error(f"Unexpected error during KafkaProducer initialization for broker {broker}: {e}")
            return None

# Initialize Kafka producers for each broker
for broker in kafka_brokers:
    producers[broker] = initialize_kafka_producer(broker)

def subscribe_and_process():
    pubsub = r.pubsub()
    pubsub.subscribe('prova')
    log.info('Subscribed to Redis channel "prova".')

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data'].decode('utf-8')
            log.info(f"Received message from Redis: {data}")
            process_and_send_to_kafka(data)


def process_and_send_to_kafka(message):
    #### TODO: Manca qua da fare la previsione col modello già pronto
    try:
        for broker, producer in producers.items():
            producer.send(topic_name, message)
            producer.flush()
            log.info(f"Sent to Kafka topic {topic_name}: {message}")
    except Exception as e:
        log.error(f"Failed to send message to Kafka: {e}")

if __name__ == '__main__':
    subscribe_and_process()
