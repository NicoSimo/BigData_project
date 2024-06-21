import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import logging
import json
import time
import weather_measurements as wm
import numpy as np
import skops.io

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
topic_name = 'energy_consumption_predictions'

# Initialize Redis and Kafka producers
r = redis.Redis(host=redis_host, port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe('data_updates')

# Load ML model
model = skops.io.load("rf.skops")

producers = {}

def RFModel(input):

    # PREPROCESSING DEI DATI PERCHE' NON SO COME ARRIVINO
    """INPUT: Dask Dataframe con colonne:
    ora del giorno, meter_reading, square_feet, year_built, air_temperature,
    precip_depth_1_hr, meter_reading di due ore prima, meter_reading di un'ora prima

    OUTPUT: numpy array di dimensione (inputs_row, 1)
    """

    pred = model.predict(input)
    return pred

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

# Configure the weather station codes to get the weather predictions
station_code1 = "T0409" #Trento
station_code2 = "T0147" #Rovereto
station_code3 = "T0392" #Borgo Valsugana
station_code4 = "T0179" #Tione

def send_to_kafka(message):
    try:
        for broker, producer in producers.items():
            producer.send(topic_name, message)
            producer.flush()
            log.info(f"Sent to Kafka topic {topic_name}: {message}")
    except Exception as e:
        log.error(f"Failed to send message to Kafka: {e}")

def process():
    for message in pubsub.listen():
        if message['type'] == 'message':
            key = message['data'].decode('utf-8')
            # Retrieve the latest data for the key
            latest_data = r.lrange(key, 0, 0)  # Get the most recent entry

            # Retrieve all weather measurements data via the Provincia API
            area1_pred = wm.get_latest_measurements(station_code1)
            area2_pred = wm.get_latest_measurements(station_code2)
            area3_pred = wm.get_latest_measurements(station_code3)
            area4_pred = wm.get_latest_measurements(station_code4)

            #QUA CI VA LA ROBA CHE EFFETTIVAMENTE FA LE PREVISIONI
            prevision = RFModel(...)

            if latest_data:
                data = latest_data[0].decode('utf-8')
                log.info(f"Received message from Redis: {data}")
                send_to_kafka(data)

if __name__ == '__main__':
    process()
