import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import random
import os
import logging
import json
import time
import Predictor.weather_measurements as wm
import numpy as np
import skops.io
import dask.dataframe as dd
from datetime import datetime
import pandas as pd

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
area_topics = {1: 'trento', 2: 'rovereto', 3: 'cavalese', 4: 'tione'}

# Initialize Redis and Kafka producers
r = redis.Redis(host=redis_host, port=6379, db=0)
pubsub = r.pubsub()
pubsub.subscribe('data_updates')

# Load ML model
model = skops.io.load("Predictor/rf.skops")

producers = {}

def RFModel(input):
    """INPUT: Dask Dataframe con colonne:
    ora del giorno, meter_reading, square_feet, year_built, air_temperature, precip_depth_1_hr,
    meter_reading di due ore prima (met-2), meter_reading di un'ora prima (met-1)

    OUTPUT: numpy array di dimensione (inputs_row)
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
area_to_station_code = {
    1: "T0409",  # Trento
    2: "T0147",  # Rovereto
    3: "T0367",  # Cavalese
    4: "T0179"   # Tione
}


def send_to_kafka(building, prediction, area):
    try:
        # Select one random producer from the producers dictionary
        broker = random.choice(list(producers.keys()))
        producer = producers[broker]

        # Send the message to the Kafka topic
        producer.send(topic_name, {building: prediction})
        producer.flush()
        producer.send(area_topics[area], {building: prediction})
        producer.flush()
        
        log.info(f"Sent to Kafka topic {topic_name}: {building} - {prediction} via broker {broker}")
    except Exception as e:
        log.error(f"Failed to send message to Kafka: {e}")

def process():
    for message in pubsub.listen():
        if message['type'] == 'message':
            key = message['data'].decode('utf-8')
            # Retrieve the latest data for the key
            data = r.lrange(key, 0, 3)  # Get the most recent entry
            
            latest = json.loads(data[-1].decode('utf-8'))
            building = latest['building'] #Building ID
            timestamp = latest['timestamp'] #Timestamp
            dt_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            hour = dt_object.hour
            meter_reading = latest['meter_reading'] #Meter reading of the previous hour

            building_info = json.loads(r.get(building).decode('utf-8'))
            area = building_info['area'] 
            year = building_info['year_built']
            square_feet = building_info['square feet']

            # Retrieve all weather measurements data via the Provincia API
            area_weather = wm.get_latest_measurements(area_to_station_code[area])
            temp = area_weather['avg_temp']
            precipitations = area_weather['precipitations']

            past_reading1 = json.loads(data[-2].decode('utf-8'))['meter_reading']
            past_reading2 = json.loads(data[-3].decode('utf-8'))['meter_reading']
            
            # Create a list of dictionaries (your data)
            data_list = [{
                'timestamp': hour,
                'meter_reading': meter_reading,
                'square_feet': square_feet,
                'year_built': year,
                'air_temperature': temp,
                'precip_depth_1_hr': precipitations,
                'met-2': past_reading2,
                'met-1': past_reading1
            }]

            # Create Pandas DataFrame from list of dictionaries
            pandas_df = pd.DataFrame(data_list)

            # Create Dask DataFrame from Pandas DataFrame
            dask_df = dd.from_pandas(pandas_df, npartitions=1)

            prediction = RFModel(dask_df.compute())

            if prediction:
                log.info(f"Received building data from Redis: {key}")
                send_to_kafka(building, prediction, area)

if __name__ == '__main__':
    process()
