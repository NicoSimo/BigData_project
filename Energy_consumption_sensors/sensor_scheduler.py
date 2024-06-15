import pandas as pd
import threading
import datetime
import time
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

# Get the path of the current file and resolve the project root directory to the sys.path
current_file_path = Path(__file__)
project_root = current_file_path.parent.parent.resolve()
sys.path.append(str(project_root))

from Sensor_core.sensor_object import Sensor

df_measurements = pd.read_csv('/app/new_data/Sensors/new_consumptions.csv')
df_sensors = pd.read_csv('/app/new_data/building_data.csv')

def check_kafka_connectivity(kafka_brokers):
    for broker in kafka_brokers:
        log.info(f"Checking Kafka broker: {broker}")
        if not broker:
            continue
        try:
            kafka_host, kafka_port = broker.split(':')
            with socket.create_connection((kafka_host, int(kafka_port)), timeout=5) as sock:
                log.info(f"Successfully connected to Kafka at {kafka_host}:{kafka_port}")
                return True
        except socket.error as err:
            log.warning(f"Failed to connect to Kafka at {kafka_host}:{kafka_port} - {err}")
        except ValueError as e:
            log.error(f"Error parsing broker '{broker}': {e}")
    return False

def assign_broker_to_site(site_id, site_to_broker):
    if site_id not in site_to_broker:
        broker_index = len(site_to_broker) % len(kafka_brokers)
        site_to_broker[site_id] = kafka_brokers[broker_index]
    return site_to_broker[site_id]

def run_sensor_scheduler():
    
    i = 0

    df_measurements['timestamp'] = pd.to_datetime(df_measurements['timestamp'])
    df_measurements.sort_values(by='timestamp', inplace=True)
    data_read = df_measurements.copy()

    INITIAL_TIMESTAMP = data_read['timestamp'].iloc[0]  # Use .iloc[0] to get the first timestamp
    LAST_TIMESTAMP = data_read['timestamp'].iloc[-1]  # Last timestamp

    global kafka_brokers
    kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')
    log.info(f"KAFKA_BROKER environment variable: {kafka_brokers}")

    while not check_kafka_connectivity(kafka_brokers):
        log.warning("Kafka brokers not available, retrying in 5 seconds...")
        time.sleep(5)

    producers = {}
    for broker in kafka_brokers:
        try:
            producers[broker] = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log.info(f"KafkaProducer created successfully for broker {broker}.")
        except Exception as e:
            log.error(f"Error initializing KafkaProducer for broker {broker}: {e}")
            continue  

    # Map site_id to brokers
    site_to_broker = {}
    sensors = []
    for _, sensor in df_sensors.iterrows():
        broker = assign_broker_to_site(sensor['site_id'], site_to_broker)
        producer = producers[broker]
        building_id = sensor['building_id']
        sensor_data = df_measurements[df_measurements['building_id'] == building_id]
        sensors.append(Sensor(building_id, 'energy_consumption_topic', sensor_data, producer))

    # This function alone updates data only ONCE. To increase the frequency of update set by the scheduler.every() function.
    def trigger_sensors():
        """ 
        Trigger all sensors to send data simultaneously. 
        """
        threads = []
        for sensor in sensors:
            thread = threading.Thread(target=sensor.send_data)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    update_time = int(os.getenv('UPDATE_TIME', 30))

    # Run the scheduler
    while True:
        timestamp = INITIAL_TIMESTAMP + datetime.timedelta(minutes=60*i)
        i += 1
        
        data_read_current = data_read.loc[data_read['timestamp'] == timestamp]

        for sensor in sensors:
            sensor.data = data_read_current[data_read_current['building_id'] == sensor.building_id]
        
        if timestamp > LAST_TIMESTAMP:  # if timestamp is greater than the last timestamp, break the loop
            break

        trigger_sensors()
        
        time.sleep(update_time)

if __name__ == '__main__':
    time.sleep(5)  # Wait time before generating data
    run_sensor_scheduler()
