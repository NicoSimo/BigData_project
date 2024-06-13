import pandas as pd
import threading
import datetime
import time
from pathlib import Path
import sys
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket

# Get the path of the current file and resolve the project root directory to the sys.path
current_file_path = Path(__file__)
project_root = current_file_path.parent.parent.resolve()
sys.path.append(str(project_root))

from Sensor_core.sensor_object import Sensor

df = pd.read_csv('/app/new_data/Sensors/new_consumptions.csv')

def check_kafka_connectivity(kafka_host, kafka_port):
    try:
        with socket.create_connection((kafka_host, kafka_port), timeout=5) as sock:
            print(f"Successfully connected to Kafka at {kafka_host}:{kafka_port}")
            return True
    except socket.error as err:
        print(f"Failed to connect to Kafka at {kafka_host}:{kafka_port} - {err}")
        return False

def run_sensor_scheduler():
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by='timestamp', inplace=True)
    data_read = df.copy()

    INITIAL_TIMESTAMP = data_read['timestamp'].iloc[0]  # Use .iloc[0] to get the first timestamp
    LAST_TIMESTAMP = data_read['timestamp'].iloc[-1]  # Last timestamp

    # Retry mechanism to wait for Kafka broker
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    #print(f"KAFKA_BROKER environment variable: {kafka_broker}")
    
    kafka_host, kafka_port = kafka_broker.split(':')
    kafka_port = int(kafka_port)
    #print(f"Connecting to Kafka broker at: {kafka_broker}")

    while not check_kafka_connectivity(kafka_host, kafka_port):
        print("Kafka broker not available, retrying in 5 seconds...")
        time.sleep(5)

    while True:
        try:
            # Ensuring the bootstrap_servers is correctly formatted
            bootstrap_servers = [kafka_broker]
            print(f"Bootstrap servers: {bootstrap_servers}, Type: {type(bootstrap_servers)}")
            
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            print("KafkaProducer created successfully.")
            break
        except NoBrokersAvailable:
            print("Kafka broker not available, retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error during KafkaProducer initialization: {e}")
            break

    # Create sensor instances
    sensors = [Sensor(building_id, 'energy_consumption_topic', df[df['building_id'] == building_id], producer) for building_id in df['building_id'].unique()]

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
        
        if timestamp > LAST_TIMESTAMP: # if timestamp is greater than the last timestamp, break the loop
            break

        trigger_sensors()
        
        time.sleep(update_time)

if __name__ == '__main__':
    time.sleep(5) # Wait time before generating data
    run_sensor_scheduler(df)
