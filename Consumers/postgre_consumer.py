from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import json
import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def run_postgre_consumer():
    # Kafka setup
    topic_name = 'energy_consumption_topic'
    kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')

    # PostgreSQL setup
    postgre_host = os.getenv('POSTGRE_HOST', 'postgres')
    postgre_user = os.getenv('POSTGRES_USER', 'postgres')
    postgre_password = os.getenv('POSTGRES_PASSWORD', 'password')
    postgre_db = os.getenv('POSTGRES_DB', 'energy_consumption')

    # Retry mechanism to wait for Kafka broker
    while True:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=kafka_brokers,
                auto_offset_reset='earliest', 
                group_id='energy_consumption'
            )
            break

        except NoBrokersAvailable:
            log.warning("Kafka broker not available, retrying in 5 seconds...")
            time.sleep(5)

        except Exception as e:
            log.error(f"Unexpected error during KafkaConsumer initialization: {e}")
            time.sleep(5)

    # PostgreSQL connection setup
    conn = psycopg2.connect(
        host=postgre_host, 
        dbname=postgre_db, 
        user=postgre_user, 
        password=postgre_password
    )
    cur = conn.cursor()

    # Consume messages
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        # Example of writing data to PostgreSQL
        try:
            cur.execute(
                "INSERT INTO measurements (building_id, timestamp, meter_reading) VALUES (%s, %s, %s)",
                (data['building_id'], data['timestamp'], data['meter_reading'])
            )
            conn.commit()
        except Exception as e:
            conn.rollback()  # Rollback in case of error
            log.error(f"Failed to insert data into PostgreSQL: {e}")

if __name__ == '__main__':
    run_postgre_consumer()
        