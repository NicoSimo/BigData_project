from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import json
import psycopg2
import os
import logging

logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG
log = logging.getLogger(__name__)

def run_postgre_consumer():
    # Kafka setup
    topic_name = 'energy_consumption_topic'
    kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')

    # PostgreSQL setup
    postgre_host = os.getenv('POSTGRES_HOST', 'postgres')
    postgre_user = os.getenv('POSTGRES_USER', 'postgres')
    postgre_password = os.getenv('POSTGRES_PASSWORD', 'Team3')
    postgre_password = os.getenv('POSTGRES_PASSWORD', 'Team3')
    postgre_db = 'testdb'

    # Retry mechanism to wait for Kafka broker
    while True:
        try:
            log.debug(f"Attempting to connect to Kafka brokers: {kafka_brokers}")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=kafka_brokers,
                auto_offset_reset='earliest', 
                group_id='energy_consumption',
                enable_auto_commit=False  # Disable auto commit to manage offsets manually
            )
            log.debug("Kafka consumer initialized")
            consumer.subscribe([topic_name])
            log.debug(f"Subscribed to Kafka topic: {topic_name}")
            break

        except NoBrokersAvailable:
            log.warning("Kafka broker not available, retrying in 5 seconds...")
            time.sleep(5)

        except Exception as e:
            log.error(f"Unexpected error during KafkaConsumer initialization: {e}")
            time.sleep(5)

    # PostgreSQL connection setup
    try:
        log.debug("Connecting to PostgreSQL database")
        conn = psycopg2.connect(
            host=postgre_host, 
            dbname=postgre_db, 
            user=postgre_user, 
            password=postgre_password
        )
        cur = conn.cursor()
        log.debug("Connected to PostgreSQL database")
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        return

    # Consume messages 
    log.debug("Starting to consume messages from Kafka topic")
    for message in consumer:
        log.info(f"Received message: {message.value.decode('utf-8')}")
        data = json.loads(message.value.decode('utf-8'))
        try:
            log.debug(f"Inserting data into PostgreSQL: {data}")
            cur.execute(
                "INSERT INTO sensor_data (building_id, timestamp, meter_reading) VALUES (%s, %s, %s)",
                (data['building_id'], data['timestamp'], data['meter_reading'])
            )
            conn.commit()
            log.debug("Data inserted successfully")
            
            # Commit the offset after successful insertion
            consumer.commit()
            log.debug("Offset committed successfully")
        except Exception as e:
            conn.rollback()  # Rollback in case of error
            log.error(f"Failed to insert data into PostgreSQL: {e}")

if __name__ == '__main__':
    run_postgre_consumer()
