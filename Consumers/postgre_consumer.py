from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import json
import psycopg2
import psycopg2.extras
from psycopg2 import extras
import os
import logging
from datetime import datetime, timedelta
import Predictor.weather_measurements as wm

logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG
log = logging.getLogger(__name__)

def run_postgre_consumer():
    # Kafka setup
    topic_name = os.getenv('KAFKA_TOPIC_POSTGRE', 'energy_consumption_postgre')
    kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')

    # PostgreSQL setup
    postgre_host = os.getenv('POSTGRE_HOST', 'postgres')
    postgre_user = os.getenv('DATABASE_USER', 'postgres')
    postgre_password = os.getenv('DATABASE_PASSWORD', 'Team3')
    postgre_db = os.getenv('DATABASE_NAME', 'testdb')

    # Retry mechanism to wait for Kafka broker
    while True:
        try:
            log.debug(f"Attempting to connect to Kafka brokers: {kafka_brokers}")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=kafka_brokers,
                auto_offset_reset='earliest',
                group_id='energy_consumption_postgre',
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
    while True:
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
            break
        except Exception as e:
            log.error(f"Failed to connect to PostgreSQL: {e}")
            time.sleep(5)

    # Batch processing configuration
    batch_size = 25 * int(os.getenv('BATCH_SIZE',3))
    batch = []

    log.debug("Starting to consume messages from Kafka topic")

    update_time = int(os.getenv('UPDATE_TIME', 60)) #The update timne is set to 60 seconds for demo purposes
    next_weather_update = datetime.now() + timedelta(seconds=60)  #The weather database is updated every 24 hours

    while True:
        try:
            # Poll for new messages from Kafka with a longer timeout
            raw_messages = consumer.poll(timeout_ms=update_time*1000)  # 60 seconds timeout for demo purposes
            if not raw_messages:
                log.debug("No messages received in this poll")

            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    log.info(f"Received message: {message.value.decode('utf-8')}")
                    data = json.loads(message.value.decode('utf-8'))
                    log.debug(f"Decoded message: {data}")
                    batch.append(data)

            # Check if the batch size is reached or the time interval has elapsed
            if len(batch) >= batch_size:
                if batch:
                    try:
                        log.debug(f"Inserting data into PostgreSQL: {batch}")
                        insert_query = "INSERT INTO sensor_data (building_id, timestamp, meter_reading) VALUES %s"
                        psycopg2.extras.execute_values(
                            cur, insert_query,
                            [(d['building_id'], d['timestamp'], d['meter_reading']) for d in batch]
                        )
                        conn.commit()
                        log.debug("Data inserted successfully")

                        # Commit the offset after successful insertion
                        consumer.commit()
                        log.debug("Offset committed successfully")

                        # Clear the batch and reset the timer
                        batch.clear()
                    except Exception as e:
                        conn.rollback()  # Rollback in case of error
                        log.error(f"Failed to insert data into PostgreSQL: {e}")
            
            # Also update the weather_measurements table every 24 hours
                if datetime.now() >= next_weather_update:
                    temperature_data, wind_speed_data, precipitation_data = wm.get_past_measurements()
                    
                    # Insert temperature data
                    if temperature_data:
                        try:
                            print("Inserting temperature data into PostgreSQL")
                            insert_query = "INSERT INTO weather_measurements (timestamp, parameter, value) VALUES %s"
                            extras.execute_values(
                                cur, insert_query,
                                [(d['timestamp'], 'temperature', d['value']) for d in temperature_data]
                            )
                            conn.commit()
                            print("Temperature data inserted successfully")
                        except Exception as e:
                            conn.rollback()  # Rollback in case of error
                            print(f"Failed to insert temperature data into PostgreSQL: {e}")
                    
                    # Insert wind speed data
                    if wind_speed_data:
                        try:
                            print("Inserting wind speed data into PostgreSQL")
                            insert_query = "INSERT INTO weather_measurements (timestamp, parameter, value) VALUES %s"
                            extras.execute_values(
                                cur, insert_query,
                                [(d['timestamp'], 'wind_speed', d['value']) for d in wind_speed_data]
                            )
                            conn.commit()
                            print("Wind speed data inserted successfully")
                        except Exception as e:
                            conn.rollback()  # Rollback in case of error
                            print(f"Failed to insert wind speed data into PostgreSQL: {e}")
                    
                    # Insert precipitation data
                    if precipitation_data:
                        try:
                            print("Inserting precipitation data into PostgreSQL")
                            insert_query = "INSERT INTO weather_measurements (timestamp, parameter, value) VALUES %s"
                            extras.execute_values(
                                cur, insert_query,
                                [(d['timestamp'], 'precipitation', d['value']) for d in precipitation_data]
                            )
                            conn.commit()
                            print("Precipitation data inserted successfully")
                        except Exception as e:
                            conn.rollback()  # Rollback in case of error
                            print(f"Failed to insert precipitation data into PostgreSQL: {e}")

        except Exception as e:
            log.error(f"Error during message consumption or processing: {e}")

if __name__ == '__main__':
    run_postgre_consumer()
