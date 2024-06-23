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
    postgre_user = os.getenv('POSTGRES_USER', 'postgres')
    postgre_password = os.getenv('POSTGRES_PASSWORD', 'Team3')
    postgre_db = os.getenv('POSTGRES_DB', 'energy_consumption')

    #define station codes:
    area_to_station_code = {
    1: "T0409",  # Trento
    2: "T0147",  # Rovereto
    3: "T0367",  # Cavalese
    4: "T0179"   # Tione
    }


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
    next_weather_update = datetime.now() + timedelta(seconds=60)  #The weather database is supposed to be updated every 24 hours

    while True:
        try:
            raw_messages = consumer.poll(timeout_ms=update_time*1000)  # 60 seconds timeout for demo purposes
            if not raw_messages:
                log.debug("No messages received in this poll")

            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    log.info(f"Received message: {message.value.decode('utf-8')}")
                    data = json.loads(message.value.decode('utf-8'))
                    log.debug(f"Decoded message: {data}")
                    batch.append(data)

            # Check if the batch size is reached 
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
                weather_data_to_insert = []
                for i in range(1, 5):
                    try:
                        # Get weather data
                        temperature_data, wind_speed_data, precipitation_data = wm.get_past_measurements(area_to_station_code[i])
                        
                        # Ensure all data is valid
                        if temperature_data is None or wind_speed_data is None or precipitation_data is None:
                            log.error(f"Received None data for station {area_to_station_code[i]}")
                            continue

                        # Process each timestamp-value pair
                        site_id = i
                        for temp_item, wind_item, precip_item in zip(temperature_data, wind_speed_data, precipitation_data):
                            if len(temp_item) != 2 or len(wind_item) != 2 or len(precip_item) != 2:
                                log.warning(f"Invalid data format for station {area_to_station_code[i]}")
                                continue

                            timestamp = temp_item[0]
                            air_temperature = temp_item[1]
                            wind_speed = wind_item[1]
                            precip_depth_1_hr = precip_item[1]

                            # Append data to batch insert list
                            weather_data_to_insert.append((site_id, timestamp, air_temperature, wind_speed, precip_depth_1_hr))

                        log.info(f"Processed weather data for station {area_to_station_code[i]}")
                    except Exception as e:
                        log.error(f"Failed to get or process weather data for station {area_to_station_code[i]}: {e}")
                
                # Batch insert data into the weather_measurements table
                if weather_data_to_insert:
                    try:
                        weather_insert_query = """
                        INSERT INTO weather_data (site_id, timestamp, air_temperature, wind_speed, precip_depth_1_hr)
                        VALUES (%s, %s, %s, %s, %s);
                        """
                        psycopg2.extras.execute_batch(cur, weather_insert_query, weather_data_to_insert)
                        conn.commit()
                        log.info("Weather data inserted successfully")
                    except Exception as e:
                        conn.rollback()  # Rollback in case of error
                        log.error(f"Failed to insert weather data into PostgreSQL: {e}")

                        # Update the next weather update time
                        next_weather_update = datetime.now() + timedelta(seconds=60)
                    except Exception as e:
                        log.error(f"Failed to insert weather data into PostgreSQL: {e}")


        except Exception as e:
            log.error(f"Error during message consumption or processing: {e}")

if __name__ == '__main__':
    run_postgre_consumer()
