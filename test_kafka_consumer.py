from kafka import KafkaConsumer
import os
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

def test_kafka_consumer():
    topic_name = 'energy_consumption_topic'
    kafka_brokers = os.getenv('KAFKA_BROKER', 'kafka1:9092,kafka2:9093,kafka3:9094,kafka4:9095').split(',')

    while True:
        try:
            log.debug(f"Attempting to connect to Kafka brokers: {kafka_brokers}")
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=kafka_brokers,
                auto_offset_reset='earliest', 
                group_id='test_group'
            )
            log.debug("Kafka consumer initialized")
            break

        except Exception as e:
            log.error(f"Unexpected error during KafkaConsumer initialization: {e}")
            time.sleep(5)

    log.debug("Starting to consume messages from Kafka topic")
    for message in consumer:
        log.info(f"Received message: {message.value.decode('utf-8')}")

if __name__ == '__main__':
    test_kafka_consumer()
