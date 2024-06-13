import threading
import Consumers.kafka_redis_consumer as kafka_redis_consumer
import Energy_consumption_sensors.sensor_scheduler as sensor_scheduler

def run_all():
    # Start sensor scheduler
    sensor_thread = threading.Thread(target=sensor_scheduler.run_sensor_scheduler)
    sensor_thread.start()

    # Start Kafka to Redis consumer
    kafka_redis_thread = threading.Thread(target=kafka_redis_consumer.run_kafka_redis_consumer)
    kafka_redis_thread.start()

    # Join threads to wait for their completion
    sensor_thread.join()
    kafka_redis_thread.join()

if __name__ == '__main__':
    run_all()
