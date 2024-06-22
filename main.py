import threading
import Consumers.redis_consumer as redis_consumer
import Consumers.postgre_consumer as postgre_consumer
import Energy_consumption_sensors.sensor_scheduler as sensor_scheduler
import Predictor.prediction_processing as prediction_processing
import Setup.buildings_info

def run_all():
    #Setup
    Setup.buildings_info.buildings_loader()  #Upload building information to redis

    # Start Kafka to Redis consumer
    kafka_redis_thread = threading.Thread(target=redis_consumer.run_redis_consumer)
    kafka_redis_thread.start()

    # Start Kafka to PostgreSQL consumer
    kafka_postgre_thread = threading.Thread(target=postgre_consumer.run_postgre_consumer)
    kafka_postgre_thread.start()
    #Start Redis to Kafka consumer
    processor_thread = threading.Thread(target=prediction_processing.process)
    processor_thread.start()

    # Start sensor scheduler
    sensor_thread = threading.Thread(target=sensor_scheduler.run_sensor_scheduler)
    sensor_thread.start()

    # Join threads to wait for their completion
    sensor_thread.join()
    kafka_redis_thread.join()
    kafka_postgre_thread.join()
    processor_thread.join()

if __name__ == '__main__':
    run_all()
