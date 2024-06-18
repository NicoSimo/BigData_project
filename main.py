import threading
import Consumers.redis_consumer as redis_consumer
import Energy_consumption_sensors.sensor_scheduler as sensor_scheduler
import Predictor.prevision_processing as prevision_processing
import Predictor.weather_measurements as wm

def run_all():
    # Start Kafka to Redis consumer
    kafka_redis_thread = threading.Thread(target=redis_consumer.run_redis_consumer)
    kafka_redis_thread.start()

    #Start Redis to Kafka consumer
    processor_thread = threading.Thread(target=prevision_processing.process)
    processor_thread.start()

    # Start sensor scheduler
    sensor_thread = threading.Thread(target=sensor_scheduler.run_sensor_scheduler)
    sensor_thread.start()

    # Join threads to wait for their completion
    sensor_thread.join()
    kafka_redis_thread.join()
    processor_thread.join()

if __name__ == '__main__':
    run_all()
