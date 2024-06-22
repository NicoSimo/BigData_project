import threading
import Consumers.redis_consumer as redis_consumer
import Consumers.postgre_consumer as postgre_consumer
import Energy_consumption_sensors.sensor_scheduler as sensor_scheduler
import Predictor.prediction_processing as prediction_processing
import Training_scripts.rf_training as rf_training  
import Setup.buildings_info

def run_all():
    Setup.buildings_info.buildings_loader()  # Load building information

    # Set up various threads for different components
    kafka_redis_thread = threading.Thread(target=redis_consumer.run_redis_consumer)
    kafka_postgre_thread = threading.Thread(target=postgre_consumer.run_postgre_consumer)
    processor_thread = threading.Thread(target=prediction_processing.process)
    sensor_thread = threading.Thread(target=sensor_scheduler.run_sensor_scheduler)
    training_thread = threading.Thread(target=rf_training.run_training)

    # Start all threads
    kafka_redis_thread.start()
    kafka_postgre_thread.start()
    processor_thread.start()
    sensor_thread.start()
    training_thread.start()

    # Now join all threads
    kafka_redis_thread.join()
    kafka_postgre_thread.join()
    processor_thread.join()
    sensor_thread.join()
    training_thread.join()

if __name__ == '__main__':
    run_all()

