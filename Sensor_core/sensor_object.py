from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)

class Sensor:
    def __init__(self, building_id, topic, data, producer):
        self.building_id = building_id
        self.topic = topic
        self.data = data
        self.producer = producer  # Use the passed producer directly

        # Logging the producer initialization details
        log.info(f"Sensor initialized for building_id: {building_id} using topic: {topic}")

    def send_data(self):
        try:
            for _, row in self.data.iterrows():
                message = {
                    'building_id': int(self.building_id),
                    'timestamp': row['timestamp'].isoformat(),
                    'meter_reading': float(row['meter_reading'])
                }
            
                self.producer.send(self.topic, value=message)
                log.info(f"Sent message: {message}")
            self.producer.flush()    
        except Exception as e:
            log.error(f"Failed to send message for building_id {self.building_id}: {e}")