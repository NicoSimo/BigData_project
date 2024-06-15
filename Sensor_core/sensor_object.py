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

        # Logging the producer initialization details
        log.info(f"Initializing KafkaProducer for building_id: {building_id}")
        log.info(f"Bootstrap servers: {producer.config['bootstrap_servers']}")

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=producer.config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8')
        )

    def json_serializer(self, obj):
        """
        JSON serializer for objects not serializable by default.
        """
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def send_data(self):
        for _, row in self.data.iterrows():
            message = {
                'building_id': int(self.building_id),  # Ensure building_id is an int
                'timestamp': row['timestamp'].isoformat(),  # Ensure timestamp is ISO formatted
                'meter_reading': float(row['meter_reading'])  # Ensure meter_reading is a float
            }
            try:
                self.producer.send(self.topic, value=message)
                self.producer.flush()
                log.info(f"Sent message to broker {self.producer.config['bootstrap_servers']} for building_id {self.building_id}: {message}")
            except Exception as e:
                log.error(f"Failed to send message for building_id {self.building_id}: {e}")
