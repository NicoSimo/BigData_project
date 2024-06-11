from kafka import KafkaProducer
import numpy as np
import json

def json_serializable(val):
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    elif isinstance(val, np.ndarray):
        return val.tolist()
    else:
        return val

class Sensor:
    def __init__(self, building_id, energy_consumption_topic, data_source, bootstrap_servers='localhost:9094'):
        self.building_id = building_id
        self.topic = energy_consumption_topic
        self.data_source = data_source
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda x: json.dumps({k: json_serializable(v) for k, v in x.items()}).encode('utf-8')
        )
        self.current_index = 0

    def send_data(self):
        """ Send data to Kafka topic from the data source at the current index. """
        if self.current_index < len(self.data_source):
            row = self.data_source.iloc[self.current_index]
            data = {
                'building_id': self.building_id,
                'timestamp': str(row['timestamp']),
                'meter_reading': row['meter_reading']
            }
            self.producer.send(self.topic, value=data)
            self.producer.flush()

            print(f"Data sent from Building ID {self.building_id}: {data}")
            
            self.current_index += 1

        else:
            print(f"No more data to send from Building ID {self.building_id}")