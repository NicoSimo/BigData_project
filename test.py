import pandas as pd
import threading
import schedule
import time
import os
import pandas as pd
import numpy as np
import datetime


df = pd.read_csv('/Users/nicolosimonato/Desktop/UNITRENTO/BigDataTech/Data/New_data/Sensors/new_consumptions.csv')

class Sensor:
    def __init__(self, building_id, topic, data):
        self.building_id = building_id
        self.topic = topic
        self.data = data

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
            print(self.topic, message)

##########################################################################################################################################

def run_sensor_scheduler(df):
    i = 0

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by='timestamp', inplace=True)
    data_read = df.copy()

    INITIAL_TIMESTAMP = data_read['timestamp'].iloc[0]  # Use .iloc[0] to get the first timestamp
    LAST_TIMESTAMP = data_read['timestamp'].iloc[-1]  # Last timestamp

    '''
    # TESTING WITH TIMESTAMPS
    test_H = data_read['timestamp'][0]
    test_H1 = data_read['timestamp'][0] + datetime.timedelta(minutes=60)
    test_H2 = data_read['timestamp'][0] + datetime.timedelta(minutes=120)

    print(data_read)

    print('###################')
    data_read =  df.loc[df['timestamp'] == test_H]

    print(data_read)
    print('###################')

    data_read =  df.loc[df['timestamp'] == test_H1]
    print(data_read)

    print('###################')

    data_read =  df.loc[df['timestamp'] == test_H2]
    print(data_read)

    print('###################')

    '''

    # Create sensor instances
    sensors = [Sensor(building_id, 'energy_consumption_topic', data_read[data_read['building_id'] == building_id]) for building_id in data_read['building_id'].unique()]
    
    '''
    for sensor in sensors:
        print(sensor.topic)
    print('###################')

    for sensor in sensors:
        print(sensor.topic, sensor.building_id, '\n', sensor.topic, '\n', sensor.data, '\n')

    print('###################')
    '''
    
    def trigger_sensors():
        """ 
        Trigger all sensors to send data simultaneously. 
        """
        threads = []
        for sensor in sensors:
            thread = threading.Thread(target=sensor.send_data)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    update_time = int(os.getenv('UPDATE_TIME', 15))


# PROBLEM IS HERE. IT PROCESSES THE WHOLE DF AND THEN IT SENDS THE DATA.
# THE SLEEPS AND SENDS ALL THE DATA AGAIN.

    # Run the scheduler
    while True:
        timestamp = INITIAL_TIMESTAMP + datetime.timedelta(minutes=60*i)
        i += 1
        
        data_read_current = data_read.loc[data_read['timestamp'] == timestamp]

        for sensor in sensors:
            sensor.data = data_read_current[data_read_current['building_id'] == sensor.building_id]
        
        if timestamp > LAST_TIMESTAMP: # if timestamp is greater than the last timestamp, break the loop
            break

        trigger_sensors()
        
        time.sleep(update_time)

if __name__ == '__main__':
    time.sleep(5) # Wait time before generating data
    run_sensor_scheduler(df)