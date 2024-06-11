import pandas as pd
import threading
import schedule
import time
from pathlib import Path
import sys

# Get the path of the current file and resolve the project root directory to the sys.path
current_file_path = Path(__file__)
project_root = current_file_path.parent.parent.resolve()
sys.path.append(str(project_root))

from Sensor_core.sensor import Sensor   

##########################################################################################################################################

# Load and prepare the data
df = pd.read_csv('/Users/nicolosimonato/Desktop/UNITRENTO/BigDataTech/Data/New_data/Sensors/new_consumptions.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.sort_values(by='timestamp', inplace=True)

# Create sensor instances
sensors = [Sensor(building_id, 'energy_consumption_topic', df[df['building_id'] == building_id]) for building_id in df['building_id'].unique()]

# This function alone updates data only ONCE. To increase the frequency of update set by the scheduler.every() function.
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

# Schedule the job every 5 seconds
schedule.every(5).seconds.do(trigger_sensors)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(10)