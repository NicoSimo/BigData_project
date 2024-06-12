from dask.distributed import Client, as_completed
import dask
import time
import redis
import json
import pandas as pd

# Initialize Dask client
client = Client()

def fetch_data_from_redis():
    '''
    Function to fetch data from Redis
    '''
    r = redis.Redis(host='localhost', port=6379, db=0)
    keys = r.keys('building:*')
    data = []
    for key in keys:
        entries = r.lrange(key, 0, -1)
        data.extend([json.loads(entry.decode('utf-8')) for entry in entries])
    return data

def process_data(data):
    '''
    Function to process the data before prediction (TO DO)
    '''
    # Convert data to DataFrame and perform any necessary processing
    df = pd.DataFrame(data)
    # You would add any specific processing here, such as feature extraction
    return df

def predict(model, df):
    '''
    Function to predict the energy consumption based on the data
    '''
    # Placeholder for model prediction ---> (TO DO) 
    return "Predicted results based on the model"

@dask.delayed
def update_workflow():
    # This function represents the workflow to update the model with new data
    data = fetch_data_from_redis()
    if data:
        df = process_data(data)
        result = predict(model, df)
        return result
    else:
        return "No data available"

if __name__ == "__main__":
    # Assuming model is already loaded
    model = None

    # Update rate should match the SENSORS update rate (at /Energy_consumption_sensors/main.py)
    update_rate = 30

    while True:
        future = update_workflow()
        result = future.compute()
        print(result)
        time.sleep(update_rate)  # Wait for the next cycle

    client.close()
