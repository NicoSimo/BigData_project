import pandas as pd
import os

def load_data(path):
    """
    Load the data from the specified path
    """
    data = pd.read_csv(path)
    return data


def save_data(data, path):
    """
    Save the data to the specified path. Create the folder if it does not exist.
    """
    # Get the directory name from the path
    directory = os.path.dirname(path)
    
    # Create the directory if it does not exist
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    
    # Save the data to the specified path
    data.to_csv(path, index=False)

# Defining the paths for the data

# Define the native folder where the data is stored
native_dataset_folder = os.path.join(os.path.expanduser("~"), 'Desktop/dataset_energyconsumption')

# Define the base directory where the data will be saved
base_dir = os.path.join(os.path.expanduser("~"), 'Desktop/UNITRENTO/BigDataTech')

# Define relative paths for your data
train_path = os.path.join(native_dataset_folder,'train.csv')
building_path = os.path.join(native_dataset_folder,'building_metadata.csv')
weather_path = os.path.join(native_dataset_folder,'weather_train.csv')

# Save paths for consumption data
save_path_historical_consumptions = os.path.join(base_dir, 'Data/DB/Sensors/historical_consumptions.csv')
save_path_new_consumptions = os.path.join(base_dir, 'Data/New_data/Sensors/new_consumptions.csv')

# Save path for building data
save_path_building = os.path.join(base_dir, 'Data/DB/building_data.csv')

# Save path for weather data
save_path_historical_weather = os.path.join(base_dir, 'Data/DB/Weather/historical_weather.csv')
save_path_new_weather = os.path.join(base_dir, 'Data/New_data/Weather/new_weather.csv')

# load the data
historical_data = load_data(train_path)
building_data = load_data(building_path)
weather_data = load_data(weather_path)

# Select 4 unique site IDs with a fixed seed, those site IDs will be our MACRO-AREA
random_sites = building_data['site_id'].drop_duplicates().sample(n=4, random_state=42)
filtered_sites = building_data[building_data['site_id'].isin(random_sites)]

# Select 25 unique building IDs with a fixed seed
random_building_ids = filtered_sites['building_id'].sample(n=25, random_state=42)

# Filter weather data to only keep relevant informations
weather_data = weather_data[['site_id', 'timestamp', 'air_temperature', 'cloud_coverage', 'precip_depth_1_hr']]

# Filter data for the selected building IDs and meter 0 (electricity)
new_data = historical_data[(historical_data['building_id'].isin(random_building_ids)) & (historical_data['meter'] == 0)]
new_building_data = building_data[building_data['building_id'].isin(random_building_ids)]
new_weather_data = weather_data[weather_data['site_id'].isin(random_sites)]

# Convert timestamp columns to datetime
new_weather_data['timestamp'] = pd.to_datetime(new_weather_data['timestamp'])
new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])

# Define the split timestamp
split_timestamp = pd.Timestamp('2016-09-01 23:00:00')

# Split the weather data based on the timestamp
historical_weather = new_weather_data[new_weather_data['timestamp'] <= split_timestamp]
new_weather = new_weather_data[new_weather_data['timestamp'] > split_timestamp]

# Split the historical data based on the timestamp
historical_consumptions = new_data[new_data['timestamp'] <= split_timestamp]
new_consumptions = new_data[new_data['timestamp'] > split_timestamp]

# save the data in the new repositories
save_data(historical_weather, save_path_historical_weather)
save_data(new_weather, save_path_new_weather)

save_data(historical_consumptions, save_path_historical_consumptions)
save_data(new_consumptions, save_path_new_consumptions)

save_data(new_building_data, save_path_building)