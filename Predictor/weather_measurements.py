import requests
import xml.etree.ElementTree as ET
from statistics import mean
from datetime import datetime, timedelta

def get_latest_measurements(station_code, count=4):
    url = f"https://dati.meteotrentino.it/service.asmx/ultimiDatiStazione?codice={station_code}"
    
    # Make a GET request
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    root = ET.fromstring(response.content)
    temperatures = root[4]
    temperatures_list = []
    for el in temperatures[-4:]:
        temperatures_list.append(float(el[1].text))
    precip = root[5]
    precip_list = []
    for el in precip[-4:]:
        precip_list.append(float(el[1].text))
    wind_speed = root[6]
    wind_speed_list = []
    for el in wind_speed[-4:]:
        wind_speed_list.append(float(el[1].text))
    #I understand that this is brute force but i couldn't understand the XML structure
    return {'avg_temp' : mean(temperatures_list), 'avg_wind_speed' : mean(wind_speed_list), 'precipitations' : sum(precip_list) }

def extract_data(data_list, param_name):
    now = datetime.now()
    data = []
    for item in data_list:
        timestamp_str = item.find('ns0:data', namespaces={'ns0': 'http://www.meteotrentino.it/'}).text
        value = float(item.find(f'ns0:{param_name}', namespaces={'ns0': 'http://www.meteotrentino.it/'}).text)
        
        # Convert timestamp string to datetime object
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
        
        # Check if timestamp is within the past 24 hours and at hourly intervals
        if now - timedelta(hours=24) <= timestamp <= now:
            if timestamp.minute == 0 and timestamp.second == 0:
                data.append([timestamp, value])

    return data

def get_past_measurements(station_code, count = 24):
    url = f"https://dati.meteotrentino.it/service.asmx/ultimiDatiStazione?codice={station_code}"

    # Make a GET request
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    root = ET.fromstring(response.content)

    temperatures = root[4]
    temperature_data = []
    precipitations = root[5]
    precipitation_data = []
    wind_speeds = root[6]
    wind_speed_data = []

    temperature_data = extract_data(temperatures, 'temperatura')
    wind_speed_data = extract_data(wind_speeds, 'v')
    precipitation_data = extract_data(precipitations, 'pioggia')

    return temperature_data, wind_speed_data, precipitation_data
