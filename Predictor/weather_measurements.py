import requests
import xml.etree.ElementTree as ET
from statistics import mean

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



