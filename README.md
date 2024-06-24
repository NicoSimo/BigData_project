Repository for the Big Data Technologies course (A.Y 2023/2024) of the Master in Data Science @ University of Trento.

PROJECT #3
------------------------------------------------------------------------------------------------------------------------
Energy Consumption Forecasting: Develop a system for forecasting energy consumption patterns at both macro and micro levels. Utilize data from smart meters, weather forecasts, 
historical consumption data, and other relevant sources to predict future energy demand. Implement machine learning algorithms to identify trends, patterns, and anomalies in energy 
consumption, helping utilities optimize energy production and distribution.

![alt text](project_schema.png)

The project uses the dataset : ASHRAE - Great Energy Predictor III available on Kaggle at : https://www.kaggle.com/competitions/ashrae-energy-prediction/overview.

The dataset_prep.py requires the download of the dataset, to run the script you need to modify the path at line 29 --> 'native_dataset_folder'. 
You can also decide where to store the data by changing the path at line 32 --> 'base_dir'.
The first portion of data will be stored on the postgreDB, acting as our 'historical consumptions', while the second portion will arrive through the sensor trying to simulate actual sensors behaviour.

The docker-compose.yml file is used to set up the different containers used in the project. (One for each application to take advantage of the lightweight nature of Docker's Containers).

The dataset_prep.py prepares and organizes the data in folders that are then moved to a PostgreSQL container and are then used to populate the database.

The repository '/Sensor_core/' contains the file 'sensor_object.py' which contains the 'Sensor class' used to build the structure of the sensors.

The repository '/Energy_consumption_sensor/' contains the 'sensor_publisher.py' file used to start Kafka by uploading the data from the '/absulute_path/Data/New_data/Sensors/new_consumptions.csv' file used to simulate the sensors retrieving the new data.

The repository '/Training/' contains the code used to train the machine learning model.

In the 'Consumers' repository, there are 2 different consumers. 
The first one is the postgre_consumer, used to transfer the data on the PostgreSQL database to store the data.
The second one is the redis_consumer, used to transfer the data on Redis to perform the prediction.

The info such as DB names, pw, user are in the .env file. The file is uploaded given the educational goal of the project. 

To run the whole project you need to (in order):

- Download the dataset 
- Fix the paths in the dataset_prep.py file 
- 'Docker-compose up --build -d' to run the containers
- (OPTIONAL) 'Docker ps' to verify the status

Now the data start to flow from the csv to both Redis and Postgre through Kafka.
There are 3 topics :
-   'energy_consumption_redis' is used to send the data on Redis;
-   ‘energy_consumption_postgre’ is set with a retention parameter and used to send the data on Postgre every 3 updates to ease the load on the DB
-   'energy_consumption_predictions' is used by Redis to upload the predictions performed on the upcoming data 

To see a live dashboard, just type 'localhost:3000' on a browser, that will open a grafana dashboard directly connected to the PostgreSQL database (Both the USER and PASSWORD are left as default : 'admin').