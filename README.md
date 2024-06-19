Repository for the Big Data Technologies course (A.Y 2023/2024) of the Master in Data Science @ University of Trento.

The project uses the dataset : ASHRAE - Great Energy Predictor III available on Kaggle at : https://www.kaggle.com/competitions/ashrae-energy-prediction/overview.

The dataset_prep.py requires the download of the dataset, to run the script you need to modify the path at line 29 --> 'native_dataset_folder'. 
You can also decide where to store the data by changing the path at line 32 --> 'base_dir'.

We created a virtual environment to install all the dependencies contained in the 'requirements.txt' file to run the project by using 'python -m myvenv' and then 'myenv activate'.
Run 'requirements.txt' to install all the dependencies.

The docker-compose.yml file is used to set up the different containers used in the project. (One for each application to take advantage of the lightweight nature of Docker's Containers).

The dataset_prep.py prepares and organizes the data in folders that are then moved to a PostgreSQL container and are then used to populate the database.

The repository '/Sensor_core/' contains the file 'sensor_object.py' which contains the 'Sensor class' used to build the structure of the sensors.

The repository '/Energy_consumption_sensor/' contains the 'sensor_publisher.py' file used to start Kafka by uploading the data from the '/absulute_path/Data/New_data/Sensors/new_consumptions.csv' file used to simulate the sensors retrieving the new data.

In the 'Consumers' repository, there are 2 different consumers. 
The first one is the postgre_consumer, used to transfer the data on the PostgreSQL database to store the data. (TO DO)
The second one is the redis_consumer, used to transfer the data on Redis to perform the prediction.

Added Data_fetching/dask_fetch.py, planning of using DASK to retrieve the data from redis and load them in a dask df. (STILL UNTESTED --> TO DO)

Moving all the python scripts in a dedicated container. (TO DO) --> NEED TO MODIFY THE dockerfile

The info such as DB names, pw, user are in the .env file. The file is uploaded given the educational goal of the project. 

To run the whole project you need to (in order):

- Download the dataset 
- Fix the paths in the dataset_prep.py file 
- 'Docker-compose up --build -d' to run the containers
- (OPTIONAL) 'Docker ps' to verify the status
- ... (TO DO)