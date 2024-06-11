Repository for the Big Data Technologies course (A.Y 2023/2024) of the Master in Data Science @ University of Trento.

The project uses the dataset : ASHRAE - Great Energy Predictor III available on Kaggle at : https://www.kaggle.com/competitions/ashrae-energy-prediction/overview.

The dataset_prep.py requires the download of the dataset, to run the script you need to modify the path at line 29 --> 'native_dataset_folder'. 
You can also decide where to store the data by changing the path at line 32 --> 'base_dir'.

The dataset_prep.py prepares and organizes the data in folders that are then moved to a PostgreSQL container and are then used to populate the database.

RICORDARE DI RIMUOVERE PW ECC DAL DOCKER COMPOSE