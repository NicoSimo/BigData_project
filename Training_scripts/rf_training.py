import dask.dataframe as dd
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import skops.io
import time
import os
import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def fetch_data(table_name, index_col=None):
    """
    Fetch data from PostgreSQL using Dask and return a Dask DataFrame.
    """
    postgre_host = os.getenv('POSTGRES_HOST', 'postgres')
    postgre_user = os.getenv('POSTGRES_USER', 'postgres')
    postgre_password = os.getenv('POSTGRES_PASSWORD', 'Team3')
    postgre_db = os.getenv('POSTGRES_DB', 'energy_consumption')

    try:
        # Create a connection string
        connection_string = f"postgresql://{postgre_user}:{postgre_password}@{postgre_host}/{postgre_db}"
        engine = create_engine(connection_string)
        # Use Dask to read the SQL table into a Dask DataFrame
        if index_col is not None:
            ddf = dd.read_sql_table(table_name, connection_string, index_col=index_col)
        else:
            # Use pandas to read SQL table into a DataFrame
            with create_engine(connection_string).connect() as conn:
                df = pd.read_sql_table(table_name, conn)

            # Create a Dask DataFrame from the fetched data
            ddf = dd.from_pandas(df, npartitions=1)
        
        log.info(f"Data fetched successfully from {table_name}.")
        return ddf
    except Exception as e:
        log.error(f"Database fetch error: {e}")
        return None

def train_model():
    try:
        df1 = fetch_data("sensor_data", index_col="measurement_id")
        df2 = fetch_data("weather_data", index_col="weather_data_id")
        df3 = fetch_data("buildings")

        # Merge to obtain building information
        df1 = df1.merge(df3, how="left", on="building_id")
        # Merge to obtain weather information
        dfjoined = df1.merge(df2, how="left", left_on=["timestamp", "site_id"], right_on=["timestamp", "site_id"])

        # Re-indexing and conversion of timestamps
        dfjoined = dfjoined.reset_index(drop=True)
        dfjoined['timestamp'] = dd.to_datetime(dfjoined['timestamp'], format="%Y-%m-%d %H:%M:%S")

        # Feature selection
        cols_to_keep = ["building_id", "timestamp", "meter_reading", "site_id", "square_feet",
                        "year_built", "air_temperature", "wind_speed", "precip_depth_1_hr"]
        dfjoined = dfjoined[cols_to_keep]

        # Insert model target
        dfjoined['target'] = 0
        dfjoined = dfjoined.map_partitions(lambda df: df.assign(
            target=df['meter_reading'].shift(-1) + df['meter_reading'].shift(-2) + df['meter_reading'].shift(-3))
        )

        # Sorting of the dataframe based on building and timestamp
        dfjoined = dfjoined.sort_values(by=["building_id", "timestamp"], axis=0, kind="stable").reset_index(drop=True)

        # Filling missing values with most recent data
        dfjoined['air_temperature'] = dfjoined['air_temperature'].ffill()
        dfjoined['wind_speed'] = dfjoined['wind_speed'].ffill()
        dfjoined['precip_depth_1_hr'] = dfjoined['precip_depth_1_hr'].fillna(0)

        # We are only interested in the hour of the day
        dfjoined['timestamp'] = dfjoined['timestamp'].dt.hour

        # Insert the previous two metric readings
        dfjoined['met-2'] = dfjoined['meter_reading'].shift(2)
        dfjoined['met-1'] = dfjoined['meter_reading'].shift(1)

        # Feature selection
        col_to_keep = ["timestamp", "meter_reading", "square_feet", "year_built", "air_temperature",
                       "wind_speed", "precip_depth_1_hr", "met-2", "met-1", "target"]
        dfjoined = dfjoined[col_to_keep]

        # Convert to Pandas for model training
        dfrf = dfjoined.dropna().compute()

        # Rearranging columns order
        dfrf = dfrf[["timestamp", "meter_reading", "square_feet", "year_built", "air_temperature",
                     "wind_speed", "precip_depth_1_hr", "met-2", "met-1", "target"]]
        dfrf.columns = ["timestamp", "meter_reading", "square_feet", "year_built", "air_temperature",
                        "wind_speed", "precip_depth_1_hr", "met-2", "met-1", "target"]
        # Random forest instantiation and training
        clf = RandomForestRegressor(n_estimators=250, max_depth=11, random_state=42, max_features="sqrt")
        clf.fit(dfrf.iloc[:, :-1], dfrf.iloc[:, -1])

        if clf:
            skops.io.dump(clf, "Predictor/rf.skops")
            log.info("Model training completed successfully. SIA SEMPRE RINGRAZIATO IL SIGNORE!")
        else:
            log.error("Model training did not complete successfully.")

    except Exception as e:
        log.warning(f"An error occurred during model training: {e}")

def run_training():
    RETRAIN_TIME = int(os.getenv('RETRAIN_TIME', 120))
    while True:
        # Retrain the model every RETRAIN_TIME seconds
        train_model()
        time.sleep(RETRAIN_TIME)

# Example usage
if __name__ == "__main__":
    run_training()