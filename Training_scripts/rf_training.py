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

def fetch_data(table_name, index_col = None):
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
        
        log.info("Data fetched successfully from {table_name}.")
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
    df1 = df1.merge(df3, how="left", on="building_id").compute()
    df1 = dd.from_pandas(df1, npartitions=2)
    # Merge to obtain weather information
    dfjoined = df1.merge(df2, how="left", left_on=["timestamp", "site_id"], right_on=["timestamp", "site_id"]).compute()

    # Re-indexing and conversion of timestamps
    dfjoined = dfjoined.reset_index(drop=True)
    dfjoined = dd.from_pandas(dfjoined, npartitions=2)
    dfjoined.timestamp = pd.to_datetime(dfjoined.timestamp, format="%Y-%m-%d %H:%M:%S").compute()

    # Features selction
    cols_to_keep = ["building_id", "timestamp", "meter_reading", "site_id", "square_feet",
                    "year_built", "air_temperature", "wind_speed", "precip_depth_1_hr"]
    df = dfjoined.loc[:,cols_to_keep].compute()

    # Insert of model target
    df.insert(len(df.columns), 'target', 0).compute()
    building_list = pd.unique(df.building_id).compute()
    for building in building_list:
      dfbuild = df[df.building_id == building].compute()
      dfbuild = dfbuild.reset_index().compute()
      for index, row in dfbuild.iterrows():
        if index+3 < dfbuild.shape[0]:
          df.at[dfbuild.at[index, 'index'],'target'] = (dfbuild.at[index+1, "meter_reading"] +
                                                        dfbuild.at[index+2, "meter_reading"] +
                                                        dfbuild.at[index+3, "meter_reading"]).compute()

    # Sorting of the dataframe based on building and timestamp
    df = df.sort_values(by=["building_id", "timestamp"], axis=0, kind="stable").compute()
    df = df.reset_index(drop=True).compute()

    # Filling missing values with most recent data
    for index, row in df.iterrows():
      i = 1
      while(pd.isnull(df.at[index, 'air_temperature'])):
        df.at[index, 'air_temperature'] = df.at[index+i, "air_temperature"].compute()
        i += 1

    df.precip_depth_1_hr = df.precip_depth_1_hr.fillna(0).compute()

    for index, row in df.iterrows():
        i = 1
        while (pd.isnull(df.at[index, 'wind_speed'])):
            df.at[index, 'wind_speed'] = df.at[index + i, "wind_speed"].compute()
            i += 1

    # We are only interested in the hour of the day
    df.timestamp = df.timestamp.dt.hour.compute()

    # Insert the previous two metric readings
    df.insert(len(df.columns)-1, 'met-2', 0).compute()
    df.insert(len(df.columns)-1, 'met-1', 0).compute()

    for index, row in df.iterrows():
        if (index%5880 not in [0,1]):
            df.at[index, "met-2"] = df.at[index-2, "meter_reading"].compute()
            df.at[index, "met-1"] = df.at[index-1, "meter_reading"].compute()

    # Feature selection
    col_to_keep = ["timestamp", "meter_reading", "square_feet", "year_built", "air_temperature",
                   "wind_speed", "precip_depth_1_hr", "met-2", "met-1", "target"]
    dfrf = df.loc[:, col_to_keep].compute()

    # Rearranging columns order
    dfrf = dfrf[["timestamp", "meter_reading", "square_feet", "year_built", "air_temperature",
                 "wind_speed", "precip_depth_1_hr", "met-2", "met-1", "target"]]

    # Random forest instantiation and training
    clf = RandomForestRegressor(n_estimators=250, max_depth=11, random_state=42, max_features="sqrt")
    clf.fit(dfrf.iloc[:,:-1].compute(), dfrf.iloc[:,-1].compute())

    if clf:
      skops.io.dump(clf, "Predictor/rf_test.skops")
    else:
      log.error("Model training did not complete successfully.")

    # Prediction of the training set
    #y_pred = clf.predict(dfrf.iloc[:,:-1])
    #mse = mean_squared_error(dfrf.iloc[:,-1], y_pred)
    #print("Mean Squared Error: ", mse)

    # Plot
    #sns.scatterplot(x=y_pred, y=dfrf.iloc[:,-1])
    #plt.show()

  except Exception as e:
    log.warn(f"An error occurred during model training: {e}")

def run_training():

  RETRAIN_TIME = int(os.getenv('RETRAIN_TIME', 15))
  while True:
    # Retrain the model every RETRAIN_TIME seconds
    train_model()
    time.sleep(RETRAIN_TIME)
    
    