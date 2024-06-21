import dask.dataframe as dd
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import skops.io

# Read the dataframes as Dask Dataframes
df1 = dd.read_csv("Training/historical_consumptions.csv")
df2 = dd.read_csv("Training/historical_weather.csv")
df3 = dd.read_csv("Training/building_data.csv")

# Merge to obtain building information
df1 = df1.merge(df3, how="left", on="building_id").compute()
df1 = dd.from_pandas(df1, npartitions=4)

# Merge to obtain weather information
dfjoined = df1.merge(df2, how="left", left_on=["timestamp", "site_id"], right_on=["timestamp", "site_id"]).compute()

# Re-indexing and conversion of timestamps
dfjoined = dfjoined.reset_index(drop=True)
dfjoined.timestamp = pd.to_datetime(dfjoined.timestamp, format="%Y-%m-%d %H:%M:%S")

# Features selction
cols_to_keep = [0, 1, 2, 3, 5, 6, 8, 9, 10]
df = dfjoined.iloc[:,cols_to_keep]

# Insert of model target
df.insert(len(df.columns), 'target', 0)
building_list = pd.unique(df.building_id)
for building in building_list:
  dfbuild = df[df.building_id == building]
  dfbuild = dfbuild.reset_index()
  for index, row in dfbuild.iterrows():
    if index+3 < dfbuild.shape[0]:
      df.at[dfbuild.at[index, 'index'],'target'] = dfbuild.iat[index+1, 3] + dfbuild.iat[index+2, 3] + dfbuild.iat[index+3, 3]

# Sorting of the dataframe based on building and timestamp
df = df.sort_values(by=["building_id", "timestamp"], axis=0, kind="stable")
df = df.reset_index(drop=True)

# Filling missing values with most recent data
for index, row in df.iterrows():
  i=1
  while(pd.isnull(df.at[index, 'air_temperature'])):
    df.at[index, 'air_temperature'] = df.at[index+i, "air_temperature"]
    i+=1

df.precip_depth_1_hr = df.precip_depth_1_hr.fillna(0)
for index, row in df.iterrows():
    if df.at[index, 'precip_depth_1_hr'] == 0:
          df.at[index, 'cloud_coverage'] = 0
    else:
          df.at[index, 'cloud_coverage'] = 6.0

# We are only interested in the hour of the day
df.timestamp = df.timestamp.dt.hour

# Insert the previous two metric readings
df.insert(len(df.columns)-1, 'met-2', 0)
df.insert(len(df.columns)-1, 'met-1', 0)

for index, row in df.iterrows():
    if (index%5880 not in [0,1]):
        df.at[index, "met-2"] = df.at[index-2, "meter_reading"]
        df.at[index, "met-1"] = df.at[index-1, "meter_reading"]

# Feature selection
col_to_keep = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11]
dfrf = df.iloc[:, col_to_keep]

# Random forest instantiation and training
clf = RandomForestRegressor(n_estimators=200, random_state=42)
clf.fit(dfrf.iloc[:,:-1], dfrf.iloc[:,-1])


# Prediction of the training set
#y_pred = clf.predict(dfrf.iloc[:,:-1])
#mse = mean_squared_error(dfrf.iloc[:,-1], y_pred)
#print("Mean Squared Error: ", mse)

# Plot
#sns.scatterplot(x=y_pred, y=dfrf.iloc[:,-1])
#plt.show()

# Save model
skops.io.dump(clf, "/Predictor/rf.skops")
