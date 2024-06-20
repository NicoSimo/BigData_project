import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm
import dask.dataframe as dd
import pandas as pd
import numpy as np

# Class to convert the Dask Dataframe into a Tensor Dataframe
class EnergyDataset(Dataset):
    def __init__(self, dataframe, sequence_length):
        self.dataframe = dataframe
        self.sequence_length = sequence_length
        self.data = self.preprocess_data()

    def __len__(self):
        return len(self.data) - self.sequence_length

    def __getitem__(self, idx):
        sequence = self.data[idx: idx + self.sequence_length, :-1]
        target = self.data[idx+self.sequence_length, -1]
        sequence = np.array(sequence).reshape((self.sequence_length, -1))
        target = np.array(target).reshape((1, -1))
        return sequence, target
        

# Deep Learning Model
class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.nl = nn.LeakyReLU()
        self.fc = nn.Linear(hidden_size, output_size)


    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, (hn, cn) = self.lstm(x, (h0.detach(), c0.detach()))
        out = self.nl(out[:, -1, :])
        out = self.nl(out)
        out = self.fc(out)
        return out

# Read the dataframes as Dask Dataframes
df1 = dd.read_csv("historical_consumptions.csv")
df2 = dd.read_csv("historical_weather.csv")
df3 = dd.read_csv("building_data.csv")

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

# Features selection
col_to_keep = [1, 2, 4, 5, 6, 7, 8, 9]
dfn = df.iloc[:,col_to_keep]

# Preprocessing
train_data = EnergyDataset(dfn, sequence_length=3)
trainloader = DataLoader(train_data, batch_size=5880, shuffle=False)

# Model instantiation
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = LSTMModel(7, 20, 2, 1)
model.to(device)

# Define optimizer
optimizer = optim.AdamW(model.parameters(), lr=1, weight_decay=1e-2)
# Define learning rate scheduler
scheduler = optim.lr_scheduler.ExponentialLR(optimizer, gamma=0.8)

# Define loss
criterion = nn.MSELoss()
for epoch in range(20):
    # Train
    model.train()
    with tqdm(trainloader) as pbar:
        for i, (data, targets) in enumerate(pbar):
            # The last 3 are removed as we do not know the predictions
            data = data[:-3, :, :]
            targets = targets[:-3, :, :]
            data = data.to(device)
            # Conversions needed for the model
            data = data.to(torch.float32)
            targets = targets.to(torch.float32)

            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, targets.to(device))
            loss.backward()
            optimizer.step()
            # Print learning rate and average loss per batch
            pbar.set_postfix(loss=loss.item()/5877, lr=optimizer.param_groups[0]['lr'])

    # Update learning rate
    scheduler.step()

    # Save the model
    torch.save(model, "/Predictor/lstm.pt")
