import pandas as pd
import numpy as np

filepath = "/home/dlp/Downloads/fhv_tripdata_2020-01.csv"
df = pd.read_csv(filepath)

print("df.shape: ", df.shape)
print(df.isna().sum())

df = df[df.pickup_datetime.notna()]
df = df[df.dropoff_datetime.notna()]

df.PULocationID = df.PULocationID.astype(np.int64)
df.DOLocationID = df.DOLocationID.astype(np.int64)

print("After cleaning df.shape: ", df.shape)

df.to_csv(filepath, index=False)

pass