import pandas as pd
import numpy as np
import argparse
import os
import pathlib
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    parquet_name = "output.parquet" # 'output.parquet' # "yellow_tripdata_2021-01.parquet"

    # download the parquet data
    os.system(f"wget {url} -O ./{parquet_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Read data file
    taxi_data_df = pd.read_parquet(parquet_name)

    # Read only the headers from the table and insert them to the database, by doing so thus creating a new database
    taxi_data_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Read Data by Chunks and insert these chunks into the database
    nr_rows = len(taxi_data_df)
    chunk_size = 100*1000
    print(f"Nr. Rows = {nr_rows}")

    chunk_start_row_ls = np.arange(0, nr_rows, chunk_size)
    for chunk_idx, chunk_row_start in enumerate(chunk_start_row_ls):
        chunk_row_end = chunk_row_start + chunk_size
        chunk_df = taxi_data_df.iloc[chunk_row_start:chunk_row_end]
        chunk_df.to_sql(name=table_name, con=engine, if_exists='append')
        print(f"Data upload progress: {(chunk_idx+1)/len(chunk_start_row_ls)*100:.2f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)

