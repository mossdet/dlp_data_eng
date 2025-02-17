import pandas as pd
import numpy as np
import argparse
import os
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    output_name = "csv_data.csv"
    os.system(f"wget {url} -O {output_name}")

    zones_df = pd.read_csv(output_name)
    zones_df.to_sql(name=table_name, con=engine, if_exists='replace')



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

