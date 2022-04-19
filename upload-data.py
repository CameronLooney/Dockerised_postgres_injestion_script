import os

import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'
    os.system(f'wget {url} -O {csv_name}')
    # need to download the csv

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True,
                          chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
        t_end = time()
        print("another chunk inserted. It took: " + str(t_end - t_start) + " seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data Postgresql')
    # args = user, password, host, port, database name , table name, url of csv
    parser.add_argument('--user', help='Username for Postgres DB')
    parser.add_argument('--password', help='Password for Postgres DB')
    parser.add_argument('--host', help='Host for Postgres DB')
    parser.add_argument('--port', help='Port for Postgres DB')
    parser.add_argument('--db', help='Database name for Postgres DB')
    parser.add_argument('--table_name', help='Name of Table to write results')
    parser.add_argument('--url', help='URL of CSV')
    args = parser.parse_args()
    main(args)

# command line arg
# python upload-data.py --user=root --password=root --host=localhost --port=5432 --db=taxi --table_name=yellow_taxi_data --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"