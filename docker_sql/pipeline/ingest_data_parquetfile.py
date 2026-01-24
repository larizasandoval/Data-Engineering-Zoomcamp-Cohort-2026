#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.parquet as pq
from time import time
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    # Read metadata
    filename = f'green_tripdata_{year}-{month}.parquet'
    pq.read_metadata(filename)

    # Read file, read the table from file and check schema
    file = pq.ParquetFile(filename)
    table = file.read()
    #table.schema

    # Convert to pandas and check data 
    df = table.to_pandas()
    #df.info()

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Insert values into the table 
    t_start = time()
    count = 0
    for batch in file.iter_batches(batch_size=chunksize):
        count+=1
        batch_df = batch.to_pandas()
        batch_df = batch_df[list(dtype.keys())+parse_dates]
        batch_df[parse_dates] = batch_df[parse_dates].apply(pd.to_datetime)
        batch_df = batch_df.astype(dtype)
        print(f'inserting batch {count}...')
        b_start = time()
    
        batch_df.to_sql(name=target_table,con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
    
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    


if __name__ == '__main__':
    run()
