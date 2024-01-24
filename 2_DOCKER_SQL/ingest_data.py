#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from pyarrow.parquet import ParquetFile
import pyarrow.parquet as pq
import pyarrow as pa
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
    parquet_name = 'yellow_tripdata_2021-01.parquet'

    os.system(f"wget {url} -O {parquet_name}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    parquet_file = pq.ParquetFile(parquet_name)

    for i in parquet_file.iter_batches(batch_size=100000):
        chunk_df = i.to_pandas()

    # for create table on postgres
    chunk_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    chunk_size = 100000
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        t_start = time()
        table = pa.Table.from_batches([batch])
        # Process the chunk (table)
        chunk_df = table.to_pandas(split_blocks=True, self_destruct=True)
        chunk_df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted another chunk, took %.3f second' %(t_end-t_start))


if __name__== '__main__':
    

    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    #user , password, host, port , database name, table name,
    #url of the parquet
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')


args = parser.parse_args()

main(args)




