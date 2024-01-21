#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pyarrow.parquet import ParquetFile
import pyarrow.parquet as pq
import pyarrow as pa
from time import time
from sqlalchemy import create_engine
import argparse

def main(params):
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()

    chunk_size = 100000
    parquet_file = pq.ParquetFile('yellow_tripdata_2021-01.parquet')

    # for create table on postgres
    parquet_file.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        t_start = time()
        table = pa.Table.from_batches([batch])
        # Process the chunk (table)
        chunk_df = table.to_pandas(split_blocks=True, self_destruct=True)
        chunk_df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        t_end = time()
        print('inserted another chunk, took %.3f second' %(t_end-t_start))



parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

#user , password, host, port , database name, table name,
#url of the parquet
parser.add_argument('user', help='user name for postgres')
parser.add_argument('pass', help='pass for postgres')
parser.add_argument('host', help='host for postgres')
parser.add_argument('port', help='port for postgres')
parser.add_argument('db', help='database for postgres')
parser.add_argument('table-name', help='name of the table where we will write the results to')
parser.add_argument('url', help='url of the parquet file')


args = parser.parse_args()
print(args.accumulate(args.integers))



