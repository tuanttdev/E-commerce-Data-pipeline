import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mongodb.connect import connect_mongodb
import pandas as pd
from clickhouse_connect import *
from mongodb.click_house_connector import *
from datetime import datetime
import time
from dotenv import load_dotenv

load_dotenv()
os.environ['TZ'] = os.getenv('TZ')
time.tzset()

COLLECTION_NAME = "product"
DIM_TABLE_NAME = "dim_product"


def extract_collection(data_interval_start=None, data_interval_end=None, full_load = False):
    ## get customer data from mongo collection

    conn = connect_mongodb()
    collection = conn[COLLECTION_NAME]

    if full_load:
        ## full load for first time
        print('full load')
        data = collection.find()
    else:
        ## incremental load
        print(f"ETL từ {data_interval_start} đến {data_interval_end}")
        data = collection.find({"update_date": {"$gte": data_interval_start, "$lt": data_interval_end}})

    return data

def convert_collection_to_df(product_data):

    prod_list = list(product_data)
    product_df  = pd.DataFrame(prod_list) ##
    if not(product_df.empty) :
        ## transformation
        ## remove column

        product_df = product_df[['_id' , 'goods-title-link--jump' , 'price', 'discount', 'category', 'createdAt']]
        product_df.rename({'createdAt': 'created_at', '_id':'product_id', 'goods-title-link--jump' : 'product_name' }, axis=1, inplace=True)

        product_df['product_id'] = product_df['product_id'].astype(str)
        product_df['product_name'] = product_df['product_name'].fillna('')

        product_df['created_at'] = pd.to_datetime(
            product_df['created_at'],
            format='mixed',
            dayfirst=True,
        )

    return product_df

def send_dataframe_to_clickhouse(data_interval_start=None, data_interval_end=None):
    clickhouse_client = connect_clickhouse()
    query = f"SELECT * FROM table_info WHERE table_name = '{DIM_TABLE_NAME}'"

    result = clickhouse_client.query(query)
    rows = result.result_rows
    full_load = False
    last_time_update = None
    if len(rows) == 0:
        full_load = True
    else:

        last_time_update = rows[0][1]

    data = extract_collection(data_interval_start=data_interval_start, data_interval_end=data_interval_end, full_load = full_load)

    product_df = convert_collection_to_df(data)
    print(product_df)
    if not product_df.empty:
        rows = product_df.values.tolist()
        print(rows)
        clickhouse_client.insert(
            table=DIM_TABLE_NAME,
            data=rows,
            column_names=product_df.columns.tolist()
        )

        print(f'Load {len(rows)} rows into ClickHouse successfully')
    else:
        print(f'No new data')

    update_table_info(DIM_TABLE_NAME)

    return True

if __name__ == '__main__':
    send_dataframe_to_clickhouse(data_interval_start=datetime.strptime('2020-01-20 07:40:00','%Y-%m-%d %H:%M:%S') , data_interval_end=datetime.strptime('2025-07-28 07:40:00', '%Y-%m-%d %H:%M:%S'))
    # print(datetime.now())
    # extract_collection()
