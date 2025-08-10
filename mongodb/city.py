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

COLLECTION_NAME = "city"
DIM_TABLE_NAME = "dim_city"


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

def convert_collection_to_df(location_data):

    location_list = list(location_data)
    location_df  = pd.DataFrame(location_list) ##
    if not(location_df.empty) :
        ## transformation
        ## remove column

        location_df = location_df[['_id' , 'city_id' , 'name', 'createdAt', 'update_date']]

        location_df.rename({'_id':'city_id', 'city_id':'city_key', 'name':'city_name' ,'createdAt': 'created_at', 'update_date': 'updated_at'}, axis=1, inplace=True)

        location_df['city_id'] = location_df['city_id'].astype(str)
        location_df['city_key'] = location_df['city_key'].astype(str)
        location_df['city_name'] = location_df['city_name'].astype(str)

        location_df['created_at'] = pd.to_datetime(
            location_df['created_at'],
            format='%d/%m/%Y',
            # format='mixed',
            # dayfirst=True,
            # errors='coerce'
        )
        location_df['updated_at'] = pd.to_datetime(
            location_df['updated_at'],
            format='%d/%m/%Y',
        )

    return location_df

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
        print(rows)
        last_time_update = rows[0][1]
    print(f'data_interval_start{data_interval_start}' )
    print(f'last_time_update {last_time_update}')

    data = extract_collection(data_interval_start=data_interval_start, data_interval_end=data_interval_end, full_load = full_load)
    location_df = convert_collection_to_df(data)
    print(location_df)
    if not location_df.empty:
        rows = location_df.values.tolist()

        clickhouse_client.insert(
            table=DIM_TABLE_NAME,
            data=rows,
            column_names=location_df.columns.tolist()
        )

        print(f'Load {len(rows)} rows into ClickHouse successfully')
    else:
        print(f'No new data')

    update_table_info(DIM_TABLE_NAME)

    return True

if __name__ == '__main__':
    send_dataframe_to_clickhouse(data_interval_start=datetime.strptime('2025-07-28 07:40:00','%Y-%m-%d %H:%M:%S') , data_interval_end=datetime.strptime('2025-07-28 07:40:00', '%Y-%m-%d %H:%M:%S'))
    print(datetime.now())
    # extract_collection()
