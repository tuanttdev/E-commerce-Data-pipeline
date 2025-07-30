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

COLLECTION_NAME = "customer"
DIM_TABLE_NAME = "dim_customer"


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

def fix_dob(x):
    try:
        return pd.to_datetime(x, dayfirst=True).date()
    except:
        if isinstance(x, str):
            parts = x.split('/')
            if len(parts) == 3 and parts[2].isdigit():
                year = parts[2]
                return pd.to_datetime(f'01/01/{year}', dayfirst=True).date()
        return datetime.date(2000, 1, 1)

def convert_collection_to_df(customer_data):

    cus_list = list(customer_data)
    customer_df  = pd.DataFrame(cus_list) ##
    if not(customer_df.empty) :
        ## transformation
        ## remove column

        customer_df = customer_df[['_id' , 'first_name' , 'last_name', 'full_name', 'dob', 'ward_id', 'createdAt']]
        customer_df.rename({'createdAt': 'created_at', '_id':'customer_id' }, axis=1, inplace=True)


        customer_df['customer_id'] = customer_df['customer_id'].astype(str)
        customer_df['ward_id'] = customer_df['ward_id'].astype(str)
        #
        customer_df['dob'] = customer_df['dob'].apply(fix_dob)

        customer_df['dob'] = customer_df['dob'].where(pd.notnull(customer_df['dob']), None)


        customer_df['created_at'] = pd.to_datetime(
            customer_df['created_at'],
            # format='%d/%m/%Y',
            format='mixed',
            dayfirst=True,
            # errors='coerce'
        )

    return customer_df

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
    customer_df = convert_collection_to_df(data)
    print(customer_df)
    if not customer_df.empty:
        rows = customer_df.values.tolist()

        clickhouse_client.insert(
            table=DIM_TABLE_NAME,
            data=rows,
            column_names=customer_df.columns.tolist()
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
