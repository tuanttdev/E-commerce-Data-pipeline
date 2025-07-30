import clickhouse_connect
import os
from dotenv import load_dotenv
import time

DATABASE_NAME = 'retailDataWareHouse'
load_dotenv()
os.environ['TZ'] = os.getenv('TZ')
time.tzset()

DIM_TABLE_NAME = "dim_customer"
def connect_clickhouse():

    client = clickhouse_connect.get_client(
        host='localhost',  # your host
        port=8123,  # default HTTP port
        username='default',  # default user
        password='tuantt',
        database=DATABASE_NAME
    )
    if client is not None:
        print('Connected to ClickHouse')
        query = f"SELECT * FROM table_info WHERE table_name = '{DIM_TABLE_NAME}'"

        result = client.query(query)
        rows = result.result_rows
        print(rows)

    else:
        print('Failed to connect to ClickHouse')
    return client

def update_table_info(table_name=None):
    if not table_name:
        raise ValueError("Missing table_name")

    client = connect_clickhouse()

    ## kiểm tra bảng thật sự có tồn tại
    query_check_table = """
    SELECT count() 
    FROM system.tables 
    WHERE database = currentDatabase() AND name = %(table_name)s
    """
    table_exists = client.query(query_check_table, parameters={'table_name': table_name}).result_rows[0][0]

    if table_exists == 0:
        print(f"❌ Table {table_name} does not exist in database {DATABASE_NAME}")
        return

    ## kiểm tra trong table_info xem đã có record chưa
    query_check_info = """
    SELECT count() 
    FROM table_info 
    WHERE table_name = %(table_name)s
    """
    info_exists = client.query(query_check_info, parameters={'table_name': table_name}).result_rows[0][0]

    if info_exists == 0:
        ## Insert mới
        insert_sql = """
        INSERT INTO table_info (table_name, last_update, modify_time)
        VALUES (%(table_name)s, now(), now())
        """
        client.query(insert_sql, parameters={'table_name': table_name})
        print(f"✅ Inserted new record for {table_name} into table_info")
    else:
        ## Update
        update_sql = """
        ALTER TABLE table_info
        UPDATE last_update = now(), modify_time = now()
        WHERE table_name = %(table_name)s
        """
        client.query(update_sql, parameters={'table_name': table_name})
        print(f"✅ Updated record for {table_name} in table_info")

if __name__ == '__main__':
    connect_clickhouse() # connect được rồi