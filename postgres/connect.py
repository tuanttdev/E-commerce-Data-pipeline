import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from windows_ultis.get_windows_host import get_windows_host_ip
from sqlalchemy import create_engine

USER_POSTGRES = 'postgres'
PASSWORD_POSTGRES = 'tuantt'
DATABASE_POSTGRES = 'retail_orders_db'
PORT_POSTGRES = '5432'
WINDOWS_HOST = get_windows_host_ip()

def connect_to_postgres():
    return create_engine(f'postgresql+psycopg2://{USER_POSTGRES}:{PASSWORD_POSTGRES}@{WINDOWS_HOST}:{PORT_POSTGRES}/{DATABASE_POSTGRES}')

if __name__ == '__main__':

    print(connect_to_postgres())
    # print(datetime.now())
    # extract_collection()