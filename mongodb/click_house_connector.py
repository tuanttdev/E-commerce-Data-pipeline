import clickhouse_connect

def connect_clickhouse():
    client = clickhouse_connect.get_client(
        host='localhost',  # your host
        port=8123,  # default HTTP port
        username='default',  # default user
        password='tuantt'
    )
    if client is not None:
        print('Connected to ClickHouse')
    else:
        print('Failed to connect to ClickHouse')
    return client


if __name__ == '__main__':
    connect_clickhouse() # connect được rồi