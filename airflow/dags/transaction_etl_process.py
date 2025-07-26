import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from datetime import datetime , time, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from mongodb.customers import send_dataframe_to_clickhouse

default_args = {
    'owner': 'Thanh Tuan',
    'start_date': datetime(2024, 7, 1)
}

with DAG(
    'process_retail_data',
    default_args=default_args,
    description='etl raw retail data from mongodb and postgres to clickhouse',
    schedule=timedelta(minutes=15),
    catchup=False,
    tags=['order', 'etl']) as dag:

    dim_customer = PythonOperator(
        task_id="dim_customer",
        python_callable=send_dataframe_to_clickhouse,
    )

    dim_product = PythonOperator(
        task_id="dim_product",
        python_callable=send_dataframe_to_clickhouse,
    )

    dim_location = PythonOperator(
        task_id="dim_location",
        python_callable=send_dataframe_to_clickhouse,
    )

    dim_date = PythonOperator(
        task_id="dim_date",
        python_callable=send_dataframe_to_clickhouse,
    )

    # Task build fact table
    fct_order = SparkSubmitOperator(
        task_id="fct_order",
        application="/path/to/transform_fct_order.py"
    )


    # Task fct_order chạy sau khi tất cả dim tables hoàn thành
    [dim_customer, dim_product, dim_location] >>  fct_order >> dim_date


