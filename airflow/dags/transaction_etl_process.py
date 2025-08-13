import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from datetime import datetime , time, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from mongodb.customers import send_dataframe_to_clickhouse
from pathlib import Path

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
    # fct_order = SparkSubmitOperator(
    #     task_id="fct_order",
    #     application="/home/tuantt/retailETLProject/spark/transform_facts/transform_fact_sale.py",
    #     conn_id="spark_default",  # hoặc bỏ nếu spark-submit đã có trong PATH
    #     name="fact_order_local",
    #     conf={"spark.master": "local[*]"},  # CHẠY LOCAL
    #     driver_memory="8g",  # local mode: chủ yếu dùng driver
    #     verbose=True
    # )

    fct_order = SparkSubmitOperator(
        task_id="fct_order",
        application=str(Path(__file__).resolve().parents[2] / "spark/transform_facts/transform_fact_sale.py"),
        conn_id="spark_local",  # dùng connection local
        name="fact_order_local",
        driver_memory="8g",
        verbose=True,
        packages=(
            "org.postgresql:postgresql:42.2.27,"
            "com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.2.1"
            # chỉ thêm httpclient5 nếu thật sự cần:
            # ",org.apache.httpcomponents.client5:httpclient5:5.2.1"
        ),
        env_vars={
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            # "SPARK_LOCAL_IP": "127.0.0.1",  # nếu muốn ẩn cảnh báo loopback
        },

        # KHÔNG đặt queue khi chạy local
        # KHÔNG thêm conf spark.master nữa để tránh xung đột
        # Nếu muốn dẹp cảnh báo loopback:
        # env_vars={"SPARK_LOCAL_IP": "127.0.0.1"},  # tùy chọn
    )


    # Task fct_order chạy sau khi tất cả dim tables hoàn thành
    [dim_customer, dim_product, dim_location] >>  fct_order >> dim_date



if __name__ == '__main__':
    print(str(Path(__file__).resolve().parents[2] ))