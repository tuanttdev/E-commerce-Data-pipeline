from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType, NullType

# from windows_ultis.get_windows_host import (get_windows_host_ip)
# from postgres.connect import USER_POSTGRES, PASSWORD_POSTGRES
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]

USER_POSTGRES = 'postgres'
PASSWORD_POSTGRES = 'tuantt'


import subprocess

def get_windows_host_ip():
    # Chạy lệnh shell để lấy dòng default route
    output = subprocess.check_output(['ip', 'route'], text=True)
    for line in output.splitlines():
        if line.startswith('default via'):
            parts = line.split()
            return parts[2]  # IP nằm ở vị trí thứ 3




WINDOWS_HOST_IP = get_windows_host_ip()

CHECKPOINT_DIR = str(project_root / './spark/transform_facts/transform_fact_sale_checkpoints')
STATES_DIR = str(project_root / './spark/transform_facts/transform_fact_sale_state')

spark = (SparkSession.builder
    .appName("Fact Sale Transformation")
         # .config("spark.jars.packages",
         #         "org.postgresql:postgresql:42.2.27,"
         #         "com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.2.1")

    .config("spark.sql.shuffle.partitions", 20)
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")   # vùng off-heap/JVM overhead
    # .config("spark.memory.fraction", "0.6")          # cho execution nhiều hơn
    # .config("spark.memory.storageFraction", "0.3")
    .master("local[*]")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

orders_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{WINDOWS_HOST_IP}:5432/retail_orders_db")
    .option("dbtable", "(SELECT * FROM orders ORDER BY order_id limit 1000) t")
    .option("user", USER_POSTGRES)
    .option("password", PASSWORD_POSTGRES)
    .option("driver", "org.postgresql.Driver")
    .option("fetchsize", "100000")
    .load()
)

order_details_df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:postgresql://{WINDOWS_HOST_IP}:5432/retail_orders_db")
    .option("dbtable", "(SELECT * FROM public.order_details ORDER BY order_id limit 1000) t")
    .option("user", USER_POSTGRES)
    .option("password", PASSWORD_POSTGRES)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", "order_seq_id")
    .option("lowerBound", "1")
    .option("upperBound", "176000000")  # ước lượng max id
    .option("numPartitions", "16")# chia partition ra, để chạy song song nhiều tasks , mà sao thấy nó vẫn chậm, đặc biệt là 2 task cuối trong 64 tasks
    .load()
)

print(type(orders_df))

# orders_df= orders_df.limit(100)
# order_details_df= order_details_df.limit(100)
# print(type(orders_df))
# print(type(order_details_df))
# print(orders_df.count())
#
# orders_df.show()
# order_details_df.show()
#
fact_df = (
    order_details_df.alias("d")
    .join(orders_df.alias("o"), col("d.order_id") == col("o.order_id"), "inner")
    .select(
        col("d.order_id"),
        col("d.product_id"),
        col("o.location_id"),
        lit("UNKNOWN").cast("String").alias("payment_method_id"),
        coalesce(col("d.unit_of_measure"), lit("UNKNOW")).alias("unit_of_measure"),
        col("d.quantity").cast("int"),
        col("d.price").cast("float").alias('regular_price'),
        col("d.discount").cast("decimal(16,2)").alias('discount_price'),
        (col("d.price") - col("d.discount"))
        .cast("decimal(16,2)")
        .alias("net_price"),
        col("d.amount").cast("decimal(16,2)"),
        col("o.status"),
        col("o.ordered_time").cast("timestamp").alias("order_created_time"),
        current_timestamp().alias("payment_completed_time"),
        current_timestamp().alias("shipping_started_time"),
        col("o.finished_time").cast("timestamp").alias("shipping_completed_time"),
        current_timestamp().alias("insert_time"),  # Thời gian hiện tại
        current_timestamp().alias("modify_time")  # Thời gian hiện tại

    )
)

fact_df.show(5, truncate=False )
print(fact_df.count())
## write to clickhouse
CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/retailDataWareHouse"  # đổi host/db nếu khác
CLICKHOUSE_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "jdbcCompliant": "false",
    "batchsize": "100000",            # batch insert lớn để nhanh hơn
    "socket_timeout": "600000",
    "use_server_time_zone": "true",    # map timestamp theo timezone server CH
    "username":'default',  # default user
    "password":'tuantt',
}

(fact_df
   .repartition(8, col("order_created_time"))  # 8-16 tùy core/IO
   .write
   .format("jdbc")
   .option("url", CLICKHOUSE_URL)
   .option("dbtable", "fact_product_sale")
   .options(**CLICKHOUSE_PROPS)
   .mode("append")     # append vào bảng đã tạo sẵn
   .save()
)

#
# print('first 5 rows of orders :')
# orders_df.show(5, truncate=False)
# print('first 5 rows of detail orders :')
# order_details_df.show(5, truncate=False)

# print(order_details_df.rdd.getNumPartitions()) # chia
# print('total value of all sold products :')
#
# order_details_df.agg( sum('amount').alias("total_amount") ).show()
