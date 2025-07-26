from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from windows_ultis.get_windows_host import get_windows_host_ip
from postgres.connect import USER_POSTGRES, PASSWORD_POSTGRES

WINDOWS_HOST_IP = get_windows_host_ip()

KAFKA_BROKERS = "localhost:9092"
SOURCE_TOPIC = 'retail_transaction'
AGGREGATE_TOPIC = 'retail_aggregation'
ANOMALIES_TOPIC = 'retail_anomalies'
CHECKPOINT_DIR = './spark/consume_fake_transaction_checkpoints'
STATES_DIR = './spark/consume_fake_transaction_state'

spark = (SparkSession.builder
    .appName("Retail Transaction Processor")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                 "org.postgresql:postgresql:42.2.27")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
    .config("spark.sql.shuffle.partition", 20)
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


# Schema cho từng dòng chi tiết đơn hàng
detail_order_schema = StructType([
    StructField("amount", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("line_number", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("product_id", StringType(), True),
    StructField("discount", FloatType(), True),
    StructField("order_id", StringType(), True)
])

# Schema cho order tổng
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("address_id", StringType(), True),
    StructField("address", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("city", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("shipping_fee", IntegerType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("status", StringType(), True),
    StructField("ordered_time", TimestampType(), True),      # hoặc TimestampType() nếu muốn parse thẳng datetime
    StructField("finished_time", TimestampType(), True),
    StructField("insert_time", TimestampType(), True),
    StructField("modify_time", TimestampType(), True)
])

# Schema tổng
transaction_schema = StructType([
    StructField("detail_order", ArrayType(detail_order_schema), True),
    StructField("order", order_schema, True)
])

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", SOURCE_TOPIC)
    .option('startingOffsets', 'earliest')
    .option('failOnDataLoss', 'false')
    .load()
)

transactions_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col('value'), transaction_schema).alias("data")) \
    .select("data.*")


# stream vào postgres sử dụng foreachbatch vì streaming không hỗ trợ ghi vào relational database
def write_to_postgres(batch_df, batch_id):

    # tách orders
    orders_df = batch_df.select("order.*")

    # tách detail_order: explode mảng và add order_id
    detail_orders_df = (
        batch_df
        .withColumn("detail", explode("detail_order"))
        .select(
            col("detail.order_id"),
            col("detail.amount"),
            col("detail.quantity"),
            col("detail.line_number"),
            col("detail.price"),
            col("detail.product_id"),
            col("detail.discount")
        )
    )

    # ghi orders
    orders_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{WINDOWS_HOST_IP}:5432/retail_orders_db") \
        .option("dbtable", "orders") \
        .option("user", USER_POSTGRES) \
        .option("password", PASSWORD_POSTGRES) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # ghi detail_orders
    detail_orders_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{WINDOWS_HOST_IP}:5432/retail_orders_db") \
        .option("dbtable", "order_details") \
        .option("user", USER_POSTGRES) \
        .option("password", PASSWORD_POSTGRES) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

transactions_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start() \
    .awaitTermination()




## aggerate data into kafka again ########################################
#
# exploded_df = transations_df.select(
#     explode(col("detail_order")).alias("detail"),
#     col("order.*")  # giữ lại các cột từ order để sau join hoặc dùng
# )
#
# exploded_df.printSchema()
#
# aggregated_df = exploded_df.groupBy(col("detail.order_id")) \
#     .agg(
#         sum(col("detail.amount")).alias("total_amount"),
#         count("*").alias("transactionCount")
#     )
# #
# # # debug null value
# #
# # aggregation_result = aggregated_df \
# #     .withColumn("order_id", col("order_id").cast("string")) \
# #     .withColumn("value_json", to_json(struct(
# #         col("order_id"),
# #         col("total_amount"),
# #         col("transactionCount")
# #     )))
# #
# # # Filter ra những bản ghi bị null để debug:
# # null_value_rows = aggregation_result.filter(col("value_json").isNull())
# #
# #
# # # Sau đó dùng:
# # final_df = aggregation_result \
# #     .filter(col("value_json").isNotNull()) \
# #     .withColumnRenamed("order_id", "key") \
# #     .withColumnRenamed("value_json", "value") \
# #     .select("key", "value")
# #
# # query = null_value_rows \
# #     .writeStream \
# #     .outputMode("update") \
# #     .format("console") \
# #     .start()
# #
# # query.awaitTermination()
# # ## end
#
# aggregation_query = aggregated_df \
#     .withColumn("key", col('order_id').cast("string")) \
#     .withColumn("value", to_json(struct(
#         col("order_id"),
#         col("total_amount"),
#         col("transactionCount")
#     ))) \
#     .selectExpr("key", "value") \
#     .writeStream \
#     .outputMode("update") \
#     .format("kafka") \
#     .option('kafka.bootstrap.servers', KAFKA_BROKERS) \
#     .option('topic', AGGREGATE_TOPIC) \
#     .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates') \
#     .start()
#
#
# aggregation_query.awaitTermination()
