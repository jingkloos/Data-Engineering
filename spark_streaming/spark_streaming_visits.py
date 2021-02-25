from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time
import datetime

spark = SparkSession \
    .builder \
    .appName("Ecom-user-activity-log-analysis-using-spark-streaming") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-streaming-etl-w-0:9092") \
    .option("subscribe", "user_browsing_logs") \
    .load() \
    .selectExpr("CAST(value as STRING)")

schema = StructType(
    [
        StructField('category', StringType(), True),
        StructField('date_time', TimestampType(), True),
        StructField('type', StringType(), True),
        StructField('pid', IntegerType(), True),
        StructField('state', StringType(), True),
        StructField('sub_cat', StringType(), True),
        StructField('ip_address', StringType(), True)
    ]
)

df_parsed = df.select("value")

df_streaming_visits = df_parsed.withColumn("data", from_json("value", schema)).select(col('data.*'))


# watermark means wait period for late data, can you control batch time?

def foreach_batch_function(df, epoch_id):
    """

    :param df: dataframe
    :param epoch_id: batch id
    :return:
    """
    df_final = df.select("category", "date_time", "sub_cat", "pid", "type", "state", "ip_address")
    print(epoch_id)
    df_final.show(10, False)

    df_final.coalesce(1).write \
        .format("parquet") \
        .mode("append") \
        .option("checkpointLocation", "gs://spark-streaming-etl/spark_checkpoints") \
        .option("path", "gs://spark-streaming-etl/streaming_raw_output") \
        .save()


# pull data every 1 minute
query = df_streaming_visits.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

query.awaitTermination()
