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
    .option("kafka.bootstrap.servers", "spark-streaming-etl-w-1:9092") \
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
df_tumbling_window = df_streaming_visits \
    .where("pid is not null") \
    .withWatermark("date_time", "5 minutes") \
    .groupBy(window("date_time", "3 minutes"),
        df_streaming_visits.sub_cat,
        df_streaming_visits.category,
        df_streaming_visits.type) \
    .count()

#

def foreach_batch_function(df, epoch_id):
    """

    :param df: dataframe
    :param epoch_id: batch id
    :return:
    """
    df_final = df.select("window.start", "window.end", "sub_cat", "category", "type", "count")
    print(epoch_id)
    df_final.show(10, False)

    df_final.coalesce(1).write \
        .format("avro") \
        .mode("append") \
        .option("checkpointLocation", "gs://spark-streaming-etl/spark_checkpoints") \
        .option("path", "gs://spark-streaming-etl/web_user_visits_data_aggregated/") \
        .save()


# df_aggregated = df_streaming_visits \
#     .groupBy(df_streaming_visits.state) \
#     .count()

# pull data every 1 minute and aggreate data of last 3 minutes
query = df_tumbling_window.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

query.awaitTermination()
