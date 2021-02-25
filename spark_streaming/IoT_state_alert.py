from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from kafka import KafkaProducer

spark = SparkSession \
    .builder \
    .appName("IoT-log-analysis-using-spark-streaming") \
    .getOrCreate()

cluster = 'spark-streaming-etl'
bucket_path = 'gs://spark-streaming-etl/IoT_files'
producer = KafkaProducer(bootstrap_servers=[cluster + '-w-0:9092', cluster + '-w-1:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
schema = StructType(
    [
        StructField('date_time', TimestampType(), True),
        StructField('house_id', IntegerType(), True),
        StructField('overall_consumption', DoubleType(), True),
        StructField('dishwasher_consumption', DoubleType(), True),
        StructField('furnace_1_consumption', DoubleType(), True),
        StructField('furnace_2_consumption', DoubleType(), True),
        StructField('fridge_consumption', DoubleType(), True),
        StructField('wine_cellar_consumption', DoubleType(), True),
        StructField('microwave_consumption', DoubleType(), True)
    ]
)

df = spark \
    .readStream \
    .format("csv") \
    .option("sep", ",") \
    .schema(schema) \
    .csv(bucket_path)

# spark maintain the state of aggregation for the delay specified in watermark
df_aggregated = df \
    .withWatermark("date_time", "1 day") \
    .groupBy(
    df.house_id) \
    .agg(
    sum(col("overall_consumption")).alias("overall_consumption"),
    sum(col("overall_consumption")).alias("overall_consumption"),
    sum(col("dishwasher_consumption")).alias("dishwasher_consumption"),
    sum(col("furnace_1_consumption")).alias("furnace_1_consumption"),
    sum(col("furnace_2_consumption")).alias("furnace_2_consumption"),
    sum(col("fridge_consumption")).alias("fridge_consumption"),
    sum(col("wine_cellar_consumption")).alias("wine_cellar_consumption"),
    sum(col("microwave_consumption")).alias("microwave_consumption"),
    max(col('date_time')).alias("latest_event_datetime")
)


def foreach_batch_function(df, epoch_id):
    overall_energy_threshold = 13
    df_alert = df.select("house_id", "overall_consumption") \
        .where("overall_consumption>=" + str(overall_energy_threshold))
    alerts = df_alert.toJSON()
    if not alerts.isEmpty():
        for row in alerts.collect():
            producer.send('high_consumption_alerts', value=row)


# pull data every 1 minute
query = df_aggregated.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()

query.awaitTermination()
