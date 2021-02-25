from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from kafka import KafkaProducer

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName("IoT-log-analysis-using-spark-streaming") \
    .getOrCreate()

cluster = 'spark-streaming-etl'
bucket_path='gs://spark-streaming-etl/IoT_files'
temp_bucket='spark-to-bq-temp'
spark.conf.set('temporaryGcsBucket', temp_bucket)
producer = KafkaProducer(bootstrap_servers=[cluster + '-w-0:9092', cluster + '-w-1:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
schema = StructType(
    [
        StructField('event_time', TimestampType(), True),
        StructField('house_id', IntegerType(), True),
        StructField('overall_consumption', DoubleType(), True),
        StructField('dishwasher_consumption', DoubleType(), True),
        StructField('furnace_1_consumption', DoubleType(), True),
        StructField('furnace_2_consumption', DoubleType(), True),
        StructField('fridge_consumption', DoubleType(), True),
        StructField('wine_cellar_consumption', DoubleType(), True),
        StructField('microwave_consumption', DoubleType(), True),
    ]
)
df = spark \
    .readStream \
    .format("csv") \
    .option("sep", ",") \
    .option("header", "true") \
    .schema(schema) \
    .csv(bucket_path)

#spark maintain the state of aggregation for the delay specified in watermark
df_aggregated = df \
    .withWatermark("event_time", "1 day") \
    .groupBy(
        df.house_id) \
    .agg(
        sum(col("overall_consumption")).alias("overall_consumption"),
        sum(col("dishwasher_consumption")).alias("dishwasher_consumption"),
        sum(col("furnace_1_consumption")).alias("furnace_1_consumption"),
        sum(col("furnace_2_consumption")).alias("furnace_2_consumption"),
        sum(col("fridge_consumption")).alias("fridge_consumption"),
        sum(col("wine_cellar_consumption")).alias("wine_cellar_consumption"),
        sum(col("microwave_consumption")).alias("microwave_consumption"),
        max(col('event_time')).alias("latest_event_datetime")
)

def to_bq_function(df, epoch_id):
    df_final = df.dropDuplicates()

    df_final.coalesce(1).write.format('bigquery') \
    .mode('overwrite') \
    .option('table', 'data_analysis.device_energy_consumption') \
    .save()

def to_kafka_function(df, epoch_id):
    overall_energy_threshold = 10
    df_alert = df.select("house_id", "overall_consumption") \
        .where("overall_consumption>=" + str(overall_energy_threshold))
    alerts = df_alert.toJSON()
    if not alerts.isEmpty():
        for row in alerts.collect():
            producer.send('high_consumption_alerts', value=row)

# pull data every 1 minute
query1 = df_aggregated.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(to_bq_function) \
    .outputMode("complete") \
    .start()

query2 = df_aggregated.writeStream \
    .trigger(processingTime="60 seconds") \
    .foreachBatch(to_kafka_function) \
    .outputMode("update") \
    .start()

query1.awaitTermination()

query2.awaitTermination()
