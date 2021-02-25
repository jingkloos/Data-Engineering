gcloud dataproc jobs submit pyspark \
spark_streaming_aggregates.py \
--cluster=spark-streaming-etl \
--region=us-central1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.12-2.4.7.jar,jar-files/spark-sql-kafka-0-10_2.12-2.4.7.jar \
--properties spark.jars.packages=org.apache.spark:spark-avro_2.12:2.4.7

gcloud dataproc jobs submit pyspark \
IoT_data_transformation.py \
--cluster=spark-streaming-etl \
--region=us-central1 \
--jars=jar-files/spark-streaming-kafka-0-10-assembly_2.12-2.4.7.jar,jar-files/spark-sql-kafka-0-10_2.12-2.4.7.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
