gcloud dataproc jobs submit pyspark \
gs://spark-etl-1/spark-job/flight-etl.py --cluster=spark-dwh --region=us-central1