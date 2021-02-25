import os
from google.cloud import bigquery


def update_user_cart(data, context):
    client = bigquery.Client()
    bucket_name = data['bucket']
    file_name = "web_user_visits_data_aggregated/*.avro"
    dataset_id = "data_analysis"
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.AVRO
    job_config.ignore_unknown_values = True
    uri = "gs://%s/%s" % (bucket_name, file_name)
    load_job = client.load_table_from_uri(uri, dataset_ref.table('cart_products'), job_config=job_config)
    print("Starting Job {}".format(load_job.job_id))
    load_job.result()
    print("Job Finished")
    destination_table = client.get_table(dataset_ref.table('cart_products'))
    print("Loaded {} rows".format(destination_table.num_rows))


def ingest_user_visit(data, context):
    client = bigquery.Client()
    bucket_name = data['bucket']
    file_name = data['name']
    dataset_id = "data_analysis"
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.ignore_unknown_values = True
    uri = "gs://%s/%s" % (bucket_name, file_name)
    load_job = client.load_table_from_uri(uri, dataset_ref.table('user_visits'), job_config=job_config)
    print("Starting Job {}".format(load_job.job_id))
    load_job.result()
    print("Job Finished")
    destination_table = client.get_table(dataset_ref.table('user_visits'))
    print("Loaded {} rows".format(destination_table.num_rows))

def iot_transformed_data(data, context):
    client = bigquery.Client()
    bucket_name = data['bucket']
    file_name = data['name']
    dataset_id = "data_analysis"
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.AVRO
    job_config.ignore_unknown_values = True
    uri = "gs://%s/%s" % (bucket_name, file_name)
    load_job = client.load_table_from_uri(uri, dataset_ref.table('device_energy_consumption'), job_config=job_config)
    print("Starting Job {}".format(load_job.job_id))
    load_job.result()
    print("Job Finished")
    destination_table = client.get_table(dataset_ref.table('device_energy_consumption'))
    print("Loaded {} rows".format(destination_table.num_rows))