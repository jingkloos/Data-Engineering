from datetime import datetime, timedelta, date
from airflow import models, DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, \
    DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from airflow.utils.trigger_rule import TriggerRule

current_date = str(date.today())
file_name = str(date.today()+timedelta(days=-1))

BUCKET = "gs://spark-etl-1"
PYSPARK_JOB = BUCKET + "/spark-job/flight-etl.py"
DEFAULT_DAG_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": "bigdata-etl-20201027",
    "scheduled_interval": "30 2 * * *"  #every day at 2:30 am utc
}

with DAG("flights_delay_etl", default_args=DEFAULT_DAG_ARGS) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-2",
        num_workers=2,
        region="us-central1",
        zone="us-central1-a"
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-central1"
    )

    bq_load_delay_by_flight_nums=GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_avg_delays_by_flight_nums",
        bucket="spark-etl-1",
        source_objects=["flights_data_output/"+file_name+"_flight_nums/*.json"],
        destination_project_dataset_table="bigdata-etl-20201027.data_analysis.avg_delays_by_flight_nums",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )
    bq_load_delay_by_distance = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_avg_delays_by_distance",
        bucket="spark-etl-1",
        source_objects=["flights_data_output/" + file_name + "_distance_category/*.json"],
        destination_project_dataset_table="bigdata-etl-20201027.data_analysis.avg_delays_by_distance_category",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )
    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-central1",
        trigger_rule=TriggerRule.ALL_DONE
    )

    delete_transformed_files = BashOperator(
        task_id="delete_transformed_files",
        bash_command="gsutil -m rm -r "+BUCKET+"/flights_data_output/*"
    )

    create_cluster.dag=dag
    create_cluster.set_downstream(submit_pyspark)
    submit_pyspark.set_downstream([bq_load_delay_by_distance, bq_load_delay_by_flight_nums, delete_cluster])
    delete_transformed_files.set_upstream([bq_load_delay_by_distance, bq_load_delay_by_flight_nums, delete_cluster])

