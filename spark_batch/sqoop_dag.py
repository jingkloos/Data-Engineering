from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.operators.bash_operator import BashOperator

from airflow.utils.trigger_rule import TriggerRule

current_date = str(date.today())
file_name = str(date.today() + timedelta(days=-1))
BUCKET = "gs://spark-etl-1"
INSTANCE_NAME = "bigdata-etl-20201027:us-central1:mysql-instance=tcp:3307"
DEFAULT_DAG_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": "bigdata-etl-20201027",
    "scheduled_interval": "30 2 * * *"  # every day at 2:30 am utc
}

with DAG("sqoop_import_full_table", default_args=DEFAULT_DAG_ARGS) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-2",
        init_actions_uris=['gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh'],
        num_workers=2,
        region="us-central1",
        zone="us-central1-a",
        service_account_scopes=['https://www.googleapis.com/auth/sqlservice.admin'],
        properties={'hive:hive.metastore.warehouse.dir': BUCKET + '/hive-warehouse'},
        metadata={'enable-cloud-sql-hive-metastore': 'false', 'additional-cloud-sql-instances': INSTANCE_NAME},
        image_version='1.5'
    )

    submit_sqoop = BashOperator(
        task_id="sqoop_full_table_import",
        bash_command='bash /home/airflow/gcs/plugins/sqoop_simple_table_imports_for_airflow.sh ephemeral-spark-cluster-{{ds_nodash}}'
    )

    bq_load_flight_delays = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_flight_delays",
        bucket="spark-etl-1",
        source_objects=["sqoop-output/flights/*.avro"],
        destination_project_dataset_table="bigdata-etl-20201027.data_analysis.flight_delays",
        autodetect=True,
        source_format="AVRO",
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

    create_cluster.dag = dag
    create_cluster.set_downstream(submit_sqoop)
    submit_sqoop.set_downstream(bq_load_flight_delays)
    bq_load_flight_delays.set_downstream(delete_cluster)
