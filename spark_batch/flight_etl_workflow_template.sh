template_name="flight_etl"
cluster_name="spark-job"
bucket=gs://spark-etl-1
current_date=$(date +"%Y-%m-%d")
gcloud beta dataproc workflow-templates delete -q ${template_name} --region="us-central1" &&
gcloud beta dataproc workflow-templates create ${template_name} --region="us-central1" &&

gcloud beta dataproc workflow-templates set-managed-cluster ${template_name} \
--region="us-central1" \
--zone="us-central1-a" \
--cluster-name=${cluster_name} \
--scopes=default \
--image-version=1.5 \
--master-machine-type=n1-standard-2 \
--master-boot-disk-size=20 \
--worker-machine-type=n1-standard-2 \
--worker-boot-disk-size=20 &&


gcloud dataproc workflow-templates \
add-job pyspark $bucket/spark-job/flight-etl.py \
--step-id=flight_delay_etl \
--workflow-template=${template_name} \
--region="us-central1" &&


gcloud beta dataproc workflow-templates instantiate ${template_name} --region="us-central1" &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
    data_analysis.avg_delays_by_distance_category \
    $bucket/flights_data_output/${current_date}"_distance_category/*.json" &&


bq load --source_format=NEWLINE_DELIMITED_JSON \
    data_analysis.avg_delays_by_flight_nums \
    $bucket/flights_data_output/${current_date}"_flight_nums/*.json"
