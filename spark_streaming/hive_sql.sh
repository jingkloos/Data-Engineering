gcloud dataproc jobs submit hive \
--cluster hive-cluster --region us-east1 \
--execute "
  create external table user_server_logs
  (category string, date_time timestamp, type string, pid integer, state string, sub_cat string, ip_address string)
  stored as parquet
  location 'gs://spark-streaming-etl/streaming_raw_output';
"

gcloud dataproc jobs submit hive \
--cluster hive-cluster --region us-east1 \
--execute "
  select * from user_server_logs limit 10
"

gcloud dataproc jobs submit hive \
--cluster hive-cluster --region us-east1 \
--execute "
  select state,collect_set(pid) as products_added
  from user_server_logs
  where pid is not null
  group by state
  limit 10
"