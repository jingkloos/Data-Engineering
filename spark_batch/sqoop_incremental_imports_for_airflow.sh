bucket="gs://spark-etl-1"
pwd_file=$bucket/sqoop-pwd/pwd.txt
cluster_name=$1
table_name="flights"
target_dir=/incremental_appends #local folder on dataproc cluster
last_value=$2
current_date=$3

gcloud dataproc jobs submit hadoop \
  --cluster="$cluster_name" \
  --region="us-central1" \
  --class=org.apache.sqoop.Sqoop \
  --jars=$bucket/sqoop-jars/sqoop-1.4.7-hadoop260.jar,$bucket/sqoop-jars/avro-tools-1.8.1.jar,file:///usr/share/java/mysql-connector-java-8.0.22.jar \
  -- import -Dmapreduce.job.classloader=true \
  -Dmapreduce.output.basename=part-"${current_date}" \
  --connect="jdbc:mysql://localhost:3307/airports" \
  --username=root --password-file=$pwd_file \
  --split-by id \
  --table $table_name \
  --check-column flight_date \
  --last-value "$last_value" \
  --incremental append \
  -m 4 \
  --target-dir $target_dir --as-avrodatafile
