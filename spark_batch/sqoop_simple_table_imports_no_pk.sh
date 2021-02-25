bucket="gs://spark-etl-1"
pwd_file=$bucket/sqoop-pwd/pwd.txt
cluster_name="ephemeral-spark-cluster-20201115"
table_name="airports"
target_dir=$bucket/sqoop-output


gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name \
--region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop-jars/sqoop-1.4.7-hadoop260.jar,$bucket/sqoop-jars/avro-tools-1.8.1.jar,file:///usr/share/java/mysql-connector-java-8.0.22.jar \
-- import -Dmapreduce.job.classloader=true \
-Dmapreduce.output.basename='part_20201117_' \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--table $table_name \
-m 1 \
--warehouse-dir $target_dir --as-avrodatafile