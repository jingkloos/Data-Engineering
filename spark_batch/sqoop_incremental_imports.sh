bucket="gs://spark-etl-1"
pwd_file=$bucket/sqoop-pwd/pwd.txt
cluster_name="ephemeral-spark-cluster-20201115"
table_name="flights"
target_dir=/incremental_appends  #local folder on dataproc cluster

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name \
--region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop-jars/sqoop-1.4.7-hadoop260.jar,$bucket/sqoop-jars/avro-tools-1.8.1.jar,file:///usr/share/java/mysql-connector-java-8.0.22.jar \
-- import -Dmapreduce.job.classloader=true \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--split-by id \
--table $table_name \
--check-column flight_date \
--last-value 2019-05-14 \
--incremental append \
-m 4 \
--target-dir $target_dir --as-avrodatafile

#move files on dataproc cluster to gcp bucket
gcloud beta compute ssh --zone "us-central1-a" "ephemeral-spark-cluster-20201115-m" \
-- -T 'hadoop distcp /incremental_appends/*.avro gs://spark-etl-1/sqoop-output/'