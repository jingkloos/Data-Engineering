bucket="gs://spark-etl-1"
pwd_file=$bucket/sqoop-pwd/pwd.txt
cluster_name="ephemeral-spark-cluster-20201115"

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name \
--region="us-central1" \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar,file:///usr/share/java/mysql-connector-java-8.0.23.jar \
-- eval \
-Dmapreduce.job.user.classpath.first=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--query="select * from flights limit 10;"


