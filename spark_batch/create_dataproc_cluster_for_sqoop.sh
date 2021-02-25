CLUSTER_NAME="ephemeral-spark-cluster-20201115"
INSTANCE_NAME="bigdata-etl-20201027:us-central1:mysql-instance"
REGION="us-central1"
ZONE="us-central1-a"
BUCKET_NAME="spark-etl-1"
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --scopes=default,sql-admin \
  --image-version=1.5 \
  --master-machine-type=n1-standard-1 \
  --master-boot-disk-size 20 \
  --num-workers 2 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size 20 \
  --initialization-actions=gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
  --properties=hive:hive.metastore.warehouse.dir=gs://${BUCKET_NAME}/hive-warehouse \
  --metadata=enable-cloud-sql-hive-metastore=false \
  --metadata=additional-cloud-sql-instances=${INSTANCE_NAME}=tcp:3307
