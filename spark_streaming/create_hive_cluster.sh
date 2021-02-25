CLUSTER_NAME="hive-cluster"
REGION="us-east1"
ZONE="us-east1-b"
BUCKET_NAME="spark-etl-1"
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --image-version=1.5 \
  --master-machine-type=n1-standard-1 \
  --master-boot-disk-size 20 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size 20 \
  --num-workers 2 \
  --bucket=${BUCKET_NAME}