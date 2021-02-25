CLUSTER_NAME="spark-streaming-etl"
REGION="us-central1"
ZONE="us-central1-c"
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --image-version=1.5 \
  --master-machine-type=n1-standard-1 \
  --master-boot-disk-size 20 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size 20 \
  --num-workers 2 \
  --metadata='PIP_PACKAGES=kafka-python' \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh,gs://dataproc-initialization-actions/zookeeper/zookeeper.sh,gs://dataproc-initialization-actions/kafka/kafka.sh
