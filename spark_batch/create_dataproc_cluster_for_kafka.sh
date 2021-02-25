CLUSTER_NAME="ephemeral-kafka-cluster"
REGION="us-central1"
ZONE="us-central1-b"

gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --image-version=1.5 \
  --master-machine-type=n1-standard-1 \
  --master-boot-disk-size 20 \
  --num-workers 2 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size 20 \
  --initialization-actions=gs://dataproc-initialization-actions/zookeeper/zookeeper.sh,gs://dataproc-initialization-actions/kafka/kafka.sh

