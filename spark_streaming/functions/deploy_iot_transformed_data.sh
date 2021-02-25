
FUNCTION="iot_transformed_data"
BUCKET="gs://spark-to-bq-temp"

gcloud functions deploy ${FUNCTION} \
    --region us-east1 \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
