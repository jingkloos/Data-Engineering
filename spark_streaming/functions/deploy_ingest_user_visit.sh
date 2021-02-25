
FUNCTION="ingest_user_visit"
BUCKET="gs://spark-streaming-etl"

gcloud functions deploy ${FUNCTION} \
    --region us-east1 \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
