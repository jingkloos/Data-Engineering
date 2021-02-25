gcloud compute ssh jingjingkloos@spark-streaming-etl-m --command="mkdir ~/kafka-producer" --zone us-central1-c
gcloud compute scp ecom-visitor-logs.py jingjingkloos@spark-streaming-etl-m:~/kafka-producer --zone us-central1-c

gcloud compute scp requirements.txt jingjingkloos@spark-streaming-etl-m:~/kafka-producer --zone us-central1-c

gcloud compute ssh jingjingkloos@spark-streaming-etl-m --command="sudo pip install -r ~/kafka-producer/requirements.txt" --zone us-central1-c


gcloud beta compute ssh --zone "us-central1-c" "spark-streaming-etl-m" --project "bigdata-etl-20201027"

#once in the master node
sudo python ~/kafka-producer/ecom-visitor-logs.py
