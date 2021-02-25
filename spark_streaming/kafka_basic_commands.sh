#Define a Variable cluster_name that will hold the name of the cluster
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name) 

#Create a new topic - 
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 \
--create \
--replication-factor 2 --partitions 2 --topic test-topic

#List all the topics in Kafka
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list 

#Delete a specific Topic 
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic user_browsing_logs


# Test kafka producer and consumer 
for i in {0..100}; do echo "message${i}"; sleep 1; done | \
    /usr/lib/kafka/bin/kafka-console-producer.sh \
    --broker-list ${CLUSTER_NAME}-w-0:9092 --topic test-topic 



/usr/lib/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
    --topic user_browsing_logs --from-beginning