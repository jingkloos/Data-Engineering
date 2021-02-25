### Project One: stream ecom website user visits data
1. user visits data generator publish data to kafka topic
2. spark streaming job reads data from the kafka topic and save it to gs every minute
3. spark streaming job reads and aggregate data from the kafka topic and save the result to gs every minute
4. use cloud function to move data from gs bucket to big query

If you choose hive as your data warehouse, then you can build hive tables on top of gs bucket directly
Kafka topic ==> google store ==> big query

### Project Two: IoT data streaming
1. IoT data generator generates energy consumption data of each house every 60 seconds 
and save them in csv files which are saved on local disk and then uploaded to gs bucket
2. IoT_data_transformation.py is a spark job that read data from IoT gs bucket as a stream, calculate the running sum of
 energy consumption during the day and if the overall consumption number of a house is higher than a threshold, those data will be published 
 to a kafka topic and the running sum will also go to big query

csv files ==> gs bucket ==> spark streaming ==> big query/kafka


### Take away
1. storage triggered cloud function monitor at bucket level, in this projects, I had only one stream running at a time
so I used one bucket; In production, multiple streams can run at the same time, you need a bucket for each stream and a
separate function for each bucket
2. For project 2, you can also save the running sum to a cloud bucket
and create a cloud function to monitor the bucket. Since spark supports sink to big query(it also uses a temp bucket), I choose
to let spark do the intermittent step. That means functions/deploy_iot_transformed_data.sh and the related function in functions/main.py is not used 
in this project
 