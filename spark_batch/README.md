### Project One: Flight data ETL
1. Daily flight raw data are saved to google storage "gs://$bucket/$folder/*.json"
2. Dataproc spark cluster is created for a spark job to run
3. Spark job is to transform flight data to aggregated format and save the result back to gs
4. run big query command to load aggregated data to big query
5. delete dataproc cluster after job is done 
6. automate the above steps using workflow template or airflow

### Project Two: use sqoop to move data from mysql to big query
0. You need to set up a mysql instance on gcp to finish this project
1. Create hadoop cluster using dataproc
2. Submit a hadoop job(one time import or incremental import) to the cluster to export data from mysql and save as avro files
3. Incremental hadoop job can only save data on the local cluster so you need to copy the files to gs using SHH
4. Load avro files to big query
5. automate the above steps using airflow


### Take away
1. Airflow is a better framework to schedule daily ETL job  
2. You can monitor the jobs and set up retries through Airflow  
3. Airflow gives you the ability to manually trigger a failed step in a job 
instead of running the whole job again  
4. Dataproc doesn't need to run all the time especially for batch jobs. They can be spun up in demand to save money  


