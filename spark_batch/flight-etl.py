from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession \
    .builder \
    .appName("Flight ETL using dataproc") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

today = datetime.now()+timedelta(days=0)
file_name = today.strftime("%Y-%m-%d")
bucket_name = "gs://spark-etl-1"
flights_data = spark.read.json(bucket_name + "/json-files/" + file_name + ".json")


flights_data.registerTempTable("flights_data")


qry = """
    select flight_date,
    flight_num,
    round(avg(arrival_delay),2) as avg_arrival_delay,
    round(avg(departure_delay),2) as avg_departure_delay
    
    from flights_data
    group by flight_date,flight_num
"""
query = """
    select *,
    case when distance between 0 and 500 then 1
         when distance between 501 and 1000 then 2
         when distance between 1001 and 2000 then 3
         when distance between 2001 and 3000 then 4
         when distance between 3001 and 4000 then 5
         when distance between 4001 and 5000 then 6
         else 7
         end as distance_category
    from flights_data
"""

# In[22]:


avg_delays_by_flight_nums = spark.sql(qry)

# In[17]:


flights_data = spark.sql(query)

# In[18]:


flights_data.registerTempTable("flights_data")

# In[19]:


qry = """
    select flight_date,
    distance_category,
    round(avg(arrival_delay),2) as avg_arrival_delay,
    round(avg(departure_delay),2) as avg_departure_delay
    
    from flights_data
    group by flight_date,distance_category
"""

# In[25]:


avg_delays_by_distance_category = spark.sql(qry)

# In[28]:


output_flight_nums = bucket_name + "/flights_data_output/" + file_name + "_flight_nums"
output_distance_category = bucket_name + "/flights_data_output/" + file_name + "_distance_category"
avg_delays_by_flight_nums.coalesce(1).write.mode("overwrite").format("json").save(output_flight_nums)
avg_delays_by_distance_category.coalesce(1).write.mode("overwrite").format("json").save(output_distance_category)

