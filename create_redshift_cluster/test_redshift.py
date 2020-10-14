import psycopg2 
from configparser import ConfigParser


config=ConfigParser()
config.read_file(open('cluster.conf'))
dbname = config.get('DW','DW_DB')
port = config.get('DW','DW_PORT')
user = config.get('DW','DW_DB_USER')
password = config.get('DW','DW_DB_PASSWORD')
host='redshift-cluster-a.cyppqmq6rib6.us-west-1.redshift.amazonaws.com'

conn=psycopg2.connect(dbname= dbname, host=host, port= port, user= user, password= password)

cur=conn.cursor()
cur.execute('drop table if exists test;')
cur.execute('create table if not exists test (id int);')
cur.execute('insert into test values(10);')
cur.execute('select * from test;')
print(cur.fetchone())
conn.commit()
conn.close()
