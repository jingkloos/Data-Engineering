import random
from google.cloud import storage
import time
from datetime import datetime, timedelta
import csv


def upload_to_bucket(blob_name, path_to_file, bucket_name):
    storage_client = storage.Client.from_service_account_json('creds.json')
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url



appliances = {'dishwasher': (0.0, 4.0), 'furnace_1': (0.3, 1.2), 'furnace_2': (0.2, 0.7), 'fridge': (0.5, 1.7),
              'wine_cellar': (0.2, 0.5), 'microwave': (0.02, 1.0)}
appl_keys = list(appliances.keys())
header = ['event_time', 'house_id', 'overall_consumption'] + [key + '_consumption' for key in appl_keys]
flag = True

while flag:
    data = []
    for i in range(100):
        otime = datetime.now()
        increment = timedelta(seconds=random.randint(30, 300))
        otime += increment
        event_time = otime.strftime('%Y-%m-%d %H:%M:%S')
        house_uid = random.randint(1000, 100000)
        energy_list = [random.uniform(*appliances[key]) for key in appl_keys]
        data_row = [event_time, house_uid, sum(energy_list)] + energy_list
        data.append(data_row)

    timestr = time.strftime('%Y%m%d%H%M%S')
    file_name = 'server_logs_' + timestr + '.csv'
    path_to_file = 'csv-files/' + file_name
    with open(path_to_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(header)
        writer.writerows(data)

    blob_name = 'IoT_files/' + file_name
    bucket_name = 'spark-streaming-etl'
    upload_to_bucket(blob_name, path_to_file, bucket_name)
    time.sleep(60)
