from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import json
import threading
import time
import function as fnc

load_dotenv ()

db_conf = {
    "DB_NAME"       : os.getenv('DB_NAME'),
    "DB_USER"       : os.getenv('DB_USER'),
    "DB_PASSWORD"   : os.getenv('DB_PASSWORD'),
    "DB_HOST"       : os.getenv('DB_HOST'),
    "DB_PORT"       : os.getenv('DB_PORT'),
}

tb_name = [
    'tb_aaws',
    'tb_arg',
    'tb_asrs',
    'tb_aws',
    'tb_awsship',
    'tb_iklim_mikro',
    'tb_soil'
]

start_time = time.time()
date_now = datetime.utcnow() - timedelta(minutes=30)
date_delta = date_now - timedelta(minutes=30)

threads_query = []
query_result = [None] * len(tb_name)
for index, row in enumerate(tb_name):
    thread_query = threading.Thread(target=fnc.queryDB, args=(db_conf, date_now, date_delta, row, query_result, index))
    threads_query.append(thread_query)
    thread_query.start()

for thread in threads_query:
    thread.join()

query_time = time.time()

merged_data = []
for i in range(len(query_result)):
    for j in range(len(query_result[i])):
        merged_data.append(query_result[i][j])

f = open('range.json')
range = json.load(f)

print("Jumlah Data =", len(merged_data))
threads_process = []
qc_result = [None] * (len(merged_data)-1)
for index, data in enumerate(merged_data):
    next_index = index + 1
    if next_index < len(merged_data):
        next_data = merged_data[next_index]
        thread_process = threading.Thread(target=fnc.QCProcess, args=(data['id_station'], data['tgl_data'], data['data'], data['type'], next_data['id_station'], next_data['tgl_data'], next_data['data'], next_data['type'], qc_result, index, range))
        threads_process.append(thread_process)
        thread_process.start()

for thread in threads_process:
    thread.join()


with open('QCResult.json', 'w') as output:
    json.dump(qc_result, output)

end_time = time.time()
print('Total Thread =', len(threads_process))
print('Query Time   =', query_time - start_time)
print('QC Time      =', end_time - query_time)
print('Total Time   =', end_time - start_time)