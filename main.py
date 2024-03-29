from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2 import pool
import os
import json
import threading
import time
import function as fnc

load_dotenv ()

tb_name = [
    'tb_aaws',
    'tb_arg',
    'tb_asrs',
    'tb_aws',
    'tb_awsship',
    'tb_iklim_mikro',
    'tb_soil'
]

date_now = datetime.utcnow() - timedelta(minutes=30)
date_delta = date_now - timedelta(minutes=30)

db_conf = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=100,
    dbname      = os.getenv('DB_NAME'),
    user        = os.getenv('DB_USER'),
    password    = os.getenv('DB_PASSWORD'),
    host        = os.getenv('DB_HOST'),
    port        = os.getenv('DB_PORT'),
)

start_query_time = time.time()
threads_query = []
query_result = [None] * len(tb_name)
for index, row in enumerate(tb_name):
    thread_query = threading.Thread(target=fnc.queryDB, args=(db_conf, date_now, date_delta, row, query_result, index))
    threads_query.append(thread_query)
    thread_query.start()

for thread in threads_query:
    thread.join()

end_query_time = time.time()

db_conf.closeall()

merged_data = []
for i in range(len(query_result)):
    for j in range(len(query_result[i])):
        merged_data.append(query_result[i][j])

f = open('range.json')
range = json.load(f)

merged_data = merged_data[:4000]

db_conf_test = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5000,
    dbname      = os.getenv('DB_NAME_TEST'),
    user        = os.getenv('DB_USER_TEST'),
    password    = os.getenv('DB_PASSWORD_TEST'),
    host        = os.getenv('DB_HOST_TEST'),
    port        = os.getenv('DB_PORT_TEST'),
)

print("Jumlah Data =", len(merged_data))
start_qc_time = time.time()
threads_process = []
qc_result = [None] * (len(merged_data)-1)
for index, data in enumerate(merged_data):
    next_index = index + 1
    if next_index < len(merged_data):
        next_data = merged_data[next_index]
        thread_process = threading.Thread(target=fnc.QCProcess, args=(db_conf_test, data['id_station'], data['tgl_data'], data['data'], data['type'], next_data['id_station'], next_data['tgl_data'], next_data['data'], next_data['type'], qc_result, index, range))
        threads_process.append(thread_process)
        thread_process.start()
    if index == len(merged_data)//2:
        time.sleep(5)

for thread in threads_process:
    thread.join()

end_qc_time = time.time()

db_conf_test.closeall()

with open('QCResult.json', 'w') as output:
    json.dump(qc_result, output)

print('Query Time   =', end_query_time - start_query_time)
print('QC Time      =', end_qc_time - start_qc_time)