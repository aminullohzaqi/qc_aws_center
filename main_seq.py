from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2 import pool
import os
import json
import time
import function as fnc

load_dotenv ()

db_conf = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=100,
    dbname      = os.getenv('DB_NAME'),
    user        = os.getenv('DB_USER'),
    password    = os.getenv('DB_PASSWORD'),
    host        = os.getenv('DB_HOST'),
    port        = os.getenv('DB_PORT'),
)

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

query_result = [None] * len(tb_name)
for index, row in enumerate(tb_name):
    fnc.queryDB(db_conf, date_now, date_delta, row, query_result, index)

query_time = time.time()

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
qc_result = [None] * (len(merged_data)-1)
for index, data in enumerate(merged_data):
    next_index = index + 1
    if next_index < len(merged_data):
        next_data = merged_data[next_index]
        fnc.QCProcess(db_conf_test, data['id_station'], data['tgl_data'], data['data'], data['type'], next_data['id_station'], next_data['tgl_data'], next_data['data'], next_data['type'], qc_result, index, range)

db_conf_test.closeall()

with open('QCResult.json', 'w') as output:
    json.dump(qc_result, output)

end_time = time.time()
print('Query Time   =', query_time - start_time)
print('QC Time      =', end_time - query_time)
print('Total Time   =', end_time - start_time)