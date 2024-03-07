import psycopg2
from psycopg2.extras import RealDictCursor

def queryDB(db_conf, date_now, date_delta, tb_name, query_value, index):
    conn = psycopg2.connect(
        dbname      = db_conf["DB_NAME"],
        user        = db_conf["DB_USER"],
        password    = db_conf["DB_PASSWORD"],
        host        = db_conf["DB_HOST"],
        port        = db_conf["DB_PORT"]
    )

    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(f"SELECT id_station, TO_CHAR(tgl_data, 'YYYY-MM-DD HH24:MI:SS') AS tgl_data, '{tb_name}' AS type, data FROM {tb_name} WHERE tgl_data BETWEEN '{date_delta}' AND '{date_now}' ORDER BY id_station ASC, tgl_data DESC")

    result = cur.fetchall()

    cur.close()
    conn.close()
    query_value[index] = result


def QCParam(param, id_station, data, next_id_station, next_data, range, qc_data):
    if param in data and data[param] != '' and data[param] != '/' and data[param] != '\r\n' and data[param] != 'null' and not data[param].isalpha():
        qc_data['data'][param] = float(data[param])
        qc = []
        if range[param][0] != None and range[param][1] != None:
            if float(data[param]) >= range[param][0] and float(data[param]) <= range[param][1]:
                qc.append(0)
            else:
                qc.append(1)
        else:
            qc.append(0)
        
        if id_station == next_id_station and param in next_data and next_data[param] != '/' and next_data[param] != '' and range[param][2] != None:
            if abs(float(data[param]) - float(next_data[param])) <= range[param][2]:
                qc.append(0)
            else:
                qc.append(1)
        else:
            qc.append(0)

        if qc == [0,0]:
            qc_data['data'][f'{param}_flag'] = 0
        elif qc == [1,0]:
            qc_data['data'][f'{param}_flag'] = 1
        elif qc == [0,1]:
            qc_data['data'][f'{param}_flag'] = 2
        elif qc == [1,1]:
            qc_data['data'][f'{param}_flag'] = 4

    else:
        qc_data['data'][param] = None
        qc_data['data'][f'{param}_flag'] = 9    


def QCProcess(id_station, date, data, type, next_id_station, next_date, next_data, next_type, qc_result, index, range):
    qc_data = {
        "time": date,
        "id_station": id_station,
        "type": type[3:],
        "data": {}
    }

    param_list = list(range.keys())
    for i in param_list:
        QCParam(i, id_station, data, next_id_station, next_data, range, qc_data)

    qc_result[index] = qc_data