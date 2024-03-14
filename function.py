from psycopg2.extras import RealDictCursor

def queryDB(db_conf, date_now, date_delta, tb_name, query_value, index):

    conn = db_conf.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(f"SELECT id_station, TO_CHAR(tgl_data, 'YYYY-MM-DD HH24:MI:SS') AS tgl_data, '{tb_name}' AS type, data FROM {tb_name} WHERE tgl_data BETWEEN '{date_delta}' AND '{date_now}' ORDER BY id_station ASC, tgl_data DESC")

    result = cur.fetchall()
    conn.close()

    if conn:
        db_conf.putconn(conn)

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


def QCProcess(db_conf, id_station, date, data, type, next_id_station, next_date, next_data, next_type, qc_result, index, range):
    qc_data = {
        "time": date,
        "id_station": id_station,
        "type": type[3:],
        "data": {}
    }

    param_list = list(range.keys())
    for i in param_list:
        QCParam(i, id_station, data, next_id_station, next_data, range, qc_data)

    conn = db_conf.getconn()
    cursor = conn.cursor()

    sql_query = f"""
        INSERT INTO tb_qc_result VALUES (
            '{qc_data["id_station"]}',
            '{qc_data["type"]}',
            '{qc_data["time"]}',
            {qc_data["data"]["rr"] if qc_data["data"]["rr"] is not None else "null"},
            {qc_data["data"]["rr_flag"] if qc_data["data"]["rr_flag"] is not None else "null"},
            {qc_data["data"]["pp_air"] if qc_data["data"]["pp_air"] is not None else "null"},
            {qc_data["data"]["pp_air_flag"] if qc_data["data"]["pp_air_flag"] is not None else "null"},
            {qc_data["data"]["rh_avg"] if qc_data["data"]["rh_avg"] is not None else "null"},
            {qc_data["data"]["rh_avg_flag"] if qc_data["data"]["rh_avg_flag"] is not None else "null"},
            {qc_data["data"]["sr_avg"] if qc_data["data"]["sr_avg"] is not None else "null"},
            {qc_data["data"]["sr_avg_flag"] if qc_data["data"]["sr_avg_flag"] is not None else "null"},
            {qc_data["data"]["sr_max"] if qc_data["data"]["sr_max"] is not None else "null"},
            {qc_data["data"]["sr_max_flag"] if qc_data["data"]["sr_max_flag"] is not None else "null"},
            {qc_data["data"]["nr"] if qc_data["data"]["nr"] is not None else "null"},
            {qc_data["data"]["nr_flag"] if qc_data["data"]["nr_flag"] is not None else "null"},
            {qc_data["data"]["wd_avg"] if qc_data["data"]["wd_avg"] is not None else "null"},
            {qc_data["data"]["wd_avg_flag"] if qc_data["data"]["wd_avg_flag"] is not None else "null"},
            {qc_data["data"]["ws_avg"] if qc_data["data"]["ws_avg"] is not None else "null"},
            {qc_data["data"]["ws_avg_flag"] if qc_data["data"]["ws_avg_flag"] is not None else "null"},
            {qc_data["data"]["ws_max"] if qc_data["data"]["ws_max"] is not None else "null"},
            {qc_data["data"]["ws_max_flag"] if qc_data["data"]["ws_max_flag"] is not None else "null"},
            {qc_data["data"]["wl"] if qc_data["data"]["wl"] is not None else "null"},
            {qc_data["data"]["wl_flag"] if qc_data["data"]["wl_flag"] is not None else "null"},
            {qc_data["data"]["tt_air_avg"] if qc_data["data"]["tt_air_avg"] is not None else "null"},
            {qc_data["data"]["tt_air_avg_flag"] if qc_data["data"]["tt_air_avg_flag"] is not None else "null"},
            {qc_data["data"]["tt_air_min"] if qc_data["data"]["tt_air_min"] is not None else "null"},
            {qc_data["data"]["tt_air_min_flag"] if qc_data["data"]["tt_air_min_flag"] is not None else "null"},
            {qc_data["data"]["tt_air_max"] if qc_data["data"]["tt_air_max"] is not None else "null"},
            {qc_data["data"]["tt_air_max_flag"] if qc_data["data"]["tt_air_max_flag"] is not None else "null"},
            {qc_data["data"]["tt_sea"] if qc_data["data"]["tt_sea"] is not None else "null"},
            {qc_data["data"]["tt_sea_flag"] if qc_data["data"]["tt_sea_flag"] is not None else "null"},
            {qc_data["data"]["ws_50cm"] if qc_data["data"]["ws_50cm"] is not None else "null"},
            {qc_data["data"]["ws_50cm_flag"] if qc_data["data"]["ws_50cm_flag"] is not None else "null"},
            {qc_data["data"]["wl_pan"] if qc_data["data"]["wl_pan"] is not None else "null"},
            {qc_data["data"]["wl_pan_flag"] if qc_data["data"]["wl_pan_flag"] is not None else "null"},
            {qc_data["data"]["ev_pan"] if qc_data["data"]["ev_pan"] is not None else "null"},
            {qc_data["data"]["ev_pan_flag"] if qc_data["data"]["ev_pan_flag"] is not None else "null"},
            {qc_data["data"]["tt_pan"] if qc_data["data"]["tt_pan"] is not None else "null"},
            {qc_data["data"]["tt_pan_flag"] if qc_data["data"]["tt_pan_flag"] is not None else "null"},
            {qc_data["data"]["konduktivitas"] if qc_data["data"]["konduktivitas"] is not None else "null"},
            {qc_data["data"]["konduktivitas_flag"] if qc_data["data"]["konduktivitas_flag"] is not None else "null"},
            {qc_data["data"]["ph_sea"] if qc_data["data"]["ph_sea"] is not None else "null"},
            {qc_data["data"]["ph_sea_flag"] if qc_data["data"]["ph_sea_flag"] is not None else "null"},
            {qc_data["data"]["par"] if qc_data["data"]["par"] is not None else "null"},
            {qc_data["data"]["par_flag"] if qc_data["data"]["par_flag"] is not None else "null"},
            {qc_data["data"]["ws_2m"] if qc_data["data"]["ws_2m"] is not None else "null"},
            {qc_data["data"]["ws_2m_flag"] if qc_data["data"]["ws_2m_flag"] is not None else "null"},
            {qc_data["data"]["tt_soil_min"] if qc_data["data"]["tt_soil_min"] is not None else "null"},
            {qc_data["data"]["tt_soil_min_flag"] if qc_data["data"]["tt_soil_min_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_m10"] if qc_data["data"]["tt_bs_m10"] is not None else "null"},
            {qc_data["data"]["tt_bs_m10_flag"] if qc_data["data"]["tt_bs_m10_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_0"] if qc_data["data"]["tt_bs_0"] is not None else "null"},
            {qc_data["data"]["tt_bs_0_flag"] if qc_data["data"]["tt_bs_0_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_2"] if qc_data["data"]["tt_bs_2"] is not None else "null"},
            {qc_data["data"]["tt_bs_2_flag"] if qc_data["data"]["tt_bs_2_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_5"] if qc_data["data"]["tt_bs_5"] is not None else "null"},
            {qc_data["data"]["tt_bs_5_flag"] if qc_data["data"]["tt_bs_5_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_10"] if qc_data["data"]["tt_bs_10"] is not None else "null"},
            {qc_data["data"]["tt_bs_10_flag"] if qc_data["data"]["tt_bs_10_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_20"] if qc_data["data"]["tt_bs_20"] is not None else "null"},
            {qc_data["data"]["tt_bs_20_flag"] if qc_data["data"]["tt_bs_20_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_50"] if qc_data["data"]["tt_bs_50"] is not None else "null"},
            {qc_data["data"]["tt_bs_50_flag"] if qc_data["data"]["tt_bs_50_flag"] is not None else "null"},
            {qc_data["data"]["tt_bs_100"] if qc_data["data"]["tt_bs_100"] is not None else "null"},
            {qc_data["data"]["tt_bs_100_flag"] if qc_data["data"]["tt_bs_100_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_m10"] if qc_data["data"]["tt_ts_m10"] is not None else "null"},
            {qc_data["data"]["tt_ts_m10_flag"] if qc_data["data"]["tt_ts_m10_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_0"] if qc_data["data"]["tt_ts_0"] is not None else "null"},
            {qc_data["data"]["tt_ts_0_flag"] if qc_data["data"]["tt_ts_0_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_2"] if qc_data["data"]["tt_ts_2"] is not None else "null"},
            {qc_data["data"]["tt_ts_2_flag"] if qc_data["data"]["tt_ts_2_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_5"] if qc_data["data"]["tt_ts_5"] is not None else "null"},
            {qc_data["data"]["tt_ts_5_flag"] if qc_data["data"]["tt_ts_5_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_10"] if qc_data["data"]["tt_ts_10"] is not None else "null"},
            {qc_data["data"]["tt_ts_10_flag"] if qc_data["data"]["tt_ts_10_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_20"] if qc_data["data"]["tt_ts_20"] is not None else "null"},
            {qc_data["data"]["tt_ts_20_flag"] if qc_data["data"]["tt_ts_20_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_50"] if qc_data["data"]["tt_ts_50"] is not None else "null"},
            {qc_data["data"]["tt_ts_50_flag"] if qc_data["data"]["tt_ts_50_flag"] is not None else "null"},
            {qc_data["data"]["tt_ts_100"] if qc_data["data"]["tt_ts_100"] is not None else "null"},
            {qc_data["data"]["tt_ts_100_flag"] if qc_data["data"]["tt_ts_100_flag"] is not None else "null"},
            {qc_data["data"]["sm_10"] if qc_data["data"]["sm_10"] is not None else "null"},
            {qc_data["data"]["sm_10_flag"] if qc_data["data"]["sm_10_flag"] is not None else "null"},
            {qc_data["data"]["sm_20"] if qc_data["data"]["sm_20"] is not None else "null"},
            {qc_data["data"]["sm_20_flag"] if qc_data["data"]["sm_20_flag"] is not None else "null"},
            {qc_data["data"]["sm_30"] if qc_data["data"]["sm_30"] is not None else "null"},
            {qc_data["data"]["sm_30_flag"] if qc_data["data"]["sm_30_flag"] is not None else "null"},
            {qc_data["data"]["sm_40"] if qc_data["data"]["sm_40"] is not None else "null"},
            {qc_data["data"]["sm_40_flag"] if qc_data["data"]["sm_40_flag"] is not None else "null"},
            {qc_data["data"]["sm_60"] if qc_data["data"]["sm_60"] is not None else "null"},
            {qc_data["data"]["sm_60_flag"] if qc_data["data"]["sm_60_flag"] is not None else "null"},
            {qc_data["data"]["sm_100"] if qc_data["data"]["sm_100"] is not None else "null"},
            {qc_data["data"]["sm_100_flag"] if qc_data["data"]["sm_100_flag"] is not None else "null"},
            {qc_data["data"]["diffuse_rad_round"] if qc_data["data"]["diffuse_rad_round"] is not None else "null"},
            {qc_data["data"]["diffuse_rad_round_flag"] if qc_data["data"]["diffuse_rad_round_flag"] is not None else "null"},
            {qc_data["data"]["dni_rad_round"] if qc_data["data"]["dni_rad_round"] is not None else "null"},
            {qc_data["data"]["dni_rad_round_flag"] if qc_data["data"]["dni_rad_round_flag"] is not None else "null"},
            {qc_data["data"]["global_rad_round"] if qc_data["data"]["global_rad_round"] is not None else "null"},
            {qc_data["data"]["global_rad_round_flag"] if qc_data["data"]["global_rad_round_flag"] is not None else "null"},
            {qc_data["data"]["reflected_rad_round"] if qc_data["data"]["reflected_rad_round"] is not None else "null"},
            {qc_data["data"]["reflected_rad_round_flag"] if qc_data["data"]["reflected_rad_round_flag"] is not None else "null"},
            {qc_data["data"]["nett_rad_round"] if qc_data["data"]["nett_rad_round"] is not None else "null"},
            {qc_data["data"]["nett_rad_round_flag"] if qc_data["data"]["nett_rad_round_flag"] is not None else "null"},
            {qc_data["data"]["sunshine_minutes"] if qc_data["data"]["sunshine_minutes"] is not None else "null"},
            {qc_data["data"]["sunshine_minutes_flag"] if qc_data["data"]["sunshine_minutes_flag"] is not None else "null"},
            {qc_data["data"]["sundir_altitude"] if qc_data["data"]["sundir_altitude"] is not None else "null"},
            {qc_data["data"]["sundir_altitude_flag"] if qc_data["data"]["sundir_altitude_flag"] is not None else "null"},
            {qc_data["data"]["sundir_azimuth"] if qc_data["data"]["sundir_azimuth"] is not None else "null"},
            {qc_data["data"]["sundir_azimuth_flag"] if qc_data["data"]["sundir_azimuth_flag"] is not None else "null"},
            {qc_data["data"]["ir"] if qc_data["data"]["ir"] is not None else "null"},
            {qc_data["data"]["ir_flag"] if qc_data["data"]["ir_flag"] is not None else "null"},
            {qc_data["data"]["global_tracker"] if qc_data["data"]["global_tracker"] is not None else "null"},
            {qc_data["data"]["global_tracker_flag"] if qc_data["data"]["global_tracker_flag"] is not None else "null"},
            {qc_data["data"]["uv_a"] if qc_data["data"]["uv_a"] is not None else "null"},
            {qc_data["data"]["uv_a_flag"] if qc_data["data"]["uv_a_flag"] is not None else "null"},
            {qc_data["data"]["uv_b"] if qc_data["data"]["uv_b"] is not None else "null"},
            {qc_data["data"]["uv_b_flag"] if qc_data["data"]["uv_b_flag"] is not None else "null"},
            {qc_data["data"]["tt_min_4m"] if qc_data["data"]["tt_min_4m"] is not None else "null"},
            {qc_data["data"]["tt_min_4m_flag"] if qc_data["data"]["tt_min_4m_flag"] is not None else "null"},
            {qc_data["data"]["tt_avg_4m"] if qc_data["data"]["tt_avg_4m"] is not None else "null"},
            {qc_data["data"]["tt_avg_4m_flag"] if qc_data["data"]["tt_avg_4m_flag"] is not None else "null"},
            {qc_data["data"]["tt_max_4m"] if qc_data["data"]["tt_max_4m"] is not None else "null"},
            {qc_data["data"]["tt_max_4m_flag"] if qc_data["data"]["tt_max_4m_flag"] is not None else "null"},
            {qc_data["data"]["rh_min_4m"] if qc_data["data"]["rh_min_4m"] is not None else "null"},
            {qc_data["data"]["rh_min_4m_flag"] if qc_data["data"]["rh_min_4m_flag"] is not None else "null"},
            {qc_data["data"]["rh_avg_4m"] if qc_data["data"]["rh_avg_4m"] is not None else "null"},
            {qc_data["data"]["rh_avg_4m_flag"] if qc_data["data"]["rh_avg_4m_flag"] is not None else "null"},
            {qc_data["data"]["rh_max_4m"] if qc_data["data"]["rh_max_4m"] is not None else "null"},
            {qc_data["data"]["rh_max_4m_flag"] if qc_data["data"]["rh_max_4m_flag"] is not None else "null"},
            {qc_data["data"]["ws_min_4m"] if qc_data["data"]["ws_min_4m"] is not None else "null"},
            {qc_data["data"]["ws_min_4m_flag"] if qc_data["data"]["ws_min_4m_flag"] is not None else "null"},
            {qc_data["data"]["ws_avg_4m"] if qc_data["data"]["ws_avg_4m"] is not None else "null"},
            {qc_data["data"]["ws_avg_4m_flag"] if qc_data["data"]["ws_avg_4m_flag"] is not None else "null"},
            {qc_data["data"]["ws_max_4m"] if qc_data["data"]["ws_max_4m"] is not None else "null"},
            {qc_data["data"]["ws_max_4m_flag"] if qc_data["data"]["ws_max_4m_flag"] is not None else "null"},
            {qc_data["data"]["wd_avg_4m"] if qc_data["data"]["wd_avg_4m"] is not None else "null"},
            {qc_data["data"]["wd_avg_4m_flag"] if qc_data["data"]["wd_avg_4m_flag"] is not None else "null"},
            {qc_data["data"]["tt_min_7m"] if qc_data["data"]["tt_min_7m"] is not None else "null"},
            {qc_data["data"]["tt_min_7m_flag"] if qc_data["data"]["tt_min_7m_flag"] is not None else "null"},
            {qc_data["data"]["tt_avg_7m"] if qc_data["data"]["tt_avg_7m"] is not None else "null"},
            {qc_data["data"]["tt_avg_7m_flag"] if qc_data["data"]["tt_avg_7m_flag"] is not None else "null"},
            {qc_data["data"]["tt_max_7m"] if qc_data["data"]["tt_max_7m"] is not None else "null"},
            {qc_data["data"]["tt_max_7m_flag"] if qc_data["data"]["tt_max_7m_flag"] is not None else "null"},
            {qc_data["data"]["rh_min_7m"] if qc_data["data"]["rh_min_7m"] is not None else "null"},
            {qc_data["data"]["rh_min_7m_flag"] if qc_data["data"]["rh_min_7m_flag"] is not None else "null"},
            {qc_data["data"]["rh_avg_7m"] if qc_data["data"]["rh_avg_7m"] is not None else "null"},
            {qc_data["data"]["rh_avg_7m_flag"] if qc_data["data"]["rh_avg_7m_flag"] is not None else "null"},
            {qc_data["data"]["rh_max_7m"] if qc_data["data"]["rh_max_7m"] is not None else "null"},
            {qc_data["data"]["rh_max_7m_flag"] if qc_data["data"]["rh_max_7m_flag"] is not None else "null"},
            {qc_data["data"]["ws_min_7m"] if qc_data["data"]["ws_min_7m"] is not None else "null"},
            {qc_data["data"]["ws_min_7m_flag"] if qc_data["data"]["ws_min_7m_flag"] is not None else "null"},
            {qc_data["data"]["ws_avg_7m"] if qc_data["data"]["ws_avg_7m"] is not None else "null"},
            {qc_data["data"]["ws_avg_7m_flag"] if qc_data["data"]["ws_avg_7m_flag"] is not None else "null"},
            {qc_data["data"]["ws_max_7m"] if qc_data["data"]["ws_max_7m"] is not None else "null"},
            {qc_data["data"]["ws_max_7m_flag"] if qc_data["data"]["ws_max_7m_flag"] is not None else "null"},
            {qc_data["data"]["wd_avg_7m"] if qc_data["data"]["wd_avg_7m"] is not None else "null"},
            {qc_data["data"]["wd_avg_7m_flag"] if qc_data["data"]["wd_avg_7m_flag"] is not None else "null"},
            {qc_data["data"]["tt_min_10m"] if qc_data["data"]["tt_min_10m"] is not None else "null"},
            {qc_data["data"]["tt_min_10m_flag"] if qc_data["data"]["tt_min_10m_flag"] is not None else "null"},
            {qc_data["data"]["tt_avg_10m"] if qc_data["data"]["tt_avg_10m"] is not None else "null"},
            {qc_data["data"]["tt_avg_10m_flag"] if qc_data["data"]["tt_avg_10m_flag"] is not None else "null"},
            {qc_data["data"]["tt_max_10m"] if qc_data["data"]["tt_max_10m"] is not None else "null"},
            {qc_data["data"]["tt_max_10m_flag"] if qc_data["data"]["tt_max_10m_flag"] is not None else "null"},
            {qc_data["data"]["rh_min_10m"] if qc_data["data"]["rh_min_10m"] is not None else "null"},
            {qc_data["data"]["rh_min_10m_flag"] if qc_data["data"]["rh_min_10m_flag"] is not None else "null"},
            {qc_data["data"]["rh_avg_10m"] if qc_data["data"]["rh_avg_10m"] is not None else "null"},
            {qc_data["data"]["rh_avg_10m_flag"] if qc_data["data"]["rh_avg_10m_flag"] is not None else "null"},
            {qc_data["data"]["rh_max_10m"] if qc_data["data"]["rh_max_10m"] is not None else "null"},
            {qc_data["data"]["rh_max_10m_flag"] if qc_data["data"]["rh_max_10m_flag"] is not None else "null"},
            {qc_data["data"]["ws_min_10m"] if qc_data["data"]["ws_min_10m"] is not None else "null"},
            {qc_data["data"]["ws_min_10m_flag"] if qc_data["data"]["ws_min_10m_flag"] is not None else "null"},
            {qc_data["data"]["ws_avg_10m"] if qc_data["data"]["ws_avg_10m"] is not None else "null"},
            {qc_data["data"]["ws_avg_10m_flag"] if qc_data["data"]["ws_avg_10m_flag"] is not None else "null"},
            {qc_data["data"]["ws_max_10m"] if qc_data["data"]["ws_max_10m"] is not None else "null"},
            {qc_data["data"]["ws_max_10m_flag"] if qc_data["data"]["ws_max_10m_flag"] is not None else "null"},
            {qc_data["data"]["wd_avg_10m"] if qc_data["data"]["wd_avg_10m"] is not None else "null"},
            {qc_data["data"]["wd_avg_10m_flag"] if qc_data["data"]["wd_avg_10m_flag"] is not None else "null"},
            {qc_data["data"]["tt_4m"] if qc_data["data"]["tt_4m"] is not None else "null"},
            {qc_data["data"]["tt_4m_flag"] if qc_data["data"]["tt_4m_flag"] is not None else "null"},
            {qc_data["data"]["rh_4m"] if qc_data["data"]["rh_4m"] is not None else "null"},
            {qc_data["data"]["rh_4m_flag"] if qc_data["data"]["rh_4m_flag"] is not None else "null"},
            {qc_data["data"]["ws_4m"] if qc_data["data"]["ws_4m"] is not None else "null"},
            {qc_data["data"]["ws_4m_flag"] if qc_data["data"]["ws_4m_flag"] is not None else "null"},
            {qc_data["data"]["wd_4m"] if qc_data["data"]["wd_4m"] is not None else "null"},
            {qc_data["data"]["wd_4m_flag"] if qc_data["data"]["wd_4m_flag"] is not None else "null"},
            {qc_data["data"]["tt_7m"] if qc_data["data"]["tt_7m"] is not None else "null"},
            {qc_data["data"]["tt_7m_flag"] if qc_data["data"]["tt_7m_flag"] is not None else "null"},
            {qc_data["data"]["rh_7m"] if qc_data["data"]["rh_7m"] is not None else "null"},
            {qc_data["data"]["rh_7m_flag"] if qc_data["data"]["rh_7m_flag"] is not None else "null"},
            {qc_data["data"]["ws_7m"] if qc_data["data"]["ws_7m"] is not None else "null"},
            {qc_data["data"]["ws_7m_flag"] if qc_data["data"]["ws_7m_flag"] is not None else "null"},
            {qc_data["data"]["wd_7m"] if qc_data["data"]["wd_7m"] is not None else "null"},
            {qc_data["data"]["wd_7m_flag"] if qc_data["data"]["wd_7m_flag"] is not None else "null"},
            {qc_data["data"]["tt_10m"] if qc_data["data"]["tt_10m"] is not None else "null"},
            {qc_data["data"]["tt_10m_flag"] if qc_data["data"]["tt_10m_flag"] is not None else "null"},
            {qc_data["data"]["rh_10m"] if qc_data["data"]["rh_10m"] is not None else "null"},
            {qc_data["data"]["rh_10m_flag"] if qc_data["data"]["rh_10m_flag"] is not None else "null"},
            {qc_data["data"]["ws_10m"] if qc_data["data"]["ws_10m"] is not None else "null"},
            {qc_data["data"]["ws_10m_flag"] if qc_data["data"]["ws_10m_flag"] is not None else "null"},
            {qc_data["data"]["wd_10m"] if qc_data["data"]["wd_10m"] is not None else "null"},
            {qc_data["data"]["wd_10m_flag"] if qc_data["data"]["wd_10m_flag"] is not None else "null"},
            {qc_data["data"]["rain"] if qc_data["data"]["rain"] is not None else "null"},
            {qc_data["data"]["rain_flag"] if qc_data["data"]["rain_flag"] is not None else "null"},
            {qc_data["data"]["rh"] if qc_data["data"]["rh"] is not None else "null"},
            {qc_data["data"]["rh_flag"] if qc_data["data"]["rh_flag"] is not None else "null"},
            {qc_data["data"]["hail"] if qc_data["data"]["hail"] is not None else "null"},
            {qc_data["data"]["hail_flag"] if qc_data["data"]["hail_flag"] is not None else "null"},
            {qc_data["data"]["temp"] if qc_data["data"]["temp"] is not None else "null"},
            {qc_data["data"]["temp_flag"] if qc_data["data"]["temp_flag"] is not None else "null"},
            {qc_data["data"]["speed"] if qc_data["data"]["speed"] is not None else "null"},
            {qc_data["data"]["speed_flag"] if qc_data["data"]["speed_flag"] is not None else "null"},
            {qc_data["data"]["wd_max"] if qc_data["data"]["wd_max"] is not None else "null"},
            {qc_data["data"]["wd_max_flag"] if qc_data["data"]["wd_max_flag"] is not None else "null"},
            {qc_data["data"]["ws_mean"] if qc_data["data"]["ws_mean"] is not None else "null"},
            {qc_data["data"]["ws_mean_flag"] if qc_data["data"]["ws_mean_flag"] is not None else "null"},
            {qc_data["data"]["dewpoint"] if qc_data["data"]["dewpoint"] is not None else "null"},
            {qc_data["data"]["dewpoint_flag"] if qc_data["data"]["dewpoint_flag"] is not None else "null"},
            {qc_data["data"]["pressure"] if qc_data["data"]["pressure"] is not None else "null"},
            {qc_data["data"]["pressure_flag"] if qc_data["data"]["pressure_flag"] is not None else "null"},
            {qc_data["data"]["wd_max10"] if qc_data["data"]["wd_max10"] is not None else "null"},
            {qc_data["data"]["wd_max10_flag"] if qc_data["data"]["wd_max10_flag"] is not None else "null"},
            {qc_data["data"]["wd_min10"] if qc_data["data"]["wd_min10"] is not None else "null"},
            {qc_data["data"]["wd_min10_flag"] if qc_data["data"]["wd_min10_flag"] is not None else "null"},
            {qc_data["data"]["ws_max10"] if qc_data["data"]["ws_max10"] is not None else "null"},
            {qc_data["data"]["ws_max10_flag"] if qc_data["data"]["ws_max10_flag"] is not None else "null"},
            {qc_data["data"]["ws_min10"] if qc_data["data"]["ws_min10"] is not None else "null"},
            {qc_data["data"]["ws_min10_flag"] if qc_data["data"]["ws_min10_flag"] is not None else "null"},
            {qc_data["data"]["c_decline"] if qc_data["data"]["c_decline"] is not None else "null"},
            {qc_data["data"]["c_decline_flag"] if qc_data["data"]["c_decline_flag"] is not None else "null"},
            {qc_data["data"]["wd_mean10"] if qc_data["data"]["wd_mean10"] is not None else "null"},
            {qc_data["data"]["wd_mean10_flag"] if qc_data["data"]["wd_mean10_flag"] is not None else "null"},
            {qc_data["data"]["ws_mean10"] if qc_data["data"]["ws_mean10"] is not None else "null"},
            {qc_data["data"]["ws_mean10_flag"] if qc_data["data"]["ws_mean10_flag"] is not None else "null"},
            {qc_data["data"]["c_directions"] if qc_data["data"]["c_directions"] is not None else "null"},
            {qc_data["data"]["c_directions_flag"] if qc_data["data"]["c_directions_flag"] is not None else "null"},
            {qc_data["data"]["pressure_msl"] if qc_data["data"]["pressure_msl"] is not None else "null"},
            {qc_data["data"]["pressure_msl_flag"] if qc_data["data"]["pressure_msl_flag"] is not None else "null"}
        )
    """

    cursor.execute(sql_query)
    conn.commit()
    conn.close()

    if conn:
        db_conf.putconn(conn)

    qc_result[index] = qc_data