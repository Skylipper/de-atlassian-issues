import json
from datetime import datetime

import src.utils.variables as var
from src.utils import http_requests_util, dwh_util
from src.utils.atlassian_util import get_atl_connection_info, get_atl_headers


def get_fields(log):
    conn_info = get_atl_connection_info()
    url = f"{conn_info.host}/{var.API_FIELDS_PATH}"

    payload = {}
    headers = get_atl_headers(conn_info)

    response = http_requests_util.execute_request("GET", url, headers, payload)

    return response


def load_fields(log):
    # Грузим одной транзакцией
    fields_json = get_fields(log)
    update_ts = datetime.now()
    conn = dwh_util.get_dwh_connection()
    with conn:
        cur = conn.cursor()
        for field in fields_json:
            log.info(f"Loading field {field['name']}")
            object_id = field['id']
            object_value = json.dumps(field)
            dwh_util.insert_stg_data(cur, var.STG_FIELDS_TABLE_NAME, object_id, object_value, update_ts.isoformat())
        dwh_util.update_last_loaded_ts(cur, var.STG_WF_TABLE_NAME, var.STG_FIELDS_TABLE_NAME, update_ts)
