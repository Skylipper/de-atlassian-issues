import json
import urllib.parse
from datetime import datetime

from airflow.hooks.base import BaseHook

import src.utils.dwh_util as dwh_util
import src.utils.http_requests_util as http_requests_util
import src.utils.variables as var

LAST_LOADED_TS_KEY = "last_loaded_ts"


def get_atl_connection_info():
    conn_info = BaseHook.get_connection(var.ATLASSIAN_CONN_NAME)

    return conn_info


def get_jql_query(date):
    date_formatted = date.strftime("%Y-%m-%d %H:%M")
    jql_query = f"{var.PLAIN_JQL} AND '{var.ISSUE_DATE_FIELD}' >= '{date_formatted}' ORDER BY {var.ISSUE_DATE_FIELD} ASC"

    return jql_query


def get_jql_results(jql_query):
    jql_query_encoded = urllib.parse.quote_plus(jql_query)
    conn_info = get_atl_connection_info()
    url = f"{conn_info.host}/{var.API_SEARCH_METHOD_PATH}?jql={jql_query_encoded}&expand={var.JQL_EXPAND}&maxResults={var.JQL_BATCH_SIZE}"

    payload = {}
    headers = get_atl_headers(conn_info)

    response = http_requests_util.execute_request("GET", url, headers, payload, 200)

    return response


def get_jql_results_batch():
    date = dwh_util.get_last_loaded_ts(var.STG_WF_TABLE_NAME, var.STG_ISSUES_TABLE_NAME)
    jql_query = get_jql_query(date)

    response = get_jql_results(jql_query)
    return response


def load_issues(log):
    processed_count = 0
    while processed_count < var.JQL_LIMIT:
        log.info(f"Processing {processed_count}/{var.JQL_LIMIT}")
        issues_json_batch = get_jql_results_batch()
        total = issues_json_batch['total']
        log.info(f"Total: {total}")
        issues_array = issues_json_batch['issues']

        conn = dwh_util.get_dwh_connection()
        with conn:
            cur = conn.cursor()
            last_load_ts = var.START_DATE
            for issue in issues_array:
                object_id = issue['id']
                object_value = json.dumps(issue)
                update_ts = datetime.strptime(issue['fields']['updated'], var.ATL_TIME_FORMAT)
                log.info(f"{last_load_ts} - {update_ts}")
                if  update_ts > last_load_ts:
                    last_load_ts = update_ts
                dwh_util.insert_stg_data(cur, var.STG_ISSUES_TABLE_NAME, object_id, object_value, update_ts)
                processed_count += 1
            dwh_util.update_last_loaded_ts(cur, var.STG_WF_TABLE_NAME, var.STG_ISSUES_TABLE_NAME, last_load_ts)

        # conn.commit()
        # cur.close()
        # conn.close()

        if total <= var.JQL_BATCH_SIZE:
            break


def get_atl_headers(conn_info):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {conn_info.password}'
    }

    return headers
