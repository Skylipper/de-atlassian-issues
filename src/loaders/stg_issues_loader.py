import json
from datetime import datetime

from airflow.hooks.base import BaseHook

import src.utils.atlassian_util as atl
import src.utils.dwh_util as dwh_util
import src.utils.variables as var


def get_jql_results_batch():
    date = dwh_util.get_last_loaded_ts(var.STG_WF_TABLE_NAME, var.STG_ISSUES_TABLE_NAME)
    jql_query = atl.get_jql_query(date)

    response = atl.get_jql_results(jql_query)
    return response

def load_issues(log):
    processed_count = 0
    while processed_count < var.JQL_RESULTS_RUN_LIMIT:
        log.info(f"Processing {processed_count}/{var.JQL_RESULTS_RUN_LIMIT}")
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
                log.info(f"{update_ts}: {object_id}")
                if  update_ts > last_load_ts:
                    last_load_ts = update_ts
                dwh_util.insert_stg_data(cur, var.STG_ISSUES_TABLE_NAME, object_id, object_value, update_ts)
                processed_count += 1
            log.info(f"{last_load_ts}")
            dwh_util.update_last_loaded_ts(cur, var.STG_WF_TABLE_NAME, var.STG_ISSUES_TABLE_NAME, last_load_ts)

        if total <= var.JQL_BATCH_SIZE:
            break
