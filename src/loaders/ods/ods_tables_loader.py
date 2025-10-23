from src.utils import  dwh_util
import src.utils.variables as var

def load_issue_components():
    conn = dwh_util.get_dwh_connection()
    last_updated = dwh_util.get_last_loaded_ts(var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_COMPS_TABLE_NAME)
    with conn:
        cur = conn.cursor()
        query = dwh_util.get_query_string_from_file(f'{var.AIRFLOW_DAGS_DIR}/src/sql/ods/load_issue_component_values.sql')
        cur.execute(query)
        cur.execute(f"SELECT COALESCE(MAX(update_ts),'{last_updated}') last_updated FROM stg.issues;")
        update_ts = cur.fetchall()[0][0]
        dwh_util.update_last_loaded_ts(cur, var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_COMPS_TABLE_NAME, update_ts)

def load_issue_versions():
    conn = dwh_util.get_dwh_connection()
    last_updated = dwh_util.get_last_loaded_ts(var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_COMPS_TABLE_NAME)
    with conn:
        cur = conn.cursor()
        query = dwh_util.get_query_string_from_file(f'{var.AIRFLOW_DAGS_DIR}/src/sql/ods/load_issue_version_values.sql')
        cur.execute(query)
        cur.execute(f"SELECT COALESCE(MAX(update_ts),'{last_updated}') last_updated FROM stg.issues;")
        update_ts = cur.fetchall()[0][0]
        dwh_util.update_last_loaded_ts(cur, var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_VERSIONS_TABLE_NAME, update_ts)

def load_issue_fix_versions():
    conn = dwh_util.get_dwh_connection()
    last_updated = dwh_util.get_last_loaded_ts(var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_COMPS_TABLE_NAME)
    with conn:
        cur = conn.cursor()
        query = dwh_util.get_query_string_from_file(f'{var.AIRFLOW_DAGS_DIR}/src/sql/ods/load_issue_fix_version_values.sql')
        cur.execute(query)
        cur.execute(f"SELECT COALESCE(MAX(update_ts),'{last_updated}') last_updated FROM stg.issues;")
        update_ts = cur.fetchall()[0][0]
        dwh_util.update_last_loaded_ts(cur, var.ODS_WF_TABLE_NAME, var.ODS_ISSUE_FIX_VERSIONS_TABLE_NAME, update_ts)