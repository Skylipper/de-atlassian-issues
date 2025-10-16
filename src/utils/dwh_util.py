import logging
import src.utils.variables as var
import psycopg2

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

def get_connection(conn_id, log=logging.getLogger("test")):
    conn_props = None
    try:
        conn_props = BaseHook.get_connection(conn_id)
    except Exception as e:
        log.error(f"Could not connect to DWH: {e}")

    conn = psycopg2.connect(
        f"host='{conn_props.host}' port='{conn_props.port}' dbname='{conn_props.schema}' user='{conn_props.login}' password='{conn_props.password}'")

    return conn


def get_dwh_connection(log=logging.getLogger("test")):
    conn = get_connection(var.DWH_CONNECTION_NAME, log)

    return conn

def get_stg_last_loaded_ts(settings_table, table, logger=log):
    query = f"""SELECT COALESCE(
                (SELECT  (workflow_settings::jsonb ->> 'last_loaded_ts')::timestamp
                FROM {settings_table}
                WHERE workflow_key = '{table}'), '{var.START_DATE}'::timestamp) as last_loaded_ts"""

    conn = get_dwh_connection(log)

    last_loaded_ts = var.START_DATE
    with conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        if(rows):
            last_loaded_ts = rows[0][0]

    return last_loaded_ts

