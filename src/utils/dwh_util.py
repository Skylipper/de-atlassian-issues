import json
import logging

import psycopg2
from airflow.hooks.base import BaseHook

import src.utils.variables as var

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


def get_dwh_connection(log=logging.getLogger("dwh_util")):
    conn = get_connection(var.DWH_CONNECTION_NAME, log)

    return conn


def get_last_loaded_ts(settings_table, table, logger=log):
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
        if (rows):
            last_loaded_ts = rows[0][0]
            logger.info(f"last_loaded_ts: {last_loaded_ts}")

    return last_loaded_ts


def insert_stg_data(cur, table, object_id, object_value, update_ts):
    cur.execute(
        f"""
            INSERT INTO {table} (object_id, object_value, update_ts)
            VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
            ON CONFLICT (object_id) DO UPDATE
                SET object_value    = EXCLUDED.object_value,
                    update_ts  = EXCLUDED.update_ts;
            """,
        {
            "object_id": object_id,
            "object_value": object_value,
            "update_ts": update_ts
        }
    )


def update_last_loaded_ts(cur, load_setting_table, table, last_loaded_ts):
    #TODO find when last updated resets to default date
    wf_setting_dict = {var.LAST_LOADED_TS_KEY: last_loaded_ts.isoformat()}
    wf_settings = json.dumps(wf_setting_dict)

    cur.execute(
        f"""
                INSERT INTO {load_setting_table} (workflow_key, workflow_settings)
                VALUES (%(etl_key)s, %(etl_setting)s)
                ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
        {
            "etl_key": table,
            "etl_setting": wf_settings
        }
    )
