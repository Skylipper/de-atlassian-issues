import logging
import src.utils.variables as var
import psycopg2

from airflow.hooks.base import BaseHook


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
