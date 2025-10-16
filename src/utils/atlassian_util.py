from airflow.hooks.base import BaseHook
import src.utils.variables as var


def get_atl_connection_info():
    conn_info = BaseHook.get_connection(var.ATLASSIAN_CONN_NAME)

    return conn_info