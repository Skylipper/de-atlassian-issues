import src.config.variables as var
import src.config.secrets as secrets
from airflow.hooks.base import BaseHook

mode = var.MODE

def get_click_conn_props():
    conn_props = {}
    if mode == "local":
        conn_props["host"] = secrets.click_host
        conn_props["port"] = secrets.click_port
        conn_props["db"] =  secrets.click_db_name
        conn_props["user"] = secrets.click_user
        conn_props["password"] = secrets.click_password
        conn_props["driver"] = var.CLICK_DRIVER

    if mode == "dag":
        airflow_conn_props = BaseHook.get_connection(var.CLICK_CONNECTION_NAME)
        conn_props["host"] = airflow_conn_props.host
        conn_props["port"] = airflow_conn_props.port
        conn_props["db"] = airflow_conn_props.schema
        conn_props["user"] = airflow_conn_props.login
        conn_props["password"] = airflow_conn_props.password
        conn_props["driver"] = var.CLICK_DRIVER

    return conn_props

def get_dwh_conn_props():
    conn_props = {}
    if mode == "local":
        conn_props["host"] = secrets.dwh_host
        conn_props["port"] = secrets.dwh_port
        conn_props["db"] = secrets.dwh_db_name
        conn_props["user"] = secrets.dwh_user
        conn_props["password"] = secrets.dwh_password
        conn_props["driver"] = var.DWH_DRIVER

    if mode == "dag":
        airflow_conn_props = BaseHook.get_connection(var.DWH_CONNECTION_NAME)
        conn_props["host"] = airflow_conn_props.host
        conn_props["port"] = airflow_conn_props.port
        conn_props["db"] = airflow_conn_props.schema
        conn_props["user"] = airflow_conn_props.login
        conn_props["password"] = airflow_conn_props.password
        conn_props["driver"] = var.DWH_DRIVER

    return conn_props


