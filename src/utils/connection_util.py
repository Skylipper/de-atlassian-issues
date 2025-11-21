import src.config.variables as var
import src.config.secrets as secrets
from airflow.hooks.base import BaseHook

mode = var.MODE

def get_click_conn_props():
    click_conn_props = {}
    if mode == "local":
        click_conn_props["host"] = secrets.click_host
        click_conn_props["port"] = secrets.click_port
        click_conn_props["db"] =  secrets.click_db_name
        click_conn_props["user"] = secrets.click_user
        click_conn_props["password"] = secrets.click_password
        click_conn_props["driver"] = var.CLICK_DRIVER

    if mode == "dag":
        airflow_conn_props = BaseHook.get_connection(var.CLICK_CONNECTION_NAME)
        click_conn_props["host"] = airflow_conn_props.host
        click_conn_props["port"] = airflow_conn_props.port
        click_conn_props["db"] = airflow_conn_props.schema
        click_conn_props["user"] = airflow_conn_props.username
        click_conn_props["password"] = airflow_conn_props.password
        click_conn_props["driver"] = var.CLICK_DRIVER

    return click_conn_props

def get_dwh_conn_props():
    dwh_conn_props = {}
    if mode == "local":
        dwh_conn_props["host"] = secrets.dwh_host
        dwh_conn_props["port"] = secrets.dwh_port
        dwh_conn_props["db"] = secrets.dwh_db_name
        dwh_conn_props["user"] = secrets.dwh_user
        dwh_conn_props["password"] = secrets.dwh_password
        dwh_conn_props["driver"] = var.DWH_DRIVER

    if mode == "dag":
        airflow_conn_props = BaseHook.get_connection(var.DWH_CONNECTION_NAME)
        dwh_conn_props["host"] = airflow_conn_props.host
        dwh_conn_props["port"] = airflow_conn_props.port
        dwh_conn_props["db"] = airflow_conn_props.schema
        dwh_conn_props["user"] = airflow_conn_props.username
        dwh_conn_props["password"] = airflow_conn_props.password
        dwh_conn_props["driver"] = var.CLICK_DRIVER

    return dwh_conn_props


