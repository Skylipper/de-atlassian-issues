import logging

from airflow.hooks.base import BaseHook
import requests
import src.utils.variables as var


def get_atl_connection_info():
    conn_info = BaseHook.get_connection(var.ATLASSIAN_CONN_NAME)

    return conn_info

def get_jql_query(date = var.START_DATE):
    date_formatted = date.strftime("%Y-%m-%d")
    jql_query = f"{var.PLAIN_JQL} AND '{var.ISSUE_DATE_FIELD}' > '{date_formatted}' ORDER BY {var.ISSUE_DATE_FIELD} ASC"

    return jql_query

def get_jql_results(jql_query = None):
    conn_info = get_atl_connection_info()
    url = f"{conn_info.host}/{var.API_SEARCH_METHOD_PATH}?jql=issue%20%3D%20JRASERVER-3943"
    logging.info(f"url: {url}")

    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {conn_info.password}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)
