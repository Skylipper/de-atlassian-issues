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

    payload = {}
    headers = get_atl_headers(conn_info)

    response = execute_request("GET", url, headers, payload, 200)

    return response

def get_atl_headers(conn_info):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {conn_info.password}'
    }

    return headers

def execute_request(method, url, headers, payload, expected_code):
    logging.info(f"method: {method}, url: {url}")

    response_code = 0
    response = None
    retries = 0

    for i in range (0, var.REQUEST_RETRY_LIMIT):
        response = requests.request("GET", url, headers=headers, data=payload)
        response_code = response.status_code
        if response_code == expected_code:
            break
        retries += 1

    if response_code != expected_code:
        raise Exception(f"Request failed with code {response_code}, expected {expected_code}")

    # while response_code != expected_code:
    #     response = requests.request("GET", url, headers=headers, data=payload)
    #     response_code = response.status_code
    #     retries += 1
    #
    #     if retries > var.REQUEST_RETRY_LIMIT:
    #         raise


    return response.json()
