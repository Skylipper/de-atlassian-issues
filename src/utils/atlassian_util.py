from airflow.hooks.base import BaseHook
import urllib.parse
import src.utils.variables as var
import src.utils.http_requests_util as http_requests_util


def get_atl_connection_info():
    conn_info = BaseHook.get_connection(var.ATLASSIAN_CONN_NAME)

    return conn_info


def get_jql_query(date):
    date_formatted = date.strftime("%Y-%m-%d")
    jql_query = f"{var.PLAIN_JQL} AND '{var.ISSUE_DATE_FIELD}' > '{date_formatted}' ORDER BY {var.ISSUE_DATE_FIELD} ASC"

    return jql_query


def get_jql_results(jql_query):
    jql_query_encoded = urllib.parse.quote_plus(jql_query)
    conn_info = get_atl_connection_info()
    url = f"{conn_info.host}/{var.API_SEARCH_METHOD_PATH}?jql={jql_query_encoded}&expand=names,changelog&maxResults={var.JQL_BATCH_SIZE}"

    payload = {}
    headers = get_atl_headers(conn_info)

    response = http_requests_util.execute_request("GET", url, headers, payload, 200)

    return response

def get_results_batch(date=var.START_DATE):
    date = get_last_loaded_ts()
    jql_query = get_jql_query(date)




def get_atl_headers(conn_info):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {conn_info.password}'
    }

    return headers
