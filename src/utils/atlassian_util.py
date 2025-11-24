import urllib.parse

import src.config.variables as var
import src.utils.connection_util as conn_util
import src.utils.http_requests_util as http_requests_util

LAST_LOADED_TS_KEY = "last_loaded_ts"


def get_jql_query(date):
    date_formatted = date.strftime("%Y-%m-%d %H:%M")
    jql_query = f"{var.PLAIN_JQL} AND '{var.ISSUE_DATE_FIELD}' >= '{date_formatted}' ORDER BY {var.ISSUE_DATE_FIELD} ASC"

    return jql_query


def get_jql_results(jql_query, start_at):
    jql_query_encoded = urllib.parse.quote_plus(jql_query)
    conn_info = conn_util.get_atlassian_conn_props()
    url = f"""{conn_info["host"]}/{var.API_SEARCH_METHOD_PATH}?jql={jql_query_encoded}&expand={var.JQL_EXPAND}&maxResults={var.JQL_BATCH_SIZE}&startAt={start_at}"""

    payload = {}
    headers = get_atl_headers(conn_info)

    response = http_requests_util.execute_request("GET", url, headers, payload)
    return response


def get_jql_results_count(jql_query):
    jql_query_encoded = urllib.parse.quote_plus(jql_query)
    conn_info = conn_util.get_atlassian_conn_props()
    url = f"{conn_info.host}/{var.API_SEARCH_METHOD_PATH}?jql={jql_query_encoded}&maxResults=0&fields=id"

    payload = {}
    headers = get_atl_headers(conn_info)

    response = http_requests_util.execute_request("GET", url, headers, payload)
    total = response['total']

    return total


def get_atl_headers(conn_info):
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {conn_info["password"]}'
    }

    return headers
