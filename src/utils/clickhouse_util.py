import logging

import clickhouse_connect

import src.config.variables as var
import src.utils.connection_util as conn_util


def execute_query(clickhouse_client, query: str, logger=logging.getLogger("clickhouse")):
    # logger.info(f'Executing query: {query}')
    clickhouse_client.command(query)


def get_query_string_from_file(file_path):
    with open(file_path, 'r') as file:
        query_string = file.read()

    return query_string


def execute_query_from_file(clickhouse_client, file_name: str, logger=logging.getLogger("clickhouse")):
    logger.info(f'Executing query: {file_name}')
    sql_file_path = f'./src/sql/{var.CDM_SCHEMA_NAME}/{file_name}.sql'
    if var.MODE == "dag":
        sql_file_path = f"{var.AIRFLOW_DAGS_DIR}/{sql_file_path}"

    query = get_query_string_from_file(sql_file_path)
    execute_query(clickhouse_client, query, logger)


def drop_table(clickhouse_client, table_name, logger=logging.getLogger("clickhouse")):
    logger.info(f'Dropping table: {table_name}')
    clickhouse_client.query(f'DROP TABLE IF EXISTS {table_name};')


def get_clickhouse_client():
    conn_props = conn_util.get_click_conn_props()

    clickhouse_client = clickhouse_connect.get_client(host=conn_props["host"],
                                                      port=conn_props["port"],
                                                      database=conn_props["db"],
                                                      username=conn_props["user"],
                                                      password=conn_props["password"],
                                                      secure=False)

    return clickhouse_client
