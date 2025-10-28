import src.utils.variables as var
from src.utils import dwh_util


def load_data_for_table(schema, table, stat_table, update_field):
    sql_file_path = f'{var.AIRFLOW_DAGS_DIR}/src/sql/{schema}/load_{table}.sql'

    conn = dwh_util.get_dwh_connection()
    last_updated = dwh_util.get_last_loaded_ts(stat_table, f'{schema}.{table}')
    with conn:
        cur = conn.cursor()
        # Insert and update data
        query = dwh_util.get_query_string_from_file(sql_file_path)
        cur.execute(query)

        # Get last updated value
        cur.execute(f"SELECT COALESCE(MAX({update_field}),'{last_updated}') last_updated FROM {schema}.{table};")
        update_ts = cur.fetchall()[0][0]

        # Fill statistics table
        dwh_util.update_last_loaded_ts(cur, stat_table, f'{schema}.{table}', update_ts)


def load_d_projects():
    stat_table = var.DDS_WF_TABLE_NAME
    schema_name = var.DDS_SCHEMA_NAME
    table = var.DDS_D_PROJECTS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_priorities():
    stat_table = var.DDS_WF_TABLE_NAME
    schema_name = var.DDS_SCHEMA_NAME
    table = var.DDS_D_PRIORITIES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')
