import src.utils.variables as var
from src.utils import dwh_util
from datetime import datetime

versions_file_path = f'{var.AIRFLOW_DAGS_DIR}/src/raw/{var.VERSIONS_FILE_NAME}'
versions_file_path = """C:\\Users\skyli\Documents\Практикум курс\de-atlassian-issues\src\\raw\\versions.txt"""
text_to_search = 'long term support'
delimiter = ' ('

def get_lts_list():
    lts_versions_list = []
    try:
        with open(versions_file_path, 'r') as file:
            for line in file:
                processed_line = line.strip()
                if text_to_search in processed_line:
                    lts_versions_list.append(processed_line)
    except FileNotFoundError:
        print(f"Error: The file '{versions_file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return lts_versions_list

lts_versions_list = get_lts_list()

conn = dwh_util.get_dwh_connection()
with conn:
    cur = conn.cursor()
    update_ts = datetime.now()
    last_load_ts = var.START_DATE
    for version in lts_versions_list:
        version_number = version.split(delimiter, 1)[0]
        object_id = version_number
        object_value = version
        dwh_util.insert_stg_data(cur, var.STG_FIELDS_TABLE_NAME, object_id, object_value, update_ts.isoformat())
    dwh_util.update_last_loaded_ts(cur, var.STG_WF_TABLE_NAME, var.STG_FIELDS_TABLE_NAME, update_ts)