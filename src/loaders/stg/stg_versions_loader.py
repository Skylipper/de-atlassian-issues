import src.utils.variables as var
from src.utils import dwh_util
from datetime import datetime

versions_file_path = f'{var.AIRFLOW_DAGS_DIR}/src/raw/{var.VERSIONS_FILE_NAME}'
# versions_file_path = """C:\\Users\skyli\Documents\Практикум курс\de-atlassian-issues\src\\raw\\versions.txt"""
text_to_search = 'long term support'
delimiter = ' '

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


def load_lts_versions():
    lts_versions_list = get_lts_list()
    conn = dwh_util.get_dwh_connection()
    with conn:
        cur = conn.cursor()
        cur.execute(f'SELECT DISTINCT object_id FROM {var.STG_LTS_VERSIONS_TABLE_NAME}')
        existing_lts_list = cur.fetchall()
        print(existing_lts_list)
        update_ts = datetime.now()
        for version in lts_versions_list:
            version_number = version.split(delimiter, 1)[0]
            print(version_number)
            object_id = version_number
            object_value = version
            print(object_id)
            print(object_value)
            if object_id not in existing_lts_list or len(existing_lts_list) == 0:
                dwh_util.insert_stg_data(cur, var.STG_LTS_VERSIONS_TABLE_NAME, object_id, object_value, update_ts.isoformat())
                existing_lts_list.append(object_id)
        dwh_util.update_last_loaded_ts(cur, var.STG_WF_TABLE_NAME, var.STG_LTS_VERSIONS_TABLE_NAME, update_ts)