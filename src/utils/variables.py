from datetime import datetime, timezone

from airflow.configuration import conf

#Airflow variables
AIRFLOW_DAGS_DIR = conf.get("core", "dags_folder")

# DWH variables
DWH_CONNECTION_NAME = "ATLAS_DWH_DB"
STG_WF_TABLE_NAME = 'stg.load_settings'
STG_ISSUES_TABLE_NAME = 'stg.issues'
LAST_LOADED_TS_KEY = "last_loaded_ts"

# ATLASSIAN variables
ATLASSIAN_CONN_NAME = "ATLASSIAN_REST_API"
REQUEST_RETRY_LIMIT = 5
API_SEARCH_METHOD_PATH = "rest/api/2/search"
CLOUD_PROJECT_KEY = "JRACLOUD"
SRV_PROJECT_KEY = "JRASERVER"
START_DATE = datetime(2010, 1, 1,0,0,0,0,tzinfo=timezone.utc)
JQL_RESULTS_RUN_LIMIT = 1000
JQL_EXPAND = 'changelog'
JQL_BATCH_SIZE = 100 # Не ставить слишком мало, в JQL поиске есть округление до минут. Если изменений в минуту будет больше, чем количество в пачке, скрипт будетт обрабатывать одни те же значения.
ISSUE_DATE_FIELD = 'updated'
PLAIN_JQL = f"project in ('{SRV_PROJECT_KEY}','{CLOUD_PROJECT_KEY}')"
ATL_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
