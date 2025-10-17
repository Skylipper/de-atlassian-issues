from datetime import datetime

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
START_DATE = datetime(2010, 1, 1)
JQL_LIMIT = 500
JQL_EXPAND = 'changelog'
JQL_BATCH_SIZE = 50
ISSUE_DATE_FIELD = 'updated'
PLAIN_JQL = f"project in ('{SRV_PROJECT_KEY}','{CLOUD_PROJECT_KEY}')"
ATL_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
