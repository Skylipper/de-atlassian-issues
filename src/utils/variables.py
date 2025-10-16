from airflow.configuration import conf

#Airflow variables
AIRFLOW_DAGS_DIR = conf.get("core", "dags_folder")

# PG variables
DWH_CONNECTION_NAME = "ATLAS_DWH_DB"

# ATLASSIAN variables
ATLASSIAN_CONN_NAME = "ATLASSIAN_REST_API"

ATLASSIAN_AUTH_TOKEN_VAR_NAME = 'ATLASSIAN_AUTH_TOKEN'
API_SEARCH_METHOD_PATH = "rest/api/2/search`"
CLOUD_PROJECT_KEY = "JRACLOUD"
SRV_PROJECT_KEY = "JRASERVER"
PLAIN_JQL = f"project in ({SRV_PROJECT_KEY},{CLOUD_PROJECT_KEY}) ORDER BY updated ASC"



