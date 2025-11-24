from datetime import datetime, timezone

from airflow.configuration import conf

# Connection variables
MODE = "local"          # local-dag - для тестирования соединений без доступа к переменным airflow. Local - берем из config/secrets

#Airflow variables
AIRFLOW_DAGS_DIR = conf.get("core", "dags_folder")

# DWH variables
DWH_DRIVER = "org.postgresql.Driver"
DWH_CONNECTION_NAME = "ATLAS_DWH_DB"
VERSIONS_FILE_NAME = 'versions.txt'
DDS_SCHEMA_NAME = 'dds'
ODS_SCHEMA_NAME = 'ods'
CDM_SCHEMA_NAME = 'cdm'
STG_WF_TABLE_NAME = 'stg.load_settings'
ODS_WF_TABLE_NAME = 'ods.load_settings'
DDS_WF_TABLE_NAME = 'dds.load_settings'
STG_ISSUES_TABLE_NAME = 'stg.issues'
STG_FIELDS_TABLE_NAME = 'stg.fields'
STG_LTS_VERSIONS_TABLE_NAME = 'stg.lts_versions'
ODS_ISSUE_COMPS_TABLE_NAME = 'issue_component_values'
ODS_LTS_VERSIONS_TABLE_NAME = 'lts_versions'
ODS_ISSUE_VERSIONS_TABLE_NAME = 'issue_version_values'
ODS_ISSUE_FIX_VERSIONS_TABLE_NAME = 'issue_fix_version_values'
ODS_ISSUES_TABLE_NAME = 'issues'
DDS_D_PROJECTS_TABLE_NAME = 'd_projects'
DDS_D_PRIORITIES_TABLE_NAME = 'd_priorities'
DDS_D_ISSUETYPES_TABLE_NAME = 'd_issuetypes'
DDS_D_RESOLUTIONS_TABLE_NAME = 'd_resolutions'
DDS_D_STATUSES_TABLE_NAME = 'd_statuses'
DDS_D_USERS_TABLE_NAME = 'd_users'
DDS_D_VERSIONS_TABLE_NAME = 'd_versions'
DDS_D_COMPONENTS_TABLE_NAME = 'd_components'
DDS_F_ISSUES_TABLE_NAME = 'f_issues'
DDS_F_ISSUE_COMPONENT_TABLE_NAME = 'f_issue_component_values'
DDS_F_ISSUE_VERSIONS_TABLE_NAME = 'f_issue_version_values'
DDS_F_ISSUE_FIX_VERSIONS_TABLE_NAME = 'f_issue_fix_version_values'
CDM_MV_ISSUES_INFO_TABLE_NAME = 'mv_issues_info'
LAST_LOADED_TS_KEY = "last_loaded_ts"

# ATLASSIAN variables
ATLASSIAN_CONN_NAME = "ATLASSIAN_REST_API"
REQUEST_RETRY_LIMIT = 5
API_SEARCH_METHOD_PATH = "rest/api/2/search"
API_FIELDS_PATH = "rest/api/2/field"
API_FIELD_OPTIONS_PATH = "rest/api/2/field"
CLOUD_PROJECT_KEY = "JRACLOUD"
SRV_PROJECT_KEY = "JRASERVER"
START_DATE = datetime(2010, 1, 1,0,0,0,0,tzinfo=timezone.utc)
JQL_RESULTS_RUN_LIMIT = 5000
JQL_BATCH_SIZE = 50
JQL_EXPAND = 'changelog'
ISSUE_DATE_FIELD = 'updated'
PLAIN_JQL = f"project in ('{SRV_PROJECT_KEY}','{CLOUD_PROJECT_KEY}')"
ATL_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

# Clickhouse
CLICK_CONNECTION_NAME = "clickhouse"
CLICK_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
CLICK_ISSUES_TEMP_TABLE_NAME = "issues_info_temp"
CLICK_ISSUES_TABLE_NAME = "issues_info"
CLICK_INIT_TMP_ISSUES_FILE_NAME = "init_temp_issues_info"
CLICK_CLEAR_ISSUES_FILE_NAME = "clear_updated_issues_info"
CLICK_LOAD_ISSUES_FILE_NAME = "load_issues_info"
