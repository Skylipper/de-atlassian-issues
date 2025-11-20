import src.config.variables as var
from src.utils.loader_util import load_data_for_table

stat_table = var.ODS_WF_TABLE_NAME
schema_name = var.ODS_SCHEMA_NAME


def load_issue_components():
    table = var.ODS_ISSUE_COMPS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_issue_versions():
    table = var.ODS_ISSUE_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_issue_fix_versions():
    table = var.ODS_ISSUE_FIX_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_issues():
    table = var.ODS_ISSUES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'updated')

def load_lts_versions():
    table = var.ODS_LTS_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')
