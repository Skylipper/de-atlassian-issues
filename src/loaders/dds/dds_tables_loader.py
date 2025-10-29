import src.utils.variables as var
from src.utils.loader_util import load_data_for_table

stat_table = var.DDS_WF_TABLE_NAME
schema_name = var.DDS_SCHEMA_NAME


def load_d_projects():
    table = var.DDS_D_PROJECTS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_priorities():
    table = var.DDS_D_PRIORITIES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_issuetypes():
    table = var.DDS_D_ISSUETYPES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_components():
    table = var.DDS_D_COMPONENTS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_resolutions():
    table = var.DDS_D_RESOLUTIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_statuses():
    table = var.DDS_D_STATUSES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_users():
    table = var.DDS_D_USERS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_d_versions():
    table = var.DDS_D_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_f_issues():
    table = var.DDS_F_ISSUES_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'updated')


def load_f_issue_components():
    table = var.DDS_F_ISSUE_COMPONENT_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_f_issue_versions():
    table = var.DDS_F_ISSUE_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')


def load_f_issue_fix_versions():
    table = var.DDS_F_ISSUE_FIX_VERSIONS_TABLE_NAME
    load_data_for_table(schema_name, table, stat_table, 'update_ts')
