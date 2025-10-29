import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

import src.utils.variables as var

log = logging.getLogger("ds_init_dag")


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'init', 'sql','atlassian'],
    template_searchpath=[f'{var.AIRFLOW_DAGS_DIR}/src/sql/'],
    is_paused_upon_creation=True
)
def init_tables_dag():
    join_task = EmptyOperator(task_id='join_point')

    init_stg_issues = SQLExecuteQueryOperator(
        task_id="init_stg_issues",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="stg/init_issues.sql",
        autocommit=True
    )


    init_stg_load_settings = SQLExecuteQueryOperator(
        task_id="stg_init_load_settings",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="stg/init_load_settings.sql.sql",
        autocommit=True
    )

    init_ods_load_settings = SQLExecuteQueryOperator(
        task_id="ods_init_load_settings",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods/init_load_settings.sql",
        autocommit=True
    )

    init_ods_issue_components = SQLExecuteQueryOperator(
        task_id="ods_init_issue_components",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods/init_issue_component_values.sql",
        autocommit=True
    )

    init_ods_issue_versions = SQLExecuteQueryOperator(
        task_id="ods_init_issue_versions",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods/init_issue_versions_values.sql",
        autocommit=True
    )

    init_ods_issue_fix_versions = SQLExecuteQueryOperator(
        task_id="ods_init_issue_fix_versions",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods/init_issue_fix_versions_values.sql",
        autocommit=True
    )

    init_ods_issues = SQLExecuteQueryOperator(
        task_id="ods_init_issues",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods/init_issues.sql",
        autocommit=True
    )

    init_dds_load_settings = SQLExecuteQueryOperator(
        task_id="dds_init_load_settings",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_load_settings.sql",
        autocommit=True
    )

    init_dds_d_projects = SQLExecuteQueryOperator(
        task_id="init_dds_d_projects",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_projects.sql",
        autocommit=True
    )

    init_dds_d_priorities = SQLExecuteQueryOperator(
        task_id="init_dds_d_priorities",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_priorities.sql",
    )

    init_dds_d_issuetypes = SQLExecuteQueryOperator(
        task_id="init_dds_d_issuetypes",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_issuetypes.sql",
        autocommit=True
    )

    init_dds_d_statuses = SQLExecuteQueryOperator(
        task_id="init_dds_d_statuses",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_statuses.sql",
        autocommit=True
    )

    init_dds_d_resolutions = SQLExecuteQueryOperator(
        task_id="init_dds_d_resolutions",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_resolutions.sql",
        autocommit=True
    )

    init_dds_d_users = SQLExecuteQueryOperator(
        task_id="init_dds_d_users",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_users.sql",
        autocommit=True
    )

    init_dds_d_components = SQLExecuteQueryOperator(
        task_id="init_dds_d_components",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_components.sql",
        autocommit=True
    )

    init_dds_d_versions = SQLExecuteQueryOperator(
        task_id="init_dds_d_versions",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_d_versions.sql",
        autocommit=True
    )

    init_dds_f_issues = SQLExecuteQueryOperator(
        task_id="init_dds_f_issues",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_f_issues.sql",
        autocommit=True
    )

    init_dds_f_issue_component_values = SQLExecuteQueryOperator(
        task_id="init_dds_f_issue_component_values",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_f_issue_component_values.sql",
        autocommit=True
    )

    init_dds_f_issue_version_values = SQLExecuteQueryOperator(
        task_id="init_dds_f_issue_version_values",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_f_issue_version_values.sql",
        autocommit=True
    )

    init_dds_f_issue_fix_version_values = SQLExecuteQueryOperator(
        task_id="init_dds_f_issue_fix_version_values",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="dds/init_f_issue_fix_version_values.sql",
        autocommit=True
    )


    [init_stg_issues, init_stg_fields, init_stg_load_settings] >> init_ods_load_settings
    init_ods_load_settings >> [init_ods_issue_components, init_ods_issue_versions, init_ods_issue_fix_versions, init_ods_issues] >> init_dds_load_settings
    init_dds_load_settings >> [init_dds_d_projects, init_dds_d_statuses, init_dds_d_resolutions, init_dds_d_issuetypes, init_dds_d_priorities, init_dds_d_users] >> join_task
    join_task >> [init_dds_d_components, init_dds_d_versions] >> init_dds_f_issues
    init_dds_f_issues >> [init_dds_f_issue_component_values, init_dds_f_issue_version_values, init_dds_f_issue_fix_version_values]


dag = init_tables_dag()
