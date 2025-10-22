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
def init_stg_dag():
    join_task = EmptyOperator(task_id='join_point')

    init_stg_issues = SQLExecuteQueryOperator(
        task_id="init_stg_issues",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="stg_init_issues.sql",
        autocommit=True
    )

    init_stg_fields = SQLExecuteQueryOperator(
        task_id="init_stg_fields",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="stg_init_fields.sql",
        autocommit=True
    )

    init_stg_load_settings = SQLExecuteQueryOperator(
        task_id="stg_init_load_settings",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="stg_init_load_settings.sql.sql",
        autocommit=True
    )

    init_ods_load_settings = SQLExecuteQueryOperator(
        task_id="ods_init_load_settings",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods_init_load_settings.sql",
        autocommit=True
    )

    init_ods_issue_components = SQLExecuteQueryOperator(
        task_id="ods_init_issue_components",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="ods_init_issue_component_values.sql",
        autocommit=True
    )




    [init_stg_issues, init_stg_fields, init_stg_load_settings] >> join_task
    join_task >> [init_ods_load_settings, init_ods_issue_components]


dag = init_stg_dag()
