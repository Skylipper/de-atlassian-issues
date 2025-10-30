import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import src.utils.variables as var

log = logging.getLogger("cdm_load_dag")


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'init', 'sql', 'atlassian'],
    template_searchpath=[f'{var.AIRFLOW_DAGS_DIR}/src/sql/'],
    is_paused_upon_creation=True
)
def cdm_load_dag():
    refresh_mv_issues_info = SQLExecuteQueryOperator(
        task_id="refresh_mv_issues_info",
        conn_id=var.DWH_CONNECTION_NAME,
        sql=f"REFRESH MATERIALIZED VIEW {var.CDM_SCHEMA_NAME}.{var.CDM_MV_ISSUES_INFO_TABLE_NAME};",
        autocommit=True
    )

    refresh_mv_issues_info


dag = cdm_load_dag()
