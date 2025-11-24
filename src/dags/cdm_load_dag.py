import logging

import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import src.config.variables as var
import src.loaders.cdm.cdm_tables_loader as ctl

log = logging.getLogger("cdm_load_dag")


@dag(
    schedule='45 * * * *',
    start_date=pendulum.datetime(2025, 11, 20, tz="UTC"),
    catchup=False,
    tags=['project', 'load', 'cdm', 'atlassian'],
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

    load_issues_info = PythonOperator(
        task_id='load_cdm_issues_info',
        python_callable=ctl.load_issues_info
    )

    refresh_mv_issues_info >> load_issues_info


dag = cdm_load_dag()
