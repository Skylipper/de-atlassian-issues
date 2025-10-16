import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



import src.utils.variables as var

log = logging.getLogger("ds_init_dag")


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'init', 'sql'],
    template_searchpath=['../'],
    is_paused_upon_creation=True
)
def init_stg_dag():
    init_stg_issues = SQLExecuteQueryOperator(
        task_id="init_stg_issues",
        conn_id=var.DWH_CONNECTION_NAME,
        sql="sql/stg_init_issues.sql",
        autocommit=True
    )

    init_stg_issues


dag = init_stg_dag()
