import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

import src.utils.variables as var

log = logging.getLogger("ds_init_dag")


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'init', 'manual'],
    template_searchpath=['src'],
    is_paused_upon_creation=True
)
def init_stg_dag():
    init_stg_issues = PostgresOperator(
        task_id="init_stg_model",
        postgres_conn_id=var.dwh_connection_name,  # Replace with your SQL connection ID
        sql="sql/stg_init_issues.sql.sql",
        autocommit=True
    )

    init_stg_issues


dag = init_stg_dag()
