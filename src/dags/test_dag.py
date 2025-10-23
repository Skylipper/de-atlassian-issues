import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

import src.loaders.ods.ods_tables_loader as otl
import src.utils.atlassian_util as atl
import src.utils.variables as var
from src.loaders.stg.issues_loader import load_issues

log = logging.getLogger("load_data_from_atlassian_dag")


def check_task_func():
    atl_conn_info = atl.get_atl_connection_info()
    log.info(atl_conn_info.host)

    load_issues(log)

    log.info(var.PLAIN_JQL)


def load_issues_f():
    otl.load_issues()


@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def test():
    load_issues_task = PythonOperator(
        task_id='load_issues',
        python_callable=load_issues_f
    )

    load_issues_task


dag = test()
