import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from src.loaders.stg.stg_issues_loader import load_issues

log = logging.getLogger("load_issues")


def load_issues_func():
    load_issues(log)

#TODO add issue count check


@dag(
    start_date=datetime(2025, 10, 15),
    schedule= '10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'stg','atlassian'],
)
def load_stg_raw_data():
    load_task = PythonOperator(
        task_id='loads_issues',
        python_callable=load_issues_func
    )

    load_task


dag = load_issues_from_atlassian_to_stg()
