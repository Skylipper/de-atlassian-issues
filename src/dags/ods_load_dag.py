import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from src.loaders.ods_issue_components_loader import load_issue_components

log = logging.getLogger("load_objects")


def load_issue_components():
    load_issue_components()


@dag(
    start_date=datetime(2025, 10, 22),
    schedule= '0 8 * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'ods','atlassian'],
)
def load_ods_tables():
    load_issue_components_task = PythonOperator(
        task_id='load_issue_components',
        python_callable=load_issue_components
    )

    load_issue_components_task


dag = load_ods_tables()
