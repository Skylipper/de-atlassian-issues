import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import src.loaders.ods.ods_tables_loader as otl

log = logging.getLogger("load_objects")


def load_issue_components_f():
    otl.load_issue_components()

def load_issue_versions_f():
    otl.load_issue_versions()

def load_issue_fix_versions_f():
    otl.load_issue_fix_versions()


@dag(
    start_date=datetime(2025, 10, 22),
    schedule= '*/20 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'ods','atlassian'],
)
def load_ods_tables():
    load_issue_components_task = PythonOperator(
        task_id='load_issue_components',
        python_callable=load_issue_components_f
    )
    load_issue_versions_task = PythonOperator(
        task_id='load_issue_versions',
        python_callable=load_issue_versions_f
    )
    load_issue_fix_versions_task = PythonOperator(
        task_id='load_issue_fix_versions',
        python_callable=load_issue_fix_versions_f
    )

    [load_issue_components_task, load_issue_versions_task, load_issue_fix_versions_task]


dag = load_ods_tables()
