import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

import src.loaders.ods.ods_tables_loader as otl

log = logging.getLogger("load_objects")


@dag(
    start_date=datetime(2025, 10, 22),
    schedule='25 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'ods', 'atlassian'],
)
def load_ods_tables():
    load_issue_components_task = PythonOperator(
        task_id='load_issue_components',
        python_callable=otl.load_issue_components
    )
    load_issue_versions_task = PythonOperator(
        task_id='load_issue_versions',
        python_callable=otl.load_issue_versions
    )
    load_issue_fix_versions_task = PythonOperator(
        task_id='load_issue_fix_versions',
        python_callable=otl.load_issue_fix_versions
    )
    load_issues_task = PythonOperator(
        task_id='load_issues',
        python_callable=otl.load_issues
    )

    [load_issue_components_task, load_issue_versions_task, load_issue_fix_versions_task] >> load_issues_task


dag = load_ods_tables()
