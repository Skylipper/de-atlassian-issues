import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import src.checks.stg.check_issues_count as stg_check

log = logging.getLogger("load_data_from_atlassian_dag")


def load_dds():
    total = stg_check.get_jql_results_count()
    print(total)



@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def test():
    load_issues_task = PythonOperator(
        task_id='load_d_projects',
        python_callable=load_dds
    )

    load_issues_task


dag = test()
