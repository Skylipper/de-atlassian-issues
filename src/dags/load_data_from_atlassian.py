from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import src.variables.variables as var


def check_task_func():
    print(var.ATLASSIAN_JIRA_URL)
    print(var.PLAIN_JQL)


@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def load_data_from_atlassian_dag():
    check_task = PythonOperator(
        task_id='check_task',
        python_callable=check_task_func
    )


    check_task

dag = load_data_from_atlassian_dag()
