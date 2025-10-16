from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import src.variables.variables as var


def check_task_func():
    print(var.ATLASSIAN_JIRA_URL)


@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def load_data_from_atlassian_dag():
    start_task = EmptyOperator(task_id='start_task')

    @task()
    def check_task():
        check_task_func()

    end_task = EmptyOperator(task_id='end_task')

    start_task >> check_task >> end_task

dag = load_data_from_atlassian_dag()
