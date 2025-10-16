from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from subprocess import call
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2025, 10, 15),
    catchup=False,
    tags=['project', 'deploy', 'manual'],
    is_paused_upon_creation=True
)
def deploy_dag():
    @task
    def deploy():
        call("sh/pull.sh")


    join_task = EmptyOperator(task_id='join_point')

    (join_task >> deploy)

dag = deploy_dag()
