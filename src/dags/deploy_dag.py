from airflow.decorators import dag, task
from subprocess import call
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime.now(),
    catchup=False,
    tags=['project', 'deploy', 'manual'],
    is_paused_upon_creation=True
)
def deploy_dag():
    @task
    def deploy():
        call("sh/pull.sh")
        pass

    (deploy)

dag = deploy_dag()
