from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.configuration import conf
from subprocess import call
from datetime import datetime
import os

def call_script(script_path):
    current_directory = os.getcwd()
    print(f"Current Working Directory: {current_directory}")
    dags_folder_path = conf.get("core", "dags_folder")
    # Print the path
    print(f"Airflow DAGs folder path: {dags_folder_path}")
    call(f"{dags_folder_path}/de-atlassian-issues/{script_path}", shell=True)

@dag(
    schedule=None,
    start_date=datetime(2025, 10, 15),
    catchup=False,
    tags=['project', 'deploy', 'manual'],
    is_paused_upon_creation=True
)
def deploy_dag():
    deploy = PythonOperator(
        task_id='deploy',
        python_callable=call_script,
        op_kwargs={'script_path': "src/sh/pull.sh"},
    )

    (deploy)

dag = deploy_dag()

