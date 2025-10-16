from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from subprocess import call
from datetime import datetime

def call_script(script_path):
    call(script_path)

@dag(
    schedule=None,
    start_date=datetime(2025, 10, 15),
    catchup=False,
    tags=['project', 'deploy', 'manual'],
    is_paused_upon_creation=True
)
def deploy_dag():
    deploy = PythonOperator(
        task_id='create_files_request',
        python_callable=call_script,
        op_kwargs={'script_path': "sh/pull.sh"},
        dag=dag
    )

    (deploy)

dag = deploy_dag()
