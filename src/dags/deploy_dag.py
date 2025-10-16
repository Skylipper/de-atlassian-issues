from airflow.decorators import dag
from airflow.operators.bash import BashOperator
import pendulum


@dag(
    schedule_interval=None,
    start_date=pendulum.now('UTC'),
    catchup=False,
    tags=['project', 'deploy', 'manual'],
    is_paused_upon_creation=True
)
def deploy_dag():
    run_bash_script = BashOperator(
        task_id='run_deploy_bash_script',
        bash_command='/opt/airflow/dags/src/sh/pull.sh ',
    )

    run_bash_script

dag = deploy_dag()
