import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='my_minimal_dag',
    start_date=datetime.datetime(2023, 1, 1),
    schedule=None,
    catchup=False,  # Set to False to avoid backfilling past runs
    tags=['example'],
) as dag:
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    start_task >> end_task