from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
        dag_id='load_data_from_atlassian',
        start_date=datetime(2025, 10, 15),
        schedule=None,
        is_paused_upon_creation=True,
        catchup=False,
        tags=['load', 'project'],
) as dag:
    # Define a placeholder task using EmptyOperator
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    # Define task dependencies (optional, but shows the structure)
    start_task >> end_task
