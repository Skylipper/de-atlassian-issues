import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from src.loaders.stg_fields_loader import load_fields

log = logging.getLogger("load_objects")


def load_fields_func():
    load_fields(log)


@dag(
    start_date=datetime(2025, 10, 15),
    schedule= '*/10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'stg'],
)
def load_objects_from_atlassian_to_stg():
    load_fields = PythonOperator(
        task_id='loads_fields',
        python_callable=load_fields_func
    )

    load_fields


dag = load_objects_from_atlassian_to_stg()
