import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

import src.loaders.dds.dds_tables_loader as dtl

log = logging.getLogger("load_objects")

def load_d_projects_f():
    dtl.load_d_projects()


@dag(
    start_date=datetime(2025, 10, 22),
    schedule='*/10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'dds', 'atlassian'],
)
def load_dds_tables():
    load_d_projects = PythonOperator(
        task_id='load_d_projects',
        python_callable=load_d_projects_f
    )

    [load_d_projects]


dag = load_dds_tables()
