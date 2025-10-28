import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

import src.loaders.dds.dds_tables_loader as dtl

log = logging.getLogger("load_objects")


def load_d_projects_f():
    dtl.load_d_projects()


def load_d_priorities_f():
    dtl.load_d_priorities()

def load_d_components_f():
    dtl.load_d_components()


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

    load_d_priorities = PythonOperator(
        task_id='load_d_priorities',
        python_callable=load_d_priorities_f
    )

    load_dds_components = PythonOperator(
        task_id='load_dds_components',
        python_callable=load_d_components_f()
    )

    [load_d_projects, load_d_priorities] >> load_dds_components


dag = load_dds_tables()
