import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

import src.loaders.ods.ods_tables_loader as otl
import src.utils.atlassian_util as atl
import src.utils.variables as var
from src.loaders.dds.dds_tables_loader import load_d_projects

log = logging.getLogger("load_data_from_atlassian_dag")


def load_dds():
    load_d_projects()



@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def test():
    load_issues_task = PythonOperator(
        task_id='load_d_projects',
        python_callable=load_dds
    )

    load_issues_task


dag = test()
