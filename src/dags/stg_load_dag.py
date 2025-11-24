import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

import src.config.variables as var
from src.loaders.stg.stg_issues_loader import load_issues
from src.loaders.stg.stg_versions_loader import load_lts_versions

log = logging.getLogger("load_issues")

# TODO add issue count check
var.MODE = "dag"


@dag(
    start_date=datetime(2025, 10, 15),
    schedule='10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'stg', 'atlassian'],
)
def load_stg_raw_data():
    load_issues_task = PythonOperator(
        task_id='loads_issues',
        python_callable=load_issues,
        op_kwargs={'log': log}
    )
    load_lts_versions_task = PythonOperator(
        task_id='load_stg_lts_versions',
        python_callable=load_lts_versions,
        op_kwargs={'log': log}
    )

    load_issues_task >> load_lts_versions_task


dag = load_stg_raw_data()
