import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLValueCheckOperator

import src.config.variables as var
import src.utils.check_util as check_util
from src.loaders.stg.stg_issues_loader import load_issues
from src.loaders.stg.stg_versions_loader import load_lts_versions

log = logging.getLogger("load_issues")


@dag(
    start_date=datetime(2025, 10, 15),
    schedule='10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    template_searchpath=[f'{var.AIRFLOW_DAGS_DIR}/src/sql/'],
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

    # Проверим, что количество обновленных запросов с начала дня одинаково в JQL и в БД
    issue_count_check = SQLValueCheckOperator(task_id="check_issue_count_jql_sql",
                                              conn_id=var.DWH_CONNECTION_NAME,
                                              sql="stg/check_updated_count.sql",
                                              pass_value=check_util.get_today_issue_count(),
                                              tolerance=0.01,
                                              on_failure_callback=check_util.inform_somebody)

    load_issues_task >> issue_count_check >> load_lts_versions_task


dag = load_stg_raw_data()
