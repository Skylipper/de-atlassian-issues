import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLValueCheckOperator

import src.config.variables as var
import src.utils.check_util as check_util

import src.loaders.ods.ods_tables_loader as otl

log = logging.getLogger("load_objects")


@dag(
    start_date=datetime(2025, 10, 22),
    schedule='25 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    template_searchpath=[f'{var.AIRFLOW_DAGS_DIR}/src/sql/'],
    tags=['load', 'project', 'ods', 'atlassian'],
)
def load_ods_tables():
    load_issue_components_task = PythonOperator(
        task_id='load_issue_components',
        python_callable=otl.load_issue_components
    )

    # check_issue_comp_count = SQLValueCheckOperator(task_id="check_issue_count_jql_sql",
    #                                           conn_id=var.DWH_CONNECTION_NAME,
    #                                           sql="ods/check_updated_issue_comp_count.sql",
    #                                           pass_value=check_util.get_today_issue_components_count(),
    #                                           tolerance=0.01,
    #                                           on_failure_callback=check_util.inform_somebody)

    load_issue_versions_task = PythonOperator(
        task_id='load_issue_versions',
        python_callable=otl.load_issue_versions
    )
    load_issue_fix_versions_task = PythonOperator(
        task_id='load_issue_fix_versions',
        python_callable=otl.load_issue_fix_versions
    )
    load_issues_task = PythonOperator(
        task_id='load_issues',
        python_callable=otl.load_issues
    )
    load_lts_versions_task = PythonOperator(
        task_id='load_lts_versions',
        python_callable=otl.load_lts_versions
    )

    [load_issue_components_task, load_issue_versions_task, load_issue_fix_versions_task,load_lts_versions_task] >> load_issues_task


dag = load_ods_tables()
