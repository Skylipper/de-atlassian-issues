import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLValueCheckOperator, SQLCheckOperator
from airflow.utils.task_group import TaskGroup

import src.config.variables as var
import src.loaders.ods.ods_tables_loader as otl
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
    tags=['load', 'project', 'stg', 'dds', 'ods', 'cdm', 'atlassian'],
)
def load_atlassian_data():
    with TaskGroup("stg_issues_loader") as stg_issues_loader:
        stg_begin = DummyOperator(task_id="stg_begin")

        stg_load_issues_task = PythonOperator(
            task_id='loads_stg_issues',
            python_callable=load_issues,
            op_kwargs={'log': log}
        )
        stg_load_lts_versions_task = PythonOperator(
            task_id='load_stg_lts_versions',
            python_callable=load_lts_versions,
            op_kwargs={'log': log}
        )

        # Проверим, что количество обновленных запросов с начала дня одинаково в JQL и в БД
        stg_issue_count_check = SQLValueCheckOperator(task_id="check_stg_issue_count_jql_sql",
                                                      conn_id=var.DWH_CONNECTION_NAME,
                                                      sql="stg/check_updated_count.sql",
                                                      pass_value=check_util.get_today_issue_count(),
                                                      tolerance=0.01,
                                                      on_failure_callback=check_util.inform_somebody)

        stg_begin >> stg_load_issues_task >> stg_issue_count_check >> stg_load_lts_versions_task

    with TaskGroup("ods_issues_loader") as ods_issues_loader:
        ods_begin = DummyOperator(task_id="ods_begin")

        ods_load_issue_components_task = PythonOperator(
            task_id='load_issue_components',
            python_callable=otl.load_issue_components
        )

        ods_check_issue_comp_count = SQLValueCheckOperator(task_id="check_issue_comp_count_jql_sql",
                                                           conn_id=var.DWH_CONNECTION_NAME,
                                                           sql="ods/check_updated_issue_comp_count.sql",
                                                           pass_value=check_util.get_today_issue_components_count(),
                                                           tolerance=0.01,
                                                           on_failure_callback=check_util.inform_somebody)

        ods_load_issue_versions_task = PythonOperator(
            task_id='load_issue_versions',
            python_callable=otl.load_issue_versions
        )

        ods_check_issue_version_count = SQLValueCheckOperator(task_id="check_issue_version_count_jql_sql",
                                                              conn_id=var.DWH_CONNECTION_NAME,
                                                              sql="ods/check_updated_issue_version_count.sql",
                                                              pass_value=check_util.get_today_issue_version_count(),
                                                              tolerance=0.01,
                                                              on_failure_callback=check_util.inform_somebody)

        ods_load_issue_fix_versions_task = PythonOperator(
            task_id='load_issue_fix_versions',
            python_callable=otl.load_issue_fix_versions
        )

        ods_check_issue_fix_ver_count = SQLValueCheckOperator(task_id="check_issue_fix_ver_count_jql_sql",
                                                          conn_id=var.DWH_CONNECTION_NAME,
                                                          sql="ods/check_updated_issue_fix_ver_count.sql",
                                                          pass_value=check_util.get_today_issue_fix_ver_count(),
                                                          tolerance=0.01,
                                                          on_failure_callback=check_util.inform_somebody)

        ods_load_issues_task = PythonOperator(
            task_id='load_issues',
            python_callable=otl.load_issues
        )
        ods_check_issues_count = SQLCheckOperator(task_id="check_issues_count",
                                              conn_id=var.DWH_CONNECTION_NAME,
                                              sql="ods/check_issues_count.sql",
                                              on_failure_callback=check_util.inform_somebody)

        ods_load_lts_versions_task = PythonOperator(
            task_id='load_lts_versions',
            python_callable=otl.load_lts_versions
        )

        ods_begin >> [ods_load_issue_components_task >> ods_check_issue_comp_count,
                      ods_load_issue_versions_task >> ods_check_issue_version_count,
                      ods_load_issue_fix_versions_task >> ods_check_issue_fix_ver_count,
                      ods_load_lts_versions_task] >> ods_load_issues_task >> ods_check_issues_count

    stg_issues_loader >> ods_issues_loader


dag = load_atlassian_data()
