import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import src.loaders.dds.dds_tables_loader as dtl

log = logging.getLogger("load_objects")


@dag(
    start_date=datetime(2025, 10, 22),
    schedule='*/10 * * * *',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project', 'dds', 'atlassian'],
)
def load_dds_tables():
    load_dds_d_projects = PythonOperator(
        task_id='load_dds_d_projects',
        python_callable=dtl.load_d_projects
    )

    load_dds_d_priorities = PythonOperator(
        task_id='load_dds_d_priorities',
        python_callable=dtl.load_d_priorities
    )

    load_dds_d_components = PythonOperator(
        task_id='load_dds_d_components',
        python_callable=dtl.load_d_components
    )

    load_dds_d_issuetypes = PythonOperator(
        task_id='load_dds_d_issuetypes',
        python_callable=dtl.load_d_issuetypes
    )

    load_dds_d_resolutions = PythonOperator(
        task_id='load_dds_d_resolutions',
        python_callable=dtl.load_d_resolutions
    )

    load_dds_d_statuses = PythonOperator(
        task_id='load_dds_d_statuses',
        python_callable=dtl.load_d_statuses
    )

    load_dds_d_users = PythonOperator(
        task_id='load_dds_d_users',
        python_callable=dtl.load_d_users
    )

    load_dds_d_versions = PythonOperator(
        task_id='load_dds_d_versions',
        python_callable=dtl.load_d_versions
    )

    load_dds_f_issues = PythonOperator(
        task_id='load_dds_f_issues',
        python_callable=dtl.load_f_issues
    )

    load_dds_f_issue_components = PythonOperator(
        task_id='load_dds_f_issue_component_values',
        python_callable=dtl.load_f_issue_components
    )

    load_dds_f_issue_versions = PythonOperator(
        task_id='load_dds_f_issue_versions',
        python_callable=dtl.load_f_issue_versions
    )

    load_dds_f_issue_fix_versions = PythonOperator(
        task_id='load_dds_f_issue_fix_versions',
        python_callable=dtl.load_f_issue_fix_versions
    )

    join_task1 = EmptyOperator(task_id='join_point')

    [load_dds_d_projects, load_dds_d_priorities, load_dds_d_issuetypes,
     load_dds_d_resolutions, load_dds_d_statuses, load_dds_d_users] >> join_task1
    join_task1 >> [load_dds_d_components, load_dds_d_versions] >> load_dds_f_issues
    load_dds_f_issues >> [load_dds_f_issue_components, load_dds_f_issue_versions, load_dds_f_issue_fix_versions]


dag = load_dds_tables()
