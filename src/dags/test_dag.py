from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import src.utils.variables as var
import src.utils.atlassian_util as atl
import logging

log = logging.getLogger("load_data_from_atlassian_dag")


def check_task_func():
    atl_conn_info = atl.get_atl_connection_info()
    log.info(atl_conn_info.host)

    log.info(atl.get_jql_results(atl.get_jql_query()))

    log.info(var.PLAIN_JQL)


@dag(
    start_date=datetime(2025, 10, 15),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['load', 'project'],
)
def test():
    check_task = PythonOperator(
        task_id='check_task',
        python_callable=check_task_func
    )

    check_task


dag = test()
