import os
from datetime import datetime

import src.utils.variables as var
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME'] = '/usr'
# os.environ['SPARK_HOME'] = '/usr/lib/spark'
# os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="test_spark",
    default_args=default_args,
    schedule_interval=None,
)

events_partitioned = SparkSubmitOperator(
    task_id='spark',
    dag=dag_spark,
    application=f'{var.AIRFLOW_DAGS_DIR}test_spark.py',
    conn_id='yarn_spark',
    executor_cores=1,
    executor_memory='1g'
)

events_partitioned