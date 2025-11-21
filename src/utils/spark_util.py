from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession, DataFrame

import src.config.variables as var


def get_spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("de-atlassian-project") \
        .getOrCreate()

    return spark


def read_issues_view(spark: SparkSession) -> DataFrame:
    df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://87.242.100.133:5432/dwh') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'cdm.mv_issues_info') \
        .option('user', 'etl_user') \
        .option('password', 'etl_user') \
        .load()

    return df


def write_issues_info(df: DataFrame):
    host = "rc1d-1f3k5g7o5m28ukjo.mdb.yandexcloud.net"
    port = "8443"
    db_name = "atlassian"
    db_table = "cdm.mv_issues_info"
    user = "admin"
    password = "3RNUAXLmvrreYFU"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"

    url = f"jdbc:clickhouse://{host}:{port}/{db_name}"

    # Write DataFrame to ClickHouse
    df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("ssl", "true") \
        .option("password", password) \
        .option("dbtable", db_table) \
        .mode("append") \
        .save()


def read_issues_view_airflow(spark: SparkSession) -> DataFrame:
    dwh_conn_props = BaseHook.get_connection(var.DWH_CONNECTION_NAME)
    df = spark.read \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://{dwh_conn_props.host}:{dwh_conn_props.port}/{dwh_conn_props.schema}') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', f'{var.CDM_SCHEMA_NAME}.{var.CDM_MV_ISSUES_INFO_TABLE_NAME}') \
        .option('user', dwh_conn_props.login) \
        .option('password', dwh_conn_props.password) \
        .load()

    return df
