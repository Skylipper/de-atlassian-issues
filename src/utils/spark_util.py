import logging

from pyspark.sql import SparkSession, DataFrame

import src.config.variables as var
import src.utils.connection_util as conn_util


def get_spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("de-atlassian-project") \
        .getOrCreate()

    return spark


def read_dwh_issues_view(spark: SparkSession, last_update_time, log=logging.getLogger("spark")) -> DataFrame:
    log.info(f"Reading dwh issues view from {last_update_time}")
    dwh_conn = conn_util.get_dwh_conn_props()
    df = spark.read \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://{dwh_conn["host"]}:{dwh_conn["port"]}/{dwh_conn["db"]}') \
        .option('driver', dwh_conn["driver"]) \
        .option('dbtable', f'{var.CDM_SCHEMA_NAME}.{var.CDM_MV_ISSUES_INFO_TABLE_NAME}') \
        .option('user', dwh_conn["user"]) \
        .option('password', dwh_conn["password"]) \
        .load() \
        .filter(f"updated >= '{last_update_time}'")

    return df


def write_issues_info_to_click(df: DataFrame, log=logging.getLogger("spark")):
    log.info(f"Writing dwh issues info to temp table")
    click_conn_props = conn_util.get_click_conn_props()

    url = f"""jdbc:clickhouse://{click_conn_props["host"]}:{click_conn_props["port"]}/{click_conn_props["db"]}"""

    # Write DataFrame to ClickHouse
    df.write \
        .format("jdbc") \
        .option("driver", click_conn_props["driver"]) \
        .option("url", url) \
        .option("user", click_conn_props["user"]) \
        .option("ssl", "true") \
        .option("password", click_conn_props["password"]) \
        .option("dbtable", var.CLICK_ISSUES_TEMP_TABLE_NAME) \
        .mode("append") \
        .save()
