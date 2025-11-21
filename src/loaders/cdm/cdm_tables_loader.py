import logging

import pyspark.sql.functions as f

import src.config.variables as var
import src.utils.clickhouse_util as clickhouse_util
from src.utils import spark_util


def load_issues_info(logger=logging.getLogger("load_cdm_clickhouse_issues_info")):
    # Get last update date from clickhouse datamart
    client = clickhouse_util.get_clickhouse_client()

    result = client.query("SELECT MAX (updated) as last_time FROM atlassian.issues_info;")
    last_updated_date = str(result.result_set[0][0])

    if not last_updated_date:
        last_updated_date = '1970-01-01 00:00:00'

    spark = spark_util.get_spark()

    with spark:
        # Get dataframe from dwh
        issues_df = spark_util.read_dwh_issues_view(spark, last_updated_date)

        issues_df = (issues_df
                     .select("issue_id", "issue_key", "project_key", "issuetype_name", "priority_name", "status_name",
                             "resolution_name", "components_json", "is_lts", "versions_json",
                             "fix_versions_json", "assignee_name", "assignee_key", "reporter_name", "reporter_key",
                             "creator_name", "creator_key", "votes", "created", "updated", "resolutiondate")
                     .orderBy(issues_df.created, ascending=False)
                     .withColumn("days_to_close",
                                 f.round((issues_df.resolutiondate - issues_df.created).cast("int") / 60 / 60 / 24, 4))
                     .withColumn("created_year", f.year(issues_df.created))
                     .withColumn("created_month", f.month(issues_df.created))
                     .withColumn("created_week", f.weekofyear(issues_df.created))
                     .withColumn("created_day", f.dayofmonth(issues_df.created))
                     .withColumn("resolved_year", f.year(issues_df.resolutiondate))
                     .withColumn("resolved_month", f.month(issues_df.resolutiondate))
                     .withColumn("resolved_week", f.weekofyear(issues_df.resolutiondate))
                     .withColumn("resolved_day", f.dayofmonth(issues_df.resolutiondate))
                     .withColumn("etl_time", f.current_timestamp())
                     )
        logger.info(f"Issues to load: {issues_df.count()}")

        if issues_df.count() == 0:
            logger.info("No issues found")
            return

        # Drop temp table
        clickhouse_util.drop_table(client, var.CLICK_ISSUES_TEMP_TABLE_NAME)

        # Create temp table
        clickhouse_util.execute_query_from_file(client, var.CLICK_INIT_TMP_ISSUES_FILE_NAME)

        # Write dataframe to temp table
        spark_util.write_issues_info_to_click(issues_df)

        # Clear updated issues from datamart
        clickhouse_util.execute_query_from_file(client, var.CLICK_CLEAR_ISSUES_FILE_NAME)

        # Load data from temp table to datamart
        clickhouse_util.execute_query_from_file(client, var.CLICK_LOAD_ISSUES_FILE_NAME)

        # Drop temp table
        clickhouse_util.drop_table(client, var.CLICK_ISSUES_TEMP_TABLE_NAME)
