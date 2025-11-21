import pyspark.sql.functions as f

from src.utils import spark_util

spark = spark_util.get_spark()
with spark:
    issues_df = spark_util.read_dwh_issues_view(spark)

    issues_df = (issues_df
                 .select("issue_id", "issue_key", "project_key", "issuetype_name", "priority_name", "status_name",
                         "resolution_name", "is_lts", "assignee_name", "assignee_key", "reporter_name", "reporter_key",
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
    issues_df = issues_df.sample(False, 0.01)

    issues_df.printSchema()
    issues_df.show(10, False)

    spark_util.write_issues_info_to_click(issues_df)
