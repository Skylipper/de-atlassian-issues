import src.utils.variables as var
from src.utils import dwh_util, atlassian_util

# project in ('JRASERVER','JRACLOUD') AND 'updated' >= '2010-01-01 00:00'



def get_jql_results_count():
    last_updated_date = dwh_util.get_last_loaded_ts(var.STG_WF_TABLE_NAME, var.STG_ISSUES_TABLE_NAME).strftime("%Y-%m-%d %H:%M")
    jql_query = f"""project in ('{var.SRV_PROJECT_KEY}','{var.CLOUD_PROJECT_KEY}') AND 'updated' >= '{var.START_DATE.strftime("%Y-%m-%d %H:%M")} and 'updated' <= '{last_updated_date}'"""
    print(jql_query)

    count = atlassian_util.get_jql_results_count(jql_query)
    return count
