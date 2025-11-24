import logging

import src.config.variables as var
from src.utils import atlassian_util


# Проверим, что количество обновленных запросов с начала дня одинаково в JQL и в БД

def get_today_issue_count(logger = logging.getLogger("Sql check")):
    jql_query = f"""({var.PLAIN_JQL}) AND 'updated' >= startOfDay()"""
    logger.info(f"Check query: {jql_query}")

    count = atlassian_util.get_jql_results_count(jql_query) + 10

    logger.info(f"JQL results: {count}")

    return count
