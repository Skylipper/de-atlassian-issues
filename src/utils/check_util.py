import logging

import src.config.variables as var
from src.utils import atlassian_util

def get_today_issue_version_count(logger = logging.getLogger("JQL check")):
    # Получаем количество обновленных запросов с fix_version с начала дня из JQL
    jql_query = f"""({var.PLAIN_JQL}) AND affectedVersion is not EMPTY AND 'updated' >= startOfDay()"""
    count = atlassian_util.get_jql_results_count(jql_query)
    logger.info(f"Check query: {jql_query}. JQL results: {count}")

    return count

def get_today_issue_fix_ver_count(logger = logging.getLogger("JQL check")):
    # Получаем количество обновленных запросов с fix_version с начала дня из JQL
    jql_query = f"""({var.PLAIN_JQL}) AND fixVersion is not EMPTY AND 'updated' >= startOfDay()"""
    count = atlassian_util.get_jql_results_count(jql_query)
    logger.info(f"Check query: {jql_query}. JQL results: {count}")

    return count

def get_today_issue_components_count(logger = logging.getLogger("JQL check")):
    # Получаем количество обновленных запросов с компонентами с начала дня из JQL
    jql_query = f"""({var.PLAIN_JQL}) AND component is not EMPTY AND 'updated' >= startOfDay()"""
    count = atlassian_util.get_jql_results_count(jql_query)
    logger.info(f"Check query: {jql_query}. JQL results: {count}")

    return count

def get_today_issue_count(logger = logging.getLogger("JQL check")):
    # Получаем количество обновленных запросов с начала дня из JQL
    jql_query = f"""({var.PLAIN_JQL}) AND 'updated' >= startOfDay()"""
    count = atlassian_util.get_jql_results_count(jql_query)
    logger.info(f"Check query: {jql_query}. JQL results: {count}")

    return count

def inform_somebody(context, logger=logging.getLogger("Load failure")):
    logger.error(f"Informing you that dag {context.get('task_instance').dag_id} is failed")