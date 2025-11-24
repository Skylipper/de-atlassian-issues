import src.config.variables as var
from src.utils import dwh_util, atlassian_util

# Проверим, что количество обновленных запросов с начала дня одинаково в JQL и в БД

def get_jql_results_count():
    jql_query = f"""({var.PLAIN_JQL}) AND 'updated' >= startOfDay()"""
    print(jql_query)

    count = atlassian_util.get_jql_results_count(jql_query)
    print(count)
    print(type(count))

    return count
