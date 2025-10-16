from airflow.hooks.base import BaseHook
import src.utils.variables as var


def get_atl_bearer():
    token = BaseHook.get_connection(var.ATLASSIAN_AUTH_TOKEN_VAR_NAME)
    return token.password