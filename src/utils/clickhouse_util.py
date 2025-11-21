import clickhouse_connect


def create_table(clickhouse_client, database, table_name, columns:dict, logger):
    logger.info(f'CREATE TEMPORARY table {database}.{table_name}')
    clickhouse_client.command(
        f"""CREATE TABLE IF NOT EXISTS {table_name}  ENGINE = Memory;""")

def get_clickhouse_client():
    host = "rc1d-1f3k5g7o5m28ukjo.mdb.yandexcloud.net"
    port = 8443
    db_name = "atlassian"
    user = "admin"
    password = "3RNUAXLmvrreYFU"

    clickhouse_client = clickhouse_connect.get_client(host=host, port=port, database=db_name,
                                                      username=user,
                                                      password=password, secure=False)

    return clickhouse_client

clickhouse_client = get_clickhouse_client()


create_table(clickhouse_client, database="atlassian", table_name="issues_info", columns={})
