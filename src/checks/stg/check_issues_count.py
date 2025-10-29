import src.utils.variables as var
# project in ('JRASERVER','JRACLOUD') AND 'updated' >= '2010-01-01 00:00'

jql_query = f"""project in ('JRASERVER','JRACLOUD') AND 'updated' >= '{var.START_DATE.strftime("%Y-%m-%d %H:%M")}'"""

print(jql_query)