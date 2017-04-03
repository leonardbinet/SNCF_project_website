"""Module with specific function for dynamo monitoring
"""

from api_etl.utils_dynamo import dynamo_get_client


def check_dynamo_connection():
    status = False
    try:
        client = dynamo_get_client()
        tables_names = client.list_tables()["TableNames"]

        tables_stats = []
        tables_desc = {}
        for table_name in tables_names:
            table_desc = client.describe_table(TableName=table_name)
            tables_desc[table_name] = table_desc
            table_stat = {
                "table": table_name,
                "count": table_desc["Table"]["ItemCount"]
            }
            tables_stats.append(table_stat)

        add_info = {
            "tables_desc": tables_desc,
            "tables_stats": tables_stats,
        }
        status = True
    except Exception as e:
        # Status stays False
        add_info = e
    return status, add_info
