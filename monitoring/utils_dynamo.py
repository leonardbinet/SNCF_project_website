"""Module with specific function for dynamo monitoring
"""


import boto3

from lib.api_etl.utils_secrets import get_secret

# Set as environment variable: boto takes it directly
AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

dynamodb = boto3.resource('dynamodb')


def dynamo_get_client():
    """
    Return Dynamo client (credentials already set up)
    """
    return boto3.client("dynamodb")


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
