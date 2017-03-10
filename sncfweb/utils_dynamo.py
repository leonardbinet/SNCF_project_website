from os import sys, path
import logging
import boto3
from boto3.dynamodb.conditions import Key
import pytz
from datetime import datetime
from dateutil.tz import tzlocal
from sncfweb.settings.secrets import get_secret

logger = logging.getLogger(__name__)

# Dynamo DB tables:
dynamo_real_dep = "real_departures_2"
dynamo_sched_dep = "scheduled_departures"

AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

dynamodb = boto3.resource('dynamodb')


def get_paris_local_datetime_now():
    paris_tz = pytz.timezone('Europe/Paris')
    datetime_paris = datetime.now(tzlocal()).astimezone(paris_tz)
    return datetime_paris


def dynamo_get_client():
    return boto3.client("dynamodb")


def dynamo_get_table(table_name):
    return dynamodb.Table(table_name)


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


def dynamo_submit_batch_getitem_request(items_keys, table_name, max_retry=3, prev_resp=None):
    # Compute query in batches of 100 items
    batches = [items_keys[i:i + 100]
               for i in range(0, len(items_keys), 100)]

    client = dynamo_get_client()

    responses = []
    unprocessed_keys = []
    for batch in batches:
        response = client.batch_get_item(
            RequestItems={
                table_name: {
                    'Keys': batch
                }
            }
        )
        try:
            responses += response["Responses"][table_name]
        except KeyError:
            pass
        try:
            unprocessed_keys += response[
                "UnprocessedKeys"][table_name]
        except KeyError:
            pass

    # TODO: add timer
    if len(unprocessed_keys) > 0:
        if max_retry == 0:
            return responses
        else:
            max_retry = max_retry - 1
            return dynamo_submit_batch_getitem_request(unprocessed_keys, table_name, max_retry=max_retry, prev_resp=responses)

    return responses
