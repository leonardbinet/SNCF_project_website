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


# Request all passages in given station (real and expected, not planned)

def dynamo_get_trains_in_station(station, day=None, max_req=100):
    """
    Query items in real_departures table, for today, in a given station

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    if not day:
        paris_date = get_paris_local_datetime_now()
        day = paris_date.strftime("%Y%m%d")

    table_name = dynamo_real_dep

    # Query
    table = dynamo_get_table(table_name)
    response = table.query(
        ConsistentRead=False,
        KeyConditionExpression=Key('station_id').eq(
            str(station)) & Key('day_train_num').begins_with(day)
    )
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        max_req -= 1
    return data


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
        return True, e
    return status, add_info
