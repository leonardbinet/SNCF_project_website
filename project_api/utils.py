from sncfweb.settings.base import dynamo_sched_dep_all
from sncfweb.utils_dynamo import dynamo_get_table, Key


def dynamo_get_trip_stops(trip_id, max_req=100):
    """
    Query items in scheduled_departures_all table, for a given trip_id

    Reminder: hash key: trip_id
    sort key: station_id
    """

    # Query
    table = dynamo_get_table(dynamo_sched_dep_all)
    response = table.query(
        ConsistentRead=False,
        KeyConditionExpression=Key('trip_id').eq(
            str(trip_id))
    )
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.query(
            ConsistentRead=False,
            KeyConditionExpression=Key('trip_id').eq(
                str(trip_id)),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        max_req -= 1
    return data
