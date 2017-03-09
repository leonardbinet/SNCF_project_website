from sncfweb.settings.base import dynamo_sched_dep_all, dynamo_real_dep
from sncfweb.utils_dynamo import dynamo_get_table, Key, get_paris_local_datetime_now

# Request all passages in given station (real and expected, not planned)


def rt_trains_in_station(station, day=None, max_req=100):
    """
    Query items in real_departures table, for today, in a given station

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    if not day:
        paris_date = get_paris_local_datetime_now()
        day = paris_date.strftime("%Y%m%d")

    # Query
    table = dynamo_get_table(dynamo_real_dep)
    response = table.query(
        ConsistentRead=False,
        KeyConditionExpression=Key('station_id').eq(
            str(station)) & Key('day_train_num').begins_with(day)
    )
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.query(
            ConsistentRead=False,
            KeyConditionExpression=Key('station_id').eq(
                str(station)) & Key('day_train_num').begins_with(day),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        max_req -= 1

    return data


def rt_train_in_stations(stations, train_num, day=None):
    """
    Query items in real_departures table, for today, in given stations for a given train_num

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    if not day:
        paris_date = get_paris_local_datetime_now()
        day = paris_date.strftime("%Y%m%d")

    responses = []
    table = dynamo_get_table(dynamo_real_dep)

    # Query
    for station_id in stations:
        response = table.query(
            ConsistentRead=False,
            KeyConditionExpression=Key('station_id').eq(
                str(station_id)) & Key('day_train_num').eq("%s_%s" % (day, train_num))
        )
        responses.extend(response['Items'])

    return responses


def sch_trip_stops(trip_id, max_req=100):
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
