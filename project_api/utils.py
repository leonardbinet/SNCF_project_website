from sncfweb.settings.base import dynamo_sched_dep_all, dynamo_real_dep
from sncfweb.utils_dynamo import dynamo_get_table, Key, get_paris_local_datetime_now
import logging

logger = logging.getLogger(__name__)


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


def rt_trip_stops(trip_id):
    """
    """
    # Step 1: find schedule and extract stations
    scheduled_stops = sch_trip_stops(trip_id)
    station_ids = list(map(lambda x: x["station_id"], scheduled_stops))

    # Step 2: try to find train_num
    train_num = trip_id[5:11]  # should be improved later
    logger.info("Search for %s, %s", trip_id, train_num)

    # Step 3: query real-time data
    rt_elements = rt_train_in_stations(
        stations=station_ids, train_num=train_num)

    # Step 4: find out which stations are passed yet
    # "expected_passage_time": "19:47:00"
    paris_time = get_paris_local_datetime_now().strftime('%H:%M:%S')

    for rt_element in rt_elements:
        rt_element["passed"] = rt_element[
            "expected_passage_time"] < paris_time

    return rt_elements


def dummy_predict():
    pass
