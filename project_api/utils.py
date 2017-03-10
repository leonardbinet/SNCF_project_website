from sncfweb.settings.base import dynamo_sched_dep_all, dynamo_real_dep, sched_dep_all_sec_index, dynamo_sched_dep
from sncfweb.utils_dynamo import dynamo_get_table, Key, get_paris_local_datetime_now, dynamo_submit_batch_getitem_request
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

import json
import pandas as pd
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


def sch_station_stops(station_id, day=None, max_req=100):
    # IndexName='video_id-index',
    """
    Query items in scheduled_departures table, for a given station_id on a given day

    """
    if not day:
        paris_date = get_paris_local_datetime_now()
        day = paris_date.strftime("%Y%m%d")

    # First step: find all trip_id/station_id tuples
    table = dynamo_get_table(dynamo_sched_dep)
    response = table.query(
        ConsistentRead=False,
        KeyConditionExpression=Key('station_id').eq(
            str(station_id)) & Key('day_train_num').begins_with(day)
    )
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.query(
            ConsistentRead=False,
            KeyConditionExpression=Key('station_id').eq(
                str(station_id)) & Key('day_train_num').begins_with(day),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        max_req -= 1

    return data


def rt_trip_stops(trip_id, sch=False):
    """ Find real-time inforamtion about a trip stops
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

    if sch:
        return extend_schedule_with_realtime(scheduled_stops, rt_elements)

    return rt_elements


def trip_dummy_predict(trip_id):
    """ This function try to provide predictions of departure time for a given trip_id, for all stations that have not been passed yet.

        It is based on a dummy idea: simple translation of delay from last station.

        - queries schedule
        - queries real-time
        - find out which stations have not been passed yet
        - find out what was delay at last station
        - generate response objects.
    """
    passages = rt_trip_stops(trip_id, sch=True)
    passages = pd.DataFrame(passages)

    # find out last departure delay (based on real-time last passage)
    passed = passages[passages["passed"] == True]
    if len(passed) == 0:
        json_list = json.loads(passed.to_json(orient='records'))
        return json_list

    last_passage_index = passed["stop_sequence"].argmax()
    last_passage = passed.loc[last_passage_index]

    last_delay = last_passage["delay"]
    last_stop_sequence = last_passage["stop_sequence"]

    # make predictions for next stations (not passed yet)
    # here it means adding "last_delay" seconds to "scheduled_departure_time"
    # to row where schedule stop sequence > last passage
    to_predict = passages[passages["stop_sequence"] > last_stop_sequence]
    # dirty pour l'instant
    to_predict["api_prediction"] = to_predict["scheduled_departure_time"].apply(
        lambda x: "%s + %s seconds" % (x, last_delay))

    # return tout
    json_list1 = json.loads(to_predict.to_json(orient='records'))
    json_list2 = json.loads(passed.to_json(orient='records'))
    json_list = json_list1 + json_list2
    return json_list


def extend_schedule_with_realtime(schedule, realtime, df_format=False):
    """ We want to return all objects in schedule, updated with real-time information.

    :rtype: list of objects
    """
    schedule = pd.DataFrame(schedule)
    realtime = pd.DataFrame(realtime)
    rt_cols_to_keep = (set(realtime.columns.values) -
                       set(schedule.columns.values))
    # {'data_freshness', 'expected_passage_time', 'request_time', 'request_day', 'date', 'term', 'etat', 'miss', 'expected_passage_day', 'station_8d', 'delay', 'passed'}
    rt_cols_to_keep.update(["station_id", "trip_id"])
    rt_cols_to_keep = list(rt_cols_to_keep)

    updated = pd.merge(
        schedule, realtime[rt_cols_to_keep],
        on=["trip_id", "station_id"],
        how='outer', indicator=False,
        suffixes=("schedule", "realtime")
    )

    if df_format:
        return updated
    else:
        json_list = json.loads(updated.to_json(orient='records'))
        return json_list
