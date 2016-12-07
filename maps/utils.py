from API import api_request as api
from API.configuration import USER, MONGOIP, MONGOPORT
import ipdb
from datetime import datetime, timedelta
from pymongo import MongoClient
import itertools


def id_to_schedule(object_id):
    # status if query fails or success
    status = False
    # one id => one schedule

    # first query mongo
    # Initialize connection with MongoClient
    c = MongoClient(MONGOIP, MONGOPORT)
    db = c["sncf"]
    collection = db["route_schedules"]
    # search for max 3 hours old information
    update_time = datetime.now() - timedelta(hours=3)
    update_time = update_time.strftime('%Y%m%dT%H%M%S')
    findquery = {"object_id": object_id, "updated_time": {"$gte": update_time}}
    result = collection.find_one(findquery)
    if result:
        status = True
        return status, result["schedule"]

    # if not present, query sncf api
    query_path = "coverage/sncf/trips/" + object_id + \
        "/route_schedules?data_freshness=adapted_schedule"
    request = api.ApiRequest(USER, query_path)
    request.compute_request_page(debug=False)
    routeparser = api.RequestParser(request.results, "route_schedules")
    routeparser.parse()
    # 0 because only one per id
    try:
        schedule = routeparser.nested_items["route_schedules"][0]
        # save it in mongo
        now = datetime.now().strftime('%Y%m%dT%H%M%S')
        mongoobject = {"object_id": object_id,
                       "schedule": schedule, "updated_time": now}
        collection.insert(mongoobject)
        status = True
        return status, schedule

    except IndexError:
        # error ?

        schedule = {}
        return status, schedule


def to_geosjon(coords_list, severity, display_informations, delay, cause):

    geoobject = {
        "type": "Feature",
        "properties": {
            "severity": severity,
            "delay": str(delay),
            "cause": cause,
            "display_informations": display_informations,
            "label": display_informations["label"] + " " + severity["name"] + ". Retard: " + str(delay) + " minutes. Cause: " + cause,
        },
        "geometry": {"type": "LineString",
                     "coordinates": coords_list
                     },
    }
    return geoobject


def disruption_to_geojsons(disruption):
    # a disruption might have multiple impacted objects/routes
    # so return a list
    # find impacted_objects ids of the given disruption: usually one
    impacted_object = disruption["impacted_objects"][0]
    impacted_object_id = impacted_object["pt_object"]["id"]
    # ipdb.set_trace()
    # query schedules of the impacted objects: usually one
    status, schedule = id_to_schedule(impacted_object_id)
    # if unable to get schedule, we don't show this disruption and return false
    if not status:
        return status
    # extract rows_list for given schedule
    rows_list = schedule["table"]["rows"]
    # extract label from each schedule
    display_informations = schedule["display_informations"]
    # extract coordonates
    coordslist = list(map(lambda x: [x["stop_point"]["coord"]["lon"], x[
        "stop_point"]["coord"]["lat"]], rows_list))
    try:
        coordslist = coordslist.remove(["0.0", "0.0"])
        # don't show if after removal there is not enough to draw a line
        if not coordslist:
            return False
        elif len(coordslist) < 2:
            return False
    except ValueError:
        # no zero coordonate
        pass

    # compute max delay
    try:
        delay = impacted_stops_to_max_delay(impacted_object["impacted_stops"])
    except KeyError:
        delay = 0
    # find cause (just look at first stop)
    try:
        cause = impacted_object["impacted_stops"][0]["cause"]
        if cause == "":
            cause = "non définie"
    except KeyError:
        cause = "pas trouvée"
    # create geojson objects
    geoJsonobject = to_geosjon(coordslist, disruption[
        "severity"], display_informations, delay, cause)
    print(geoJsonobject)
    return geoJsonobject


def impacted_stops_to_max_delay(stop_list):
    """
    Computes delay for each impacted stop and returns maximum.
    """
    def strings_to_time_diff(stringtuple):
        """
        Return time difference in minutes
        """
        #"amended_arrival_time": "093800"
        # base_arrival_time": "092800",
        # first, lets convert time
        string1 = stringtuple[0]
        string2 = stringtuple[1]
        hour1 = int(string1[0:2])
        minute1 = int(string1[2:4])
        second1 = int(string1[4:6])
        time1 = timedelta(hours=hour1, minutes=minute1, seconds=second1)
        hour2 = int(string2[0:2])
        minute2 = int(string2[2:4])
        second2 = int(string2[4:6])
        time2 = timedelta(hours=hour2, minutes=minute2, seconds=second2)
        diff = time1 - time2
        # return in minutes instead of seconds
        diff = diff.seconds / 60
        return diff
    times_tuples = map(lambda x: (x["amended_arrival_time"], x[
                       "base_arrival_time"]), stop_list)
    time_diffs = list(map(strings_to_time_diff, times_tuples))
    max_delay = max(time_diffs)
    return max_delay
