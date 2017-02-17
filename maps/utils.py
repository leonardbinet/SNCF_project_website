
from . import parser
import os
from monitoring.utils import connect_mongoclient
from datetime import datetime, timedelta
from pymongo import MongoClient
from navitia_client import Client
from gevent.pool import Pool
import ipdb
import pandas as pd

MONGO_USER = os.environ["MONGO_USER"]
MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]

SNCF_API_USER = os.environ["SNCF_API_USER"]


def get_collection(collection):
    c = connect_mongoclient(
        host=MONGO_HOST, user=MONGO_USER, password=MONGO_PASSWORD)
    db = c["sncf"]
    collection = db[collection]
    return collection


def request_mongo_schedule(object_id):
    collection = get_collection("route_schedules")
    # search for max 3 hours old information
    update_time = datetime.now() - timedelta(hours=3)
    update_time = update_time.strftime('%Y%m%dT%H%M%S')
    findquery = {"object_id": object_id, "updated_time": {"$gte": update_time}}
    result = collection.find_one(findquery)
    return result


def save_mongo_schedule(object_id, schedule):
    collection = get_collection("route_schedules")
    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    mongoobject = {"object_id": object_id,
                   "schedule": schedule, "updated_time": now}
    collection.insert(mongoobject)


def request_sncf_api_schedule(object_id):
    query_path = "coverage/sncf/trips/" + object_id + \
        "/route_schedules"

    client = Client(core_url="https://api.sncf.com/v1/",
                    user=SNCF_API_USER, region="sncf")
    response = client.raw(query_path, verbose=True)
    routeparser = parser.RequestParser({0: response}, "route_schedules")
    routeparser.parse()
    schedule = routeparser.nested_items["route_schedules"][0]
    return schedule


def id_to_schedule(object_id):
    # status if query fails or success
    status = False

    # First query mongo database if information already available
    result = request_mongo_schedule(object_id)
    if result:
        status = True
        print("Data available in Mongo")
        return status, result["schedule"]

    # If not, query sncf api and save it in mongo
    print("Not available in Mongo")
    try:
        schedule = request_sncf_api_schedule(object_id)
        save_mongo_schedule(object_id, schedule)
        status = True
    except:
        print("Cannot get data from SNCF and save it in Mongo")
        schedule = {}
    return status, schedule


def to_geosjon(coords_list, severity, display_informations, delay, cause, trip_id):

    geoobject = {
        "type": "Feature",
        "properties": {
            "severity": severity,
            "delay": str(delay),
            "cause": cause,
            "display_informations": display_informations,
            "label": "<ul><li>" + display_informations["label"] + " </li><li>" + severity["name"] + ". </li><li>Retard: " + str(delay) + " minutes. </li><li>Cause: " + cause + ". </li><li>Trip ID: " + trip_id + ".</li><ul>",
        },
        "geometry": {"type": "LineString",
                     "coordinates": coords_list
                     },
    }
    return geoobject


def disruption_to_geojsons(disruption):
    # A disruption always has only on impacted object/route
    impacted_object = disruption["impacted_objects"][0]
    impacted_object_id = impacted_object["pt_object"]["id"]
    # Query schedules of the impacted objects: always one
    status, schedule = id_to_schedule(impacted_object_id)
    # If unable to get schedule, we don't show this disruption and return false
    if not status:
        return False
    # Extract rows_list for given schedule
    rows_list = schedule["table"]["rows"]
    # Extract label from each schedule
    display_informations = schedule["display_informations"]
    # Extract coordonates
    coordslist = list(map(lambda x: [x["stop_point"]["coord"]["lon"], x[
        "stop_point"]["coord"]["lat"]], rows_list))
    try:
        coordslist = coordslist.remove(["0.0", "0.0"])
        # Don't show if after removal there is not enough to draw a line
        if not coordslist:
            return False
        elif len(coordslist) < 2:
            return False
    except ValueError:
        # No zero coordonate
        pass

    # Compute max delay
    try:
        delay = impacted_stops_to_max_delay(impacted_object["impacted_stops"])
    except KeyError:
        delay = 0
    # Find cause (just look at first stop)
    try:
        cause = impacted_object["impacted_stops"][0]["cause"]
        if cause == "":
            cause = "non définie"
    except KeyError:
        cause = "pas trouvée"

    # Create geojson objects
    geoJsonobject = to_geosjon(coordslist, disruption[
        "severity"], display_informations, delay, cause, impacted_object_id)
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


def geosjons_split_cancel_delay(geoobjects):
    """
    Takes a list of geojson objects and split it, returns delayed, and canceled. (doesn't return 'false' objects)
    """
    delayed = []
    canceled = []
    for geoobject in geoobjects:
        # if objet is 'false', stays false
        if not geoobject:
            continue
        elif geoobject["properties"]["severity"]["name"] == "trip delayed":
            delayed.append(geoobject)
        elif geoobject["properties"]["severity"]["name"] == "trip canceled":
            canceled.append(geoobject)
        else:
            print(geoobject)
    return delayed, canceled


def query_mongo_active_disruptions(limit):
    collection = get_collection("disruptions")
    # Find disruptions still active
    today = datetime.now().strftime('%Y%m%dT%H%M%S')
    findquery = {"application_periods.end": {"$gte": today}}
    disruptions_list = collection.find(findquery).limit(limit)
    return disruptions_list


def query_mongo_near_stations(lat, lng, limit=3000, max_distance=12000000):
    # Assuming mongodb is running on 'localhost' with port 27017
    collection = get_collection("stop_points")
    # Get points in these bounds
    filter1 = {"geometry": {"$near": {"$geometry": {
        "type": "Point",  "coordinates": [float(lng), float(lat)]},
        "$maxDistance": max_distance}}}
    # Restult dict, with message and status
    stop_points = list(collection.find(filter1, {'_id': 0}).limit(limit))
    return stop_points


def insert_disruption_mongo(disruption):
    print("Saving disruption %s" % disruption["disruption_id"])
    collection = get_collection("disruptions")

    findquery = {"disruption_id": disruption["disruption_id"]}
    collection.update(findquery, disruption, upsert=True)


def query_and_save_disruptions(today=True):
    # Update data from API and save it in mongo
    client = Client(core_url="https://api.sncf.com/v1/",
                    user=SNCF_API_USER, region="sncf")
    response = client.explore("disruptions", multipage=True, page_limit=30,
                              count_per_page=50, verbose=True)
    parsed = parser.RequestParser(response, "disruptions")
    print("Begin parsing")
    parsed.parse()
    disruptions_list = parsed.nested_items["disruptions"]
    print("Result parsed, begin saving in MongoDB")

    # Filter only today disruptions
    df = pd.DataFrame(disruptions_list)
    ipdb.set_trace()

    # Initialize connection with MongoClient
    # Save elements
    pool = Pool(3)
    pool.map(insert_disruption_mongo, disruptions_list)
    pool.join()

    # for disruption in disruptions_list:
    #    findquery = {"disruption_id": disruption["disruption_id"]}
    #    collection.update(findquery, disruption, upsert=True)
