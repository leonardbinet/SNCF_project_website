from API import api_request as api
from API.configuration import USER, MONGOIP, MONGOPORT
import ipdb
from datetime import datetime


def id_to_schedule(object_id):
    # one id => one schedule
    # first query mongo

    # if not present, query sncf api

    query_path = "coverage/sncf/trips/" + object_id + \
        "/route_schedules?data_freshness=adapted_schedule"
    request = api.ApiRequest(USER, query_path)
    request.compute_request_page(debug=False)
    routeparser = api.RequestParser(request.results, "route_schedules")
    routeparser.parse()
    # 0 because only one per id
    schedule = routeparser.nested_items["route_schedules"][0]
    return schedule


def rows_to_geosjon(rows):
    coordslist = list(map(lambda x: [x["stop_point"]["coord"]["lon"], x[
        "stop_point"]["coord"]["lat"]], rows))
    geoobject = {
        "type": "Feature",
        "properties": {},
        "geometry": {"type": "LineString",
                     "coordinates": coordslist
                     },
    }
    return geoobject


def disruption_to_geojsons(disruption):
    # a disruption might have multiple impacted objects/routes
    # so return a list
    # find impacted_objects ids of the given disruption
    impacted_objects_ids = list(map(lambda x: x["pt_object"]["id"], disruption[
        "impacted_objects"]))
    # ipdb.set_trace()
    # query schedules of the impacted objects
    schedules_lists = map(id_to_schedule, impacted_objects_ids)
    # extract
    rows_lists = map(lambda x: x["table"]["rows"], schedules_lists)
    # create geojson objects
    geoJsonobjects = list(map(rows_to_geosjon, rows_lists))
    print(geoJsonobjects)
    return geoJsonobjects
