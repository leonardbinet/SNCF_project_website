from django.shortcuts import render
from pymongo import MongoClient
from django.http import JsonResponse
import json
from .parameters import *
# Create your views here.
from API import api_request as api
from API.configuration import USER, MONGOIP, MONGOPORT
import ipdb
from datetime import datetime
from .utils import id_to_schedule, rows_to_geosjon, disruption_to_geojsons


def index(request):

    map_name = request.GET.get('map', 'sncf')

    context = {
        'map': map_name,
        'mapcollection': map_params[map_name]["bdd_collection"],
        'marker_label': map_params[map_name]["marker_label"],

    }
    return render(request, 'map.html', context)


def ajax_stop_points(request):
    # Get the url get parameter of bounds, = "not found" if parameter does not
    # exist
    lat = request.GET.get('lat', 'not found')
    lng = request.GET.get('lng', 'not found')
    map_name = request.GET.get('map', 'error')

    # Assuming mongodb is running on 'localhost' with port 27017
    c = MongoClient('localhost', 27017)
    db = c[map_name]
    collection = db[map_params[map_name]["bdd_collection"]]
    # Get points in these bounds
    filter = {"geometry": {"$geoWithin": {"$centerSphere": [
        [float(lng), float(lat)],
        100 / 3963.2
    ]
    }}
    }
    filter2 = {"geometry": {"$near": {"$geometry": {
        "type": "Point",  "coordinates": [float(lng), float(lat)]},
        "$maxDistance": 1200000}}}
    # Restult dict, with message and status
    features = list(collection.find(filter2, {'_id': 0}).limit(10500))
    resultdict = {map_params[map_name]["bdd_collection"]: features}
    return JsonResponse(resultdict, safe=False)


def update_disruptions(request):
    # Get updated data from API
    path = 'coverage/sncf/disruptions'
    request = api.ApiRequest(USER, path)
    request.compute_request_pages(count=200, debug=True, page_limit=120)
    parser = api.RequestParser(request.results, "disruptions")
    parser.parse()
    disruptions_list = parser.nested_items["disruptions"]

    # Initialize connection with MongoClient
    c = MongoClient('localhost', 27017)
    db = c["sncf"]
    collection = db["disruptions"]

    # Save elements
    for disruption in disruptions_list:
        findquery = {"disruption_id": disruption["disruption_id"]}
        collection.update(findquery, disruption, upsert=True)
    return JsonResponse({"good": True}, safe=False)


def ajax_disruptions(request):
    path = 'coverage/sncf/disruptions'
    # ask API for 50 disruptions
    # Get current disruptions
    # Initialize connection with MongoClient
    c = MongoClient('localhost', 27017)
    db = c["sncf"]
    collection = db["disruptions"]
    # Find disruptions still active
    today = datetime.now().strftime('%Y%m%dT%H%M%S')
    findquery = {"application_periods.end": {"$gte": today}}
    disruptions_list = collection.find(findquery).limit(30)
    print("There are %d disruptions currently active." %
          disruptions_list.count())
    # ipdb.set_trace()
    allobjects = []
    for disruption in disruptions_list:
        geoJsonobjects = disruption_to_geojsons(disruption)
        allobjects += geoJsonobjects
        # merge scheduled and real-time in GeoJSON objects

    return JsonResponse(allobjects, safe=False)
