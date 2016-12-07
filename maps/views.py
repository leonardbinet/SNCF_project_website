from django.shortcuts import render
from pymongo import MongoClient
from django.http import JsonResponse
import json
# Create your views here.
from API import api_request as api
from API.configuration import USER, MONGOIP, MONGOPORT
import ipdb
from datetime import datetime
from .utils import id_to_schedule, disruption_to_geojsons


def index(request):
    # page showing sncf stations and disruptions
    context = {
        'map': "sncf",
        'mapcollection': "stop_points",
        'marker_label': "label",
    }
    return render(request, 'map.html', context)


def ajax_stop_points(request):
    # Get the url get parameter of bounds, = "not found" if parameter does not
    # exist
    lat = request.GET.get('lat', 'not found')
    lng = request.GET.get('lng', 'not found')

    # Assuming mongodb is running on 'localhost' with port 27017
    c = MongoClient('localhost', 27017)
    db = c["sncf"]
    collection = db["stop_points"]
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
    resultdict = {"stop_points": features}
    return JsonResponse(resultdict, safe=False)


def update_disruptions(request):
    # Update data from API and save it in mongo
    path = 'coverage/sncf/disruptions'
    request = api.ApiRequest(USER, path)
    request.compute_request_pages(count=200, debug=True, page_limit=120)
    parser = api.RequestParser(request.results, "disruptions")
    parser.parse()
    disruptions_list = parser.nested_items["disruptions"]

    # Initialize connection with MongoClient
    c = MongoClient(MONGOIP, MONGOPORT)
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
    disruptions_list = collection.find(findquery).limit(200)
    print("There are %d disruptions currently active." %
          disruptions_list.count())
    # ipdb.set_trace()
    allobjects = []
    for disruption in disruptions_list:
        geoJsonobject = disruption_to_geojsons(disruption)
        if geoJsonobject:
            allobjects.append(geoJsonobject)
        # merge scheduled and real-time in GeoJSON objects

    return JsonResponse(allobjects, safe=False)
