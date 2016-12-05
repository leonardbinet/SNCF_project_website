from django.shortcuts import render
from pymongo import MongoClient
from django.http import JsonResponse
import json
from .parameters import *
# Create your views here.


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
