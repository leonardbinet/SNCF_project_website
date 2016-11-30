from django.shortcuts import render
from pymongo import MongoClient
from django.http import JsonResponse
import json
# Create your views here.


def index(request):
    context = {
    }
    return render(request, 'map.html', context)


def ajax_stop_points(request):
    # Get the url get parameter of bounds, = "not found" if parameter does not
    # exist
    lat = request.GET.get('lat', 'not found')
    lng = request.GET.get('lng', 'not found')

    # Assuming mongodb is running on 'localhost' with port 27017
    c = MongoClient('localhost', 27017)
    db = c.sncf
    collection = db.stop_points
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
    stop_points = list(collection.find(filter2, {'_id': 0}).limit(5000))
    resultdict = {"stop_points": stop_points}
    return JsonResponse(resultdict, safe=False)
