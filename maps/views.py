from django.shortcuts import render
from django.http import JsonResponse
from multiprocessing import Pool
from .utils import (
    disruption_to_geojsons, geosjons_split_cancel_delay, query_mongo_active_disruptions,
    query_mongo_near_stations, query_and_save_disruptions
)


def sncf_fr_map(request):
    # page showing sncf stations and disruptions
    context = {}
    return render(request, 'map.html', context)


def ajax_stop_points(request):
    # Get the url get parameter of bounds, = "not found" if parameter does not
    # exist
    lat = request.GET.get('lat', 'not found')
    lng = request.GET.get('lng', 'not found')
    features = query_mongo_near_stations(
        lat, lng, limit=10000, max_distance=12000000)
    resultdict = {"stop_points": features}
    return JsonResponse(resultdict, safe=False)


def ajax_disruptions(request):
    """
    This view serves information to map.
    """
    disruptions_list = query_mongo_active_disruptions(limit=300)
    print("There are %d disruptions currently active." %
          disruptions_list.count())

    # Get active disruptions routes and convert it in geojson objects
    pool = Pool(processes=30)
    allgeojsonobjects = pool.map(disruption_to_geojsons, disruptions_list)
    pool.close()
    pool.join()

    # Split results in delayed or canceled trips
    delayed, canceled = geosjons_split_cancel_delay(allgeojsonobjects)
    result = {"delayed": delayed, "canceled": canceled}
    return JsonResponse(result, safe=False)


def transilien_map(request):
    context = {
        "map_js": "todo"
    }
    return render(request, 'transilien.html', context)


def update_disruptions(request):
    # Update data from API and save it in mongo
    query_and_save_disruptions()
    return JsonResponse({"status": True})
