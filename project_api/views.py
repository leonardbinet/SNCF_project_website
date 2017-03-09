from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from sncfweb.utils_dynamo import dynamo_get_trains_in_station
from project_api.utils import dynamo_get_trip_stops
from rest_framework import generics
from project_api.serializers import TrainPassage
from datetime import datetime


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class GetStationDisplayedTrains(APIView):
    """
    Query a given station to get:
    - past trains
    - expected trains (those displayed on stations' boards)

    Do not provide (yet) information on scheduled trains that have not been displayed on boards.
    """

    def get(self, request, format=None):
        """
        """
        station_id = request.query_params.get('station_id', None)
        day = request.query_params.get('day', None)

        if day:
            try:
                yyyymmdd_date = datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not station_id:
            return Response({"Error": "must specify station_id"})

        response = dynamo_get_trains_in_station(station_id, day=day)
        return Response(response)


class GetTripSchedule(APIView):
    """
    Query a given trip_id to get all stop times and stations
    """

    def get(self, request, format=None):
        """
        """
        trip_id = request.query_params.get('trip_id', None)

        if not trip_id:
            return Response({"Error": "must specify trip_id"})

        response = dynamo_get_trip_stops(trip_id)
        return Response(response)
