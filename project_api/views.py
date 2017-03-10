from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from project_api.utils import sch_trip_stops, rt_trains_in_station, rt_train_in_stations, rt_trip_stops, sch_station_stops, trip_dummy_predict
from rest_framework import generics
from project_api.serializers import TrainPassage
from datetime import datetime


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class Station(APIView):
    """
    Query a given station to get:
    - past trains
    - expected trains (those displayed on stations' boards)

    Do not provide (yet) information on scheduled trains that have not been displayed on boards.
    """

    def get(self, request, format=None):
        """ Get station information. Depending on parameter "info" passed:
         - real-time
         - schedule
         - prediction
        """
        station_id = request.query_params.get('station_id', None)
        day = request.query_params.get('day', None)
        info = request.query_params.get('info', "real-time")

        if day:
            try:
                yyyymmdd_date = datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not station_id:
            return Response({"Error": "must specify station_id"})

        if info not in ["real-time", "schedule", "prediction"]:
            return Response({"Error": "info must be real-time schedule or prediction"})

        if info == "real-time":
            response = rt_trains_in_station(station_id, day=day)
            return Response(response)

        if info == "schedule":
            response = sch_station_stops(station_id, day=day)
            return Response(response)

        else:
            return Response({"Error": "not implemented yet, for now, only real-time"})


class Trip(APIView):
    """
    Query a given trip_id information about stops:
     - real-time
     - schedule
     - predictions
    """

    def get(self, request, format=None):
        """
        For real-time:
        Steps:
         - 1: get scheduled stop times to know all scheduled stations for this trip, and list stations
         - 2: try to find train_num (extract digits), and get date
         - 3: get real-time information for this trip in all scheduled stations
         - 4: find out which stations are passed already
        """
        trip_id = request.query_params.get('trip_id', None)
        info = request.query_params.get('info', "real-time")

        if not trip_id:
            return Response({"Error": "must specify trip_id"})

        if info not in ["real-time", "schedule", "prediction"]:
            return Response({"Error": "info must be real-time schedule or prediction"})

        if info == "schedule":
            response = sch_trip_stops(trip_id)
            return Response(response)

        if info == "real-time":
            response = rt_trip_stops(trip_id)
            return Response(response)

        if info == "prediction":
            response = trip_dummy_predict(trip_id)
            return Response(response)

        else:
            return Response({"Error": "not implemented yet, for now, only real-time"})
