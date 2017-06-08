"""Api views.
"""

from datetime import datetime

from django.shortcuts import render
from rest_framework.response import Response
from rest_framework import generics

from api_etl.query import DBQuerier
from project_api.serializers import FullStopTimeSerializer


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class StationStopTimes(generics.ListCreateAPIView):  # APIView):
    """
    Query a given station to get:
    - past trains
    - expected trains (those displayed on stations' boards)
    """

    def get_serializer_class(self):
        return FullStopTimeSerializer

    def get_queryset(self):
        # Get station information. Depending on parameter "info" passed:
        # - real-time
        # - schedule
        # - prediction

        station_id = self.request.query_params.get('station_id', None)
        day = self.request.query_params.get('day', None)

        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not station_id:
            return Response({"Error": "must specify station_id"})

        querier = DBQuerier(yyyymmdd=day)
        # Get Schedule
        result = querier.station_trips_stops(
            station_id=station_id,
            yyyymmdd=day
        )
        # Get realtime
        result.batch_realtime_query(yyyymmdd=day)

        response = result.get_nested_dicts(realtime_only=False)

        return response


class TripStopTimes(generics.ListCreateAPIView):
    """
    Query a given trip_id information about stops:
     - real-time
     - schedule
     - predictions
    """

    def get_serializer_class(self):
        return FullStopTimeSerializer

    def get_queryset(self):

        trip_id = self.request.query_params.get('trip_id', None)
        day = self.request.query_params.get('day', None)

        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not trip_id:
            return Response({"Error": "must specify trip_id"})

        querier = DBQuerier(yyyymmdd=day)
        # Get Schedule
        result = querier.trip_stops(trip_id=trip_id, yyyymmdd=day)
        # Get realtime
        result.batch_realtime_query(yyyymmdd=day)

        response = result.get_nested_dicts(realtime_only=False)

        return response
