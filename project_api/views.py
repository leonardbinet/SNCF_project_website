"""Api views.
"""
import json

from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
# from rest_framework.pagination import PaginationSerializer
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics

from api_etl.query import DBQuerier

from datetime import datetime


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class Station(APIView):
    """
    Query a given station to get:
    - past trains
    - expected trains (those displayed on stations' boards)
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
        form = request.query_params.get('form', None)
        page = request.GET.get('page', None)

        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not station_id:
            return Response({"Error": "must specify station_id"})

        if info == "schedule" or info == "real-time":
            querier = DBQuerier(yyyymmdd=day)
            # Get Schedule
            result = querier.station_trips_stops(
                station_id=station_id,
                yyyymmdd=day
            )
            # Get realtime
            result.batch_realtime_query(yyyymmdd=day)

            if form == "nested":
                response = result.get_nested_dicts(
                    realtime_only=False, normalize=True
                )
            else:
                response = result.get_flat_dicts(
                    realtime_only=False, normalize=True
                )
            paginator = Paginator(response, 10)

            # response = response[:50]
            try:
                # serializer = PaginationSerializer(instance=page)
                pag_response = paginator.page(page)
            except PageNotAnInteger:
                # If page is not an integer, deliver first page.
                pag_response = paginator.page(1)
            except EmptyPage:
                # If page is out of range (e.g. 9999), deliver last page of
                # results.
                pag_response = paginator.page(paginator.num_pages)
            # json.dumps(pag_response.object_list)
            pag_response = pag_response.object_list
            return Response(pag_response)

        elif info == "prediction":
            return Response({"Not implemented yet": "soon"})

        else:
            return Response({"Error": "info must be real-time schedule or prediction"})


class Trip(APIView):
    """
    Query a given trip_id information about stops:
     - real-time
     - schedule
     - predictions
    """

    def get(self, request, format=None):

        trip_id = request.query_params.get('trip_id', None)
        info = request.query_params.get('info', "real-time")
        day = request.query_params.get('day', None)
        form = request.query_params.get('form', None)

        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        if not trip_id:
            return Response({"Error": "must specify trip_id"})

        if info not in ["real-time", "schedule", "prediction"]:
            return Response({"Error": "info must be real-time schedule or prediction"})

        if info == "schedule" or info == "real-time":
            querier = DBQuerier(yyyymmdd=day)
            # Get Schedule
            result = querier.trip_stops(trip_id=trip_id, yyyymmdd=day)
            # Get realtime
            result.batch_realtime_query(yyyymmdd=day)

            if form == "nested":
                response = result.get_nested_dicts(
                    realtime_only=False, normalize=True
                )
            else:
                response = result.get_flat_dicts(
                    realtime_only=False, normalize=True
                )
            # for now: before pagination is implemented
            response = response[:50]
            return Response(response)

        if info == "prediction":
            return Response({"Error": "not implemented yet"})

        else:
            return Response({"Error": ""})
