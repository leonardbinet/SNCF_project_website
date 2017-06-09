"""Api views.
"""

from datetime import datetime
from distutils.util import strtobool

from django.shortcuts import render
from rest_framework.response import Response
from rest_framework import generics

from api_etl.query import DBQuerier
from api_etl.serializers import (
    ResultSetSerializer, NestedSerializer,
    CalendarSerializer, CalendarDateSerializer, TripSerializer,
    StopTimeSerializer, StopSerializer, AgencySerializer,
    RouteSerializer, AgencySerializer, RealTimeDepartureSerializer
)


def extractLevel(request, default=1):
    """ Extract level from get parameters and parse it.
    """
    level = request.query_params.get('level', default)
    try:
        level = int(level)
    except:
        level = default
    return level


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class Stations(generics.ListCreateAPIView):
    """
    Return stations objects.
    """

    def get_serializer_class(self):
        return StopSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        uic_code = self.request.query_params.get('uic_code', None)
        level = extractLevel(self.request)

        # uic_code
        if uic_code:
            uic_code = str(uic_code)
            assert (len(uic_code) == 7) or (len(uic_code) == 8)

        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier.stations(level=level)
        return results


class StopTimes(generics.ListCreateAPIView):
    """
    For a given day provides scheduled stoptimes.

    """

    def get_serializer_class(self):
        level = extractLevel(self.request)

        if level == 1:
            return StopTimeSerializer
        else:
            return NestedSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        uic_code = self.request.query_params.get('uic_code', None)
        day = self.request.query_params.get('day', None)
        trip_id_filter = self.request.query_params.get('trip_id_filter', False)
        realtime = self.request.query_params.get('realtime', None)
        realtime_only = self.request.query_params.get('realtime_only', None)
        level = extractLevel(self.request)

        limit = self.request.query_params.get('query_limit', 1000)

        # Day
        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                day = False

        # Trip active at time: default value is set to now
        trip_active_at_time = self.request.query_params\
            .get('trip_active_at_time', True)
        if trip_active_at_time:
            try:
                trip_active_at_time = strtobool(trip_active_at_time)
            except:
                # if it doesn't work, keep it as such, it is supposed to be
                # time and handled by dbquery methods
                pass

        # Realtime or not
        if realtime:
            try:
                realtime = strtobool(realtime)
            except:
                pass

        # Realtime only
        if realtime_only:
            try:
                realtime_only = strtobool(realtime_only)
            except:
                pass

        # Day
        if day:
            try:
                datetime.strptime(day, "%Y%m%d")
            except:
                return Response({"Error": "day must be in yyyymmdd format"})

        # Uic code
        if uic_code:
            uic_code = str(uic_code)
            assert (len(uic_code) == 7) or (len(uic_code) == 8)

        # Limit
        if isinstance(limit, str):
            try:
                limit = int(limit)
            except:
                limit = 1000

        # PERFORM QUERY
        querier = DBQuerier(yyyymmdd=day)
        # Get Schedule
        result = querier.stoptimes(
            on_day=day,
            uic_filter=uic_code,
            level=level,
            trip_active_at_time=trip_active_at_time,
            trip_id_filter=trip_id_filter,
            limit=limit
        )

        # Get realtime
        if realtime and level > 0:
            result_serializer = ResultSetSerializer(result, yyyymmdd=day)
            result_serializer.batch_realtime_query()
            response = result_serializer\
                .get_nested_dicts(realtime_only=realtime_only)
            return response

        else:
            return result
