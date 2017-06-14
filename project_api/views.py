"""Api views.
"""

import logging
from datetime import datetime
from distutils.util import strtobool

from django.shortcuts import render
from rest_framework.response import Response
from rest_framework import generics

from api_etl.query import DBQuerier
from api_etl.serializers import ResultSetSerializer
from project_api.serializers import (
    NestedSerializer, CalendarSerializer, CalendarDateSerializer,
    TripSerializer, StopTimeSerializer, StopSerializer, AgencySerializer,
    RouteSerializer, AgencySerializer, RealTimeDepartureSerializer
)

logger = logging.getLogger("django")


def extractLevel(request, default=1):
    """ Extract level from get parameters and parse it.
    """
    level = request.query_params.get('level', default)
    try:
        level = int(level)
    except:
        level = default
    return level


def displayParams(params_dict):
    message = "\n\nVIEW PARAMS \n"
    for name, value in params_dict.items():
        message += " -%s: %s\n" % (name, value)
    logger.info(message)


def extractInt(request, name, default=10000):
    """ Extract int from get parameters and parse it.
    """
    answer = request.query_params.get(name, default)
    try:
        answer = int(answer)
        return answer
    except:
        return default


def extractUicCode(request, name):
    uic_code = request.query_params.get(name, None)
    if uic_code and (not((len(uic_code) == 7) or (len(uic_code) == 8))):
        uic_code = None
    return uic_code


def extractBool(request, name, default=None):
    """ Extract bool from get parameters and parse it.
    """
    answer = request.query_params.get(name, default)
    try:
        answer = bool(strtobool(answer))
        return answer
    except:
        return default


def extractAtDate(request, name, regex, default=True):
    """ Extract level from get parameters and parse it.
    """
    answer = request.query_params.get(name, default)
    # first check if date
    try:
        datetime.strptime(answer, regex)
        return answer
    except:
        pass
    # then check if boolean
    try:
        answer = bool(strtobool(answer))
        return answer
    except:
        # finally returns default is none works
        return default


def index(request):
    context = {}
    return render(request, 'project_api/index.html', context)


class Services(generics.ListCreateAPIView):
    """
    Return Calendar objects.
    """

    def get_serializer_class(self):
        return CalendarSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        level = extractLevel(self.request)
        on_day = extractAtDate(
            self.request,
            "on_day",
            "%Y%m%d",
            True
        )
        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier.services(on_day=on_day, level=level)
        return results


class Routes(generics.ListCreateAPIView):
    """
    Return routes objects.
    """

    def get_serializer_class(self):
        return RouteSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        level = extractLevel(self.request)

        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier.routes(level=level)
        return results


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
        level = extractLevel(self.request)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)
        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier\
            .stations(level=level, on_route_short_name=on_route_short_name)
        return results


class Trips(generics.ListCreateAPIView):
    """
    Return trips objects.
    - active_at_time: hh:mm:ss or boolean, default True (active now)
    - on_day: yyyymmdd, default True (-> today)
    - level: int, default 2
    - limit: int, default 10000
    - get_trip: default None (to filter one Trip): not implemented yet
    """

    def get_serializer_class(self):
        level = extractLevel(self.request)
        if level == 1:
            return TripSerializer
        else:
            return NestedSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        active_at_time = extractAtDate(
            self.request,
            "active_at_time",
            "%H:%M:%S",
            True
        )
        on_day = extractAtDate(
            self.request,
            "on_day",
            "%Y%m%d",
            True
        )
        level = extractLevel(self.request)
        limit = extractInt(self.request, 'query_limit', 10000)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)

        info_params = {
            "active_at_time": active_at_time,
            "on_day": on_day,
            "level": level,
            "limit": limit,
            "on_route_short_name": on_route_short_name
        }
        displayParams(info_params)

        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier.trips(
            on_day=on_day,
            active_at_time=active_at_time,
            level=level,
            limit=limit,
            on_route_short_name=on_route_short_name
        )
        return results


class StopTimes(generics.ListCreateAPIView):
    """
    Return stoptimes objects.
    - active_at_time: hh:mm:ss or boolean, default True (active now)
    - on_day: yyyymmdd, default True (-> today)
    - level: int, default 2
    - limit: int, default 10000
    - uic_code: station uic code, default None
    - trip_id_filter: default None
    - realtime: bool, default False
    - realtime_only: bool, default False
    """

    def get_serializer_class(self):
        realtime = extractBool(self.request, "realtime", None)
        realtime_only = extractBool(self.request, "realtime_only", None)
        if realtime or realtime_only:
            return NestedSerializer
        level = extractLevel(self.request)
        if level == 1:
            return StopTimeSerializer
        else:
            return NestedSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        logger.info("STOPTIMES API")
        # ARGS PARSING

        active_at_time = extractAtDate(
            self.request,
            "active_at_time",
            "%H:%M:%S",
            True
        )
        on_day = extractAtDate(
            self.request,
            "on_day",
            "%Y%m%d",
            True
        )

        level = extractLevel(self.request)
        limit = extractInt(self.request, 'query_limit', 10000)
        uic_code = extractUicCode(self.request, 'uic_code')
        trip_id_filter = self.request.query_params.get('trip_id_filter', False)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)

        realtime = extractBool(self.request, "realtime", None)
        realtime_only = extractBool(self.request, "realtime_only", None)

        if realtime_only:
            realtime = True

        info_params = {
            "active_at_time": active_at_time,
            "on_day": on_day,
            "level": level,
            "limit": limit,
            "uic_code": uic_code,
            "trip_id_filter": trip_id_filter,
            "on_route_short_name": on_route_short_name,
            "realtime": realtime,
            "realtime_only": realtime_only
        }
        displayParams(info_params)

        # PERFORM QUERY
        querier = DBQuerier()
        result = querier.stoptimes(
            trip_active_at_time=active_at_time,
            on_day=on_day,
            level=level,
            limit=limit,
            uic_filter=uic_code,
            trip_id_filter=trip_id_filter,
            on_route_short_name=on_route_short_name
        )

        # Get realtime
        if realtime and level > 0:
            logger.info(
                "Gathering REALTIME information for %s items."
                % len(result))
            result_serializer = ResultSetSerializer(
                result, yyyymmdd=on_day
                if on_day is not True else None)
            result_serializer.batch_realtime_query(yyyymmdd=on_day if on_day is
                                                   not True else None)
            response = result_serializer\
                .get_nested_dicts(realtime_only=realtime_only)
            logger.info("Found %s of them." % len(response))
            return response

        else:
            return result
