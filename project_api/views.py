"""Api views.
"""

import logging
from datetime import datetime
from distutils.util import strtobool

from django.shortcuts import render
from rest_framework import generics

from lib.api_etl.querier_schedule import DBQuerier
from lib.api_etl.querier_realtime import ResultsSet
from lib.api_etl.builder_feature_vector import TripPredictor, StopTimePredictor

from project_api.serializers import (
    NestedSerializer, CalendarSerializer, CalendarDateSerializer,
    TripSerializer, StopTimeSerializer, StopSerializer, AgencySerializer,
    RouteSerializer, AgencySerializer, RealTimeDepartureSerializer, StopTimePredictorSerializer
)

logger = logging.getLogger("django")


def display_params(params_dict):
    message = "\n\nVIEW PARAMS \n"
    for name, value in params_dict.items():
        message += " -%s: %s\n" % (name, value)
    logger.info(message)


def extract_level(request, default=1):
    """ Extract level from get parameters and parse it.
    """
    level = request.query_params.get('level', default)
    try:
        level = int(level)
    except:
        level = default
    return level


def extract_int(request, name, default=10000):
    """ Extract int from get parameters and parse it.
    """
    answer = request.query_params.get(name, default)
    try:
        answer = int(answer)
        return answer
    except:
        return default


def extract_uic_code(request, name):
    uic_code = request.query_params.get(name, None)
    if uic_code and (not((len(uic_code) == 7) or (len(uic_code) == 8))):
        uic_code = None
    return uic_code


def extract_bool(request, name, default=None):
    """ Extract bool from get parameters and parse it.
    """
    answer = request.query_params.get(name, default)
    try:
        answer = bool(strtobool(answer))
        return answer
    except:
        return default


def extract_at_date(request, name, regex, default=True):
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
        level = extract_level(self.request)
        on_day = extract_at_date(
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
        level = extract_level(self.request)

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
        level = extract_level(self.request)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)

        query_params = {
            "level": level,
            "on_route_short_name": on_route_short_name
        }
        display_params(query_params)

        # PERFORM QUERY
        querier = DBQuerier()
        results = querier\
            .stations(**query_params)
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
        level = extract_level(self.request)
        if level == 1:
            return TripSerializer
        else:
            return NestedSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        # ARGS PARSING
        active_at_time = extract_at_date(
            self.request,
            "active_at_time",
            "%H:%M:%S",
            True
        )
        on_day = extract_at_date(
            self.request,
            "on_day",
            "%Y%m%d",
            True
        )
        level = extract_level(self.request)
        limit = extract_int(self.request, 'query_limit', 10000)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)

        query_params = {
            "active_at_time": active_at_time,
            "on_day": on_day,
            "level": level,
            "limit": limit,
            "on_route_short_name": on_route_short_name
        }
        display_params(query_params)

        # PERFORM QUERY
        querier = DBQuerier()
        # Get Schedule
        results = querier.trips(**query_params)
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
        realtime = extract_bool(self.request, "realtime", None)
        realtime_only = extract_bool(self.request, "realtime_only", None)
        if realtime or realtime_only:
            return NestedSerializer
        level = extract_level(self.request)
        if level == 1:
            return StopTimeSerializer
        else:
            return NestedSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        logger.info("STOPTIMES API")
        # ARGS PARSING

        active_at_time = extract_at_date(
            self.request,
            "active_at_time",
            "%H:%M:%S",
            True
        )
        on_day = extract_at_date(
            self.request,
            "on_day",
            "%Y%m%d",
            True
        )

        level = extract_level(self.request)
        limit = extract_int(self.request, 'query_limit', 10000)
        uic_code = extract_uic_code(self.request, 'uic_code')
        trip_id_filter = self.request.query_params.get('trip_id_filter', False)
        on_route_short_name = self.request.query_params\
            .get('on_route_short_name', None)

        realtime = extract_bool(self.request, "realtime", None)
        realtime_only = extract_bool(self.request, "realtime_only", None)
        prediction = extract_bool(self.request, "prediction", None)

        if realtime_only or prediction:
            realtime = True

        query_params = {
            "trip_active_at_time": active_at_time,
            "on_day": on_day,
            "level": level,
            "limit": limit,
            "uic_filter": uic_code,
            "trip_id_filter": trip_id_filter,
            "on_route_short_name": on_route_short_name
        }
        realtime_params = {
            "realtime": realtime,
            "realtime_only": realtime_only,
            "prediction": prediction
        }

        display_params(query_params)
        display_params(realtime_params)

        # PERFORM QUERY
        querier = DBQuerier()
        result = querier.stoptimes(**query_params)

        # Get realtime
        if realtime and level > 0:
            logger.info(
                "Gathering REALTIME information for %s items."
                % len(result))
            result_serializer = ResultsSet(
                result, scheduled_day=on_day
                if on_day is not True else None
            )
            result_serializer.batch_realtime_query(
                scheduled_day=on_day if on_day is not True else None
            )

            response = result_serializer.results

            if realtime_only:
                response = [resp for resp in response if resp.has_realtime()]

            return response

        else:
            return result


class TripPrediction(generics.ListCreateAPIView):
    """
    Return stoptimes predictions

    You have to provide a trip_id for it to work.

    Example: /api/trip-prediction/?trip_id=DUASN145833F05002-1_408310
        (works if trip_id is present in base and is running on that day)
    """

    def get_serializer_class(self):
        return StopTimePredictorSerializer

    def get_queryset(self):
        """ Queryset provider
        """
        logger.info("TRIP PREDICTION API")
        # ARGS PARSING

        trip_id = self.request.query_params.get('trip_id', None)
        if not trip_id:
            return []

        # PERFORM QUERY
        trip_predictor = TripPredictor(trip_id=trip_id)
        return list(trip_predictor._stoptime_predictors.values())