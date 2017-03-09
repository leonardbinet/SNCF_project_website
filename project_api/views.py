from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from project_api.utils import sch_trip_stops, rt_trains_in_station, rt_train_in_stations, get_paris_local_datetime_now
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
            # Step 1: find schedule and extract stations
            scheduled_stops = sch_trip_stops(trip_id)
            station_ids = list(map(lambda x: x["station_id"], scheduled_stops))

            # Step 2: try to find train_num
            train_num = trip_id[5:11]  # should be improved later

            # Step 3: query real-time data
            rt_elements = rt_train_in_stations(
                stations=station_ids, train_num=train_num)

            # Step 4: find out which stations are passed yet
            # "expected_passage_time": "19:47:00"
            paris_time = get_paris_local_datetime_now().strftime('%H:%M:%S')

            for rt_element in rt_elements:
                rt_element["passed"] = rt_element[
                    "expected_passage_time"] < paris_time
            return Response(rt_elements)

        else:
            return Response({"Error": "not implemented yet, for now, only real-time"})


class GetTripPredictions(APIView):
    """
    Query a given trip_id to get all stop times and stations, and predictions of arrival times in all remaining stations based on real-time information.
    """

    def get(self, request, format=None):
        """
        Steps:
         - 1: get scheduled stop times to know all scheduled stations for this trip, and list stations
         - 2: try to find train_num (extract digits), and get date
         - 3: get real-time information for this trip in all scheduled stations
         - 4: find out which are the remaining stations (those to be predicted)
         - 5: build X matrix (features)
         - 5: compute predictions
         - 6: return predictions in asked format

        """
        trip_id = request.query_params.get('trip_id', None)

        if not trip_id:
            return Response({"Error": "must specify trip_id"})

        # Step 1: find schedule and extract stations
        scheduled_stops = sch_trip_stops(trip_id)
        station_ids = list(map(lambda x: x["station_id"], scheduled_stops))

        # Step 2: try to find train_num
        train_num = trip_id[5:11]  # should be improved later

        # Step 3: query real-time data
        rt_elements = rt_train_in_stations(
            stations=station_ids, train_num=train_num)

        # Step 4: find out which stations are passed yet
        # "expected_passage_time": "19:47:00"
        paris_time = get_paris_local_datetime_now().strftime('%H:%M:%S')

        for rt_element in rt_elements:
            rt_element["passed"] = rt_element[
                "expected_passage_time"] < paris_time

        # Step 5: build X matrix
        return Response(rt_elements)
