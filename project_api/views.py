from rest_framework.views import APIView
from rest_framework.response import Response
from project_api.utils_dynamo import dynamo_get_trains_in_station
from rest_framework import generics
from project_api.serializers import TrainPassage


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

        if not station_id:
            return Response({"Error": "must specify station_id"})

        response = dynamo_get_trains_in_station(station_id)
        return Response(response)
