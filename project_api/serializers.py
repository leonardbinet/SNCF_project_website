""" Serializers for API
"""

from rest_framework import serializers

from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)


def ClassFactory(class_name, ExtractedClass):
    """ Transforms a model class in a corresponding Serializer class
    """
    BaseClass = serializers.Serializer

    def __init__(self, **kwargs):
        BaseClass.__init__(self, **kwargs)

    class_body = {"__init__": __init__}

    for key, value in ExtractedClass.__dict__.items():
        # for all non-hidden attributes
        if not key.startswith("_") and not callable(value):
            # set them as CharFields
            class_body[key] = serializers.CharField(max_length=300)

    newclass = type(class_name, (BaseClass,), class_body)
    return newclass

# Declaring my new serializers
# Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route, RealTimeDeparture

CalendarSerializer = ClassFactory("CalendarSerializer", Calendar)
CalendarDateSerializer = ClassFactory("CalendarDateSerializer", CalendarDate)
TripSerializer = ClassFactory("TripSerializer", Trip)
StopTimeSerializer = ClassFactory("StopTimeSerializer", StopTime)
StopSerializer = ClassFactory("StopSerializer", Stop)
AgencySerializer = ClassFactory("AgencySerializer", Agency)
RouteSerializer = ClassFactory("RouteSerializer", Route)
AgencySerializer = ClassFactory("AgencySerializer", Agency)
RealTimeDepartureSerializer = ClassFactory(
    "RealTimeDepartureSerializer", RealTimeDeparture)


class FullStopTimeSerializer(serializers.Serializer):
    Calendar = CalendarSerializer(required=False)
    CalendarDate = CalendarDateSerializer(required=False)
    Trip = TripSerializer(required=False)
    StopTime = StopTimeSerializer()
    Stop = StopSerializer(required=False)
    Agency = AgencySerializer(required=False)
    Route = RouteSerializer(required=False)
    RealTimeDeparture = RealTimeDepartureSerializer(required=False)

"""
{
        "Delay": "Unknown",
        "Calendar": {
            "service_id": "9722",
            "friday": "1",
            "sunday": "1",
            "saturday": "1",
            "thursday": "1",
            "end_date": "20170526",
            "wednesday": "1",
            "monday": "1",
            "start_date": "20170213",
            "tuesday": "1"
        },
        "StopTime": {
            "departure_time": "19:52:00",
            "stop_headsign": "nan",
            "pickup_type": "0",
            "station_id": "8727622",
            "arrival_time": "19:51:00",
            "stop_id": "StopPoint:DUA8727622",
            "trip_id": "DUASN154824F01004-1_411309",
            "drop_off_type": "0",
            "day_train_num": "20170216_154824",
            "train_num": "154824",
            "stop_sequence": "1",
            "yyyymmdd": "20170216"
        },
        "Route": {
            "route_url": "nan",
            "route_desc": "nan",
            "route_type": "2",
            "agency_id": "DUA804",
            "route_short_name": "D",
            "route_long_name": "Creil - Corbeil Essonnes / Melun / Malesherbes",
            "route_color": "5E9620",
            "route_id": "DUA800804081",
            "route_text_color": "FFFFFF"
        },
        "Trip": {
            "service_id": "9722",
            "direction_id": "0",
            "trip_headsign": "ROPO",
            "trip_id": "DUASN154824F01004-1_411309",
            "block_id": "nan",
            "route_id": "DUA800804081"
        },
        "Passed": "Unknown",
        "RealTime": {
            "date": "Unknown",
            "data_freshness": "Unknown",
            "expected_passage_day": "Unknown",
            "request_day": "Unknown",
            "term": "Unknown",
            "station_id": "Unknown",
            "request_time": "Unknown",
            "miss": "Unknown",
            "expected_passage_time": "Unknown",
            "etat": "Unknown",
            "day_train_num": "Unknown",
            "train_num": "Unknown",
            "station_8d": "Unknown"
        },
        "Stop": {
            "location_type": "0",
            "stop_lat": "48.993777",
            "zone_id": "nan",
            "stop_lon": "2.416187",
            "stop_name": "VILLIERS LE BEL GONESSE ARNOUVILLE",
            "parent_station": "StopArea:DUA8727622",
            "stop_desc": "nan",
            "stop_url": "nan",
            "stop_id": "StopPoint:DUA8727622"
        },
        "Agency": {
            "agency_timezone": "Europe/Paris",
            "agency_lang": "fr",
            "agency_url": "http://www.transilien.com",
            "agency_name": "RER D",
            "agency_id": "DUA804"
        }
    },
"""
