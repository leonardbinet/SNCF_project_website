"""
Classes used to serialize results of db queries.

- NestedSerializer and directs Serializers: raw serializers:
    main ability is to provide a suitable serializer for django rest api,
    especially for pagination purposes.

These are created through a Class Factory transforming model classes into
serializer classes.
"""


from rest_framework import serializers

from lib.api_etl.data_models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)
from lib.api_etl.querier_realtime import StopTimeState
from lib.api_etl.feature_vector import StopTimeFeatureVector

def ModelToSerializerFactory(class_name, ExtractedClass):
    """ Transforms a model class in a corresponding Serializer class
    """
    BaseClass = serializers.Serializer

    class_body = {}

    for key, value in ExtractedClass.__dict__.items():
        # for all non-hidden attributes
        if not key.startswith("_") and not callable(value):
            # set them as CharFields
            class_body[key] = serializers.CharField(max_length=300, required=False)

    newclass = type(class_name, (BaseClass,), class_body)
    return newclass

# Declaring my new serializers
# Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route, RealTimeDeparture

CalendarSerializer = ModelToSerializerFactory("CalendarSerializer", Calendar)
CalendarDateSerializer = ModelToSerializerFactory("CalendarDateSerializer", CalendarDate)
TripSerializer = ModelToSerializerFactory("TripSerializer", Trip)
StopTimeSerializer = ModelToSerializerFactory("StopTimeSerializer", StopTime)
StopSerializer = ModelToSerializerFactory("StopSerializer", Stop)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RouteSerializer = ModelToSerializerFactory("RouteSerializer", Route)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RealTimeDepartureSerializer = ModelToSerializerFactory("RealTimeDepartureSerializer", RealTimeDeparture)
StopTimeStateSerializer = ModelToSerializerFactory("StopTimeStateSerializer", StopTimeState)
StopTimeFeatureVectorSerializer = ModelToSerializerFactory("StopTimeFeatureVectorSerializer", StopTimeFeatureVector)


class NestedSerializer(serializers.Serializer):
    Calendar = CalendarSerializer(required=False)
    CalendarDate = CalendarDateSerializer(required=False)
    Trip = TripSerializer(required=False)
    StopTime = StopTimeSerializer(required=False)
    Stop = StopSerializer(required=False)
    Agency = AgencySerializer(required=False)
    Route = RouteSerializer(required=False)
    RealTime = RealTimeDepartureSerializer(required=False)
    StopTimeState = StopTimeStateSerializer(required=False)


class StopTimePredictorSerializer(serializers.Serializer):
    StopTime = StopTimeSerializer(required=False)
    Stop = StopSerializer(required=False)
    RealTime = RealTimeDepartureSerializer(required=False)
    StopTimeState = StopTimeStateSerializer(required=False)

    StopTimeFeatureVector = StopTimeFeatureVectorSerializer(required=False)

    at_datetime = serializers.CharField(max_length=300, required=False)
    scheduled_day = serializers.CharField(max_length=300, required=False)
    next_stop_passed_realtime = serializers.CharField(max_length=300, required=False)
    to_predict = serializers.CharField(max_length=300, required=False)
    prediction = serializers.CharField(max_length=300, required=False)