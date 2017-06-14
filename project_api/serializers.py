"""
Classes used to serialize results of db queries.

- NestedSerializer and directs Serializers: raw serializers:
    main ability is to provide a suitable serializer for django rest api,
    especially for pagination purposes.

These are created through a Class Factory transforming model classes into
serializer classes.
"""

import logging
from datetime import datetime

from rest_framework import serializers

from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)


def ModelToSerializerFactory(class_name, ExtractedClass):
    """ Transforms a model class in a corresponding Serializer class
    """
    BaseClass = serializers.Serializer

    class_body = {}

    for key, value in ExtractedClass.__dict__.items():
        # for all non-hidden attributes
        if not key.startswith("_") and not callable(value):
            # set them as CharFields
            class_body[key] = serializers.CharField(
                max_length=300,
                required=False
            )

    newclass = type(class_name, (BaseClass,), class_body)
    return newclass

# Declaring my new serializers
# Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route, RealTimeDeparture

CalendarSerializer = ModelToSerializerFactory("CalendarSerializer", Calendar)
CalendarDateSerializer = ModelToSerializerFactory(
    "CalendarDateSerializer", CalendarDate)
TripSerializer = ModelToSerializerFactory("TripSerializer", Trip)
StopTimeSerializer = ModelToSerializerFactory("StopTimeSerializer", StopTime)
StopSerializer = ModelToSerializerFactory("StopSerializer", Stop)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RouteSerializer = ModelToSerializerFactory("RouteSerializer", Route)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RealTimeDepartureSerializer = ModelToSerializerFactory(
    "RealTimeDepartureSerializer", RealTimeDeparture)


class NestedSerializer(serializers.Serializer):
    Calendar = CalendarSerializer(required=False)
    CalendarDate = CalendarDateSerializer(required=False)
    Trip = TripSerializer(required=False)
    StopTime = StopTimeSerializer(required=False)
    Stop = StopSerializer(required=False)
    Agency = AgencySerializer(required=False)
    Route = RouteSerializer(required=False)
    RealTime = RealTimeDepartureSerializer(required=False)
