from rest_framework import serializers


class TrainPassage(serializers.Serializer):
    station_id = serializers.CharField(max_length=200)
    route_short_name = serializers.CharField(max_length=200)
    stop_sequence = serializers.CharField(max_length=200)
    data_freshness = serializers.CharField(max_length=200)
    delay = serializers.CharField(max_length=200)
    request_day = serializers.CharField(max_length=200)
    term = serializers.CharField(max_length=200)
    day_train_num = serializers.CharField(max_length=200)
    trip_id = serializers.CharField(max_length=200)
    expected_passage_time = serializers.CharField(max_length=200)
    trip_headsign = serializers.CharField(max_length=200)
    request_time = serializers.CharField(max_length=200)
    service_id = serializers.CharField(max_length=200)
    date = serializers.CharField(max_length=200)
    scheduled_departure_time = serializers.CharField(max_length=200)
    train_num = serializers.CharField(max_length=200)
    miss = serializers.CharField(max_length=200)
    etat = serializers.CharField(max_length=200)
    station_8d = serializers.CharField(max_length=200)
    expected_passage_day = serializers.CharField(max_length=200)
