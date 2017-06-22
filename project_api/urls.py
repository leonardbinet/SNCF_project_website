from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from project_api import views

urlpatterns = [
    url(r'^$', views.index, name='api'),
    url(r'^services/$', views.Services.as_view(), name='api_service'),
    url(r'^routes/$', views.Routes.as_view(), name='api_route'),
    url(r'^stations/$', views.Stations.as_view(), name='api_station'),
    url(r'^trips/$', views.Trips.as_view(), name='api_trip'),
    url(r'^stoptimes/$', views.StopTimes.as_view(), name='api_stoptime'),
    url(r'^trip-prediction/$', views.TripPrediction.as_view(), name='api_trip_prediction'),
]

urlpatterns = format_suffix_patterns(urlpatterns)
