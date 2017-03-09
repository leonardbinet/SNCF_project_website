from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from project_api import views

urlpatterns = [
    url(r'^$', views.index, name='api'),
    url(r'^station/$', views.GetStationDisplayedTrains.as_view(), name='api_station'),
    url(r'^trip_schedule/$', views.GetTripSchedule.as_view(), name='trip_schedule'),
]

urlpatterns = format_suffix_patterns(urlpatterns)
