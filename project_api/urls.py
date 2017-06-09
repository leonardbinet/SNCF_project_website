from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from project_api import views

urlpatterns = [
    url(r'^$', views.index, name='api'),
    url(r'^station/$', views.Stations.as_view(), name='api_station'),
    url(r'^stoptimes/$', views.StopTimes.as_view(), name='api_trip'),
]

urlpatterns = format_suffix_patterns(urlpatterns)
