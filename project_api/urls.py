from django.conf.urls import url
from rest_framework.urlpatterns import format_suffix_patterns
from project_api import views

urlpatterns = [
    url(r'^station/$', views.GetStationDisplayedTrains.as_view(), name='api'),
]

urlpatterns = format_suffix_patterns(urlpatterns)
