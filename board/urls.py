from django.conf.urls import url
from board import views

urlpatterns = [
    url(r'^trip$', views.trip, name='trip_board'),
    url(r'^station$', views.station, name='station_board'),

]
