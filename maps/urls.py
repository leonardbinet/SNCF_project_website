from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='accueil'),
    url(r'^ajax$', views.ajax_stop_points, name='ajax_stop_points'),
]
