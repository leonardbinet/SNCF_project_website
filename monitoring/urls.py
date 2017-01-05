from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='monitoring_home'),
    url(r'^mongodbstatus$', views.ajax_monitoring_mongo_db,
        name='ajax_monitoring_mongo_db'),

]
