from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='accueil'),
    url(r'^stop_points$', views.ajax_stop_points, name='ajax_stop_points'),
    url(r'^disruptions$', views.ajax_disruptions, name='ajax_disruptions'),
    url(r'^update_disruptions$', views.update_disruptions,
        name='update_disruptions'),
    url(r'^transilien$', views.transilien_map, name='transilien'),

]
