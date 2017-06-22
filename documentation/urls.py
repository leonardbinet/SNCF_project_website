from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^documentation$', views.documentation, name='documentation'),
    url(r'^source_code$', views.source_code, name='source_code'),
]
